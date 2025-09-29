import gspread
from google.oauth2.service_account import Credentials
from flask import Flask, jsonify, send_from_directory
from lxml import etree
import os
import logging
from dotenv import load_dotenv
from datetime import datetime

# --- Load environment variables from .env file ---
load_dotenv()

# --- Settings ---
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
CREDENTIALS_FILE = os.getenv('GOOGLE_CREDENTIALS_PATH', 'credentials.json')
SPREADSHEET_NAME = os.getenv('SPREADSHEET_NAME')
WORKSHEET_NAME = os.getenv('WORKSHEET_NAME')

# --- NEW BLOCK: Constants with XSD schema constraints ---
XSD_VENDOR_ID_MAX_LENGTH = 64
XSD_ATTRIBUTE_NAME_MAX_LENGTH = 32
XSD_ATTRIBUTE_VALUE_MAX_LENGTH = 32
XSD_ALLOWED_PRICE_TYPES = {
    "FIXED_PRICE", "BIDDING", "NEGOTIABLE", "NOT_APPLICABLE",
    "CREDIBLE_BID", "SWAP", "FREE", "RESERVED",
    "SEE_DESCRIPTION", "ON_DEMAND", "BIDDING_FROM"
    
}
# --- END OF NEW BLOCK ---
# --- NEW BLOCK: List of columns that will be converted to XML attributes ---
ATTRIBUTE_COLUMNS = [
    'area_sqm',
    'property_type',
    'deal_type',
    # Add other attribute columns here if they appear
]
# --- END OF NEW BLOCK ---

# --- Logging setup (simplified format) ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Cloudinary configuration ---
cloudinary_cloud_name = os.getenv('CLOUDINARY_CLOUD_NAME')
cloudinary_api_key = os.getenv('CLOUDINARY_API_KEY')
cloudinary_api_secret = os.getenv('CLOUDINARY_API_SECRET')

if cloudinary_cloud_name and cloudinary_api_key and cloudinary_api_secret:
    import cloudinary
    import cloudinary.uploader
    cloudinary.config(
        cloud_name = cloudinary_cloud_name,
        api_key = cloudinary_api_key,
        api_secret = cloudinary_api_secret,
        secure = True
    )
else:
    logger.warning("Cloudinary environment variables not configured. Feed upload will be unavailable.")


# --- Flask application initialization ---
app = Flask(__name__)

# --- Local file storage configuration ---
XML_STORAGE_DIR = 'xml_files'
if not os.path.exists(XML_STORAGE_DIR):
    os.makedirs(XML_STORAGE_DIR)

# --- List of columns with additional images ---
IMAGE_COLUMNS = ['img_2', 'img_3', 'img_4', 'img_5', 'img_6', 'img_7', 'img_8', 'img_9', 'img_10']

def get_sheet_data():
    """Connects to Google Sheets and retrieves data."""
    try:
        if not os.path.exists(CREDENTIALS_FILE):
            raise FileNotFoundError(f"credentials.json file not found at path: {CREDENTIALS_FILE}")
            
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPE)
        client = gspread.authorize(creds)
        sheet = client.open(SPREADSHEET_NAME).worksheet(WORKSHEET_NAME)
        records = sheet.get_all_records()
        logger.info(f"Success: retrieved {len(records)} records from table '{SPREADSHEET_NAME}'.")
        return records
    except Exception as e:
        logger.error(f"Google Sheets access error: {e}")
        raise

def validate_record(record):
    """Checks that the record contains required fields."""
    required_fields = ['vendorId', 'title', 'description', 'categoryId', 'priceType']
    
    for field in required_fields:
        if not record.get(field):
            return False, f"missing required field '{field}'"
    
    # Price validation block was removed, as we now handle empty price with fallback.

    
    return True, None

    # --- NEW BLOCK: Function for XSD constraint validation ---
def validate_xsd_constraints(record):
    """Validates record data against XSD schema (lengths, types, enumerations)."""
    # Check vendorId length
    vendor_id = str(record.get('vendorId', ''))
    if len(vendor_id) > XSD_VENDOR_ID_MAX_LENGTH:
        return False, f"field 'vendorId' ('{vendor_id[:10]}...') exceeds max length {XSD_VENDOR_ID_MAX_LENGTH} characters"

    # Check allowed values for priceType
    price_type = str(record.get('priceType', '')).upper()
    if price_type and price_type not in XSD_ALLOWED_PRICE_TYPES:
        return False, f"value '{price_type}' in field 'priceType' is not allowed by schema"

    # Check that categoryId is a positive integer
    try:
        category_id = int(float(record.get('categoryId')))
        if category_id <= 0:
            return False, f"field 'categoryId' must be a positive number, but got: {category_id}"
    except (ValueError, TypeError, AttributeError):
        return False, f"field 'categoryId' ('{record.get('categoryId')}') cannot be converted to a number"

    # Old attribute validation removed, as they are now collected from separate columns.

    return True, None

def clean_text(text):
    """Cleans and prepares text for XML."""
    return str(text).strip() if text is not None else ""

def is_valid_url(url):
    """Checks that URL is valid."""
    url_str = str(url).strip()
    return url_str.startswith(('http://', 'https://'))

def get_attribute_value_with_fallback(key, value):
    """Applies fallback logic for specific attributes."""
    if key == 'area_sqm':
        try:
            # Try to convert value to number
            numeric_value = int(float(value))
            # If value is 0 or less, return '1'
            if numeric_value <= 0:
                return '1'
        except (ValueError, TypeError):
            # If value is empty or not a number (e.g., "N/A"), return '1'
            return '1'
    
    # For all other attributes, return their original value unchanged
    return value

def get_price_with_fallback(price_value, price_type):
    """
    Converts price value to integer considering priceType.
    - For types requiring price, returns 1 if price is invalid, empty or 0.
    - For other types returns 0 so that <price> tag simply exists in XML.
    """
    # Price types for which price is mandatory
    types_requiring_price = ['FIXED_PRICE', 'BIDDING_FROM']

    if price_type in types_requiring_price:
        if price_value is None:
            return 1  # Fallback for missing price
        try:
            numeric_price = int(float(price_value))
            return 1 if numeric_price <= 0 else numeric_price
        except (ValueError, TypeError):
            return 1  # Fallback for non-numeric values ("N/A")
    else:
        # For NEGOTIABLE, FREE etc. types, price is not needed.
        # Return 0 to satisfy schema requirement for <price> tag presence.
        return 0

def generate_xml_feed(records):
    """Generates XML feed and returns dictionary with statistics."""
    ns = "http://admarkt.marktplaats.nl/schemas/1.0"
    root = etree.Element(f"{{{ns}}}ads", nsmap={'admarkt': ns})
    
    processed_count = 0
    skipped_count = 0
    error_details = []
    
    for i, record in enumerate(records):
        # --- DEBUG BLOCK ---
        if i == 0: # Print information only for the very first data row
            print("\n\n--- DEBUG START ---")
            print("DEBUG: All headers that Python sees from your table:")
            print(list(record.keys()))
            print("--- DEBUG END ---\n\n")
        # --- END OF BLOCK ---
        row_num = i + 2  # Row numbering in Google Sheets starts from 1, +1 for header
        vendor_id = record.get('vendorId') or f'ROW-{row_num}'
        
        try:
            # Skip inactive ads
            if str(record.get('Available', '')).upper() not in ['TRUE', 'YES', '1']:
                continue

            # Stage 1: Check for required fields
            is_valid, error_msg = validate_record(record)
            if not is_valid:
                reason = f"Row {row_num} (ID: {vendor_id}): skipped due to validation error - {error_msg}."
                logger.warning(reason)
                error_details.append({"vendorId": vendor_id, "reason": reason})
                skipped_count += 1
                continue # <-- continue now inside if

            # Stage 2: Check compliance with XSD constraints
            is_xsd_valid, xsd_error_msg = validate_xsd_constraints(record)
            if not is_xsd_valid:
                reason = f"Row {row_num} (ID: {vendor_id}): skipped due to XSD non-compliance - {xsd_error_msg}."
                logger.warning(reason)
                error_details.append({"vendorId": vendor_id, "reason": reason})
                skipped_count += 1
                continue # <-- and this continue is also inside its if
            
            ad_element = etree.SubElement(root, f"{{{ns}}}ad")
            
            # --- Main fields ---
            etree.SubElement(ad_element, f"{{{ns}}}vendorId").text = clean_text(record.get('vendorId'))
            etree.SubElement(ad_element, f"{{{ns}}}title").text = clean_text(record.get('title'))
            etree.SubElement(ad_element, f"{{{ns}}}description").text = etree.CDATA(clean_text(record.get('description')))
            etree.SubElement(ad_element, f"{{{ns}}}categoryId").text = str(int(float(record.get('categoryId'))))
            etree.SubElement(ad_element, f"{{{ns}}}priceType").text = clean_text(record.get('priceType')).upper()
            
            price_type_val = clean_text(record.get('priceType')).upper()
            price_content = get_price_with_fallback(record.get('price'), price_type_val)
            etree.SubElement(ad_element, f"{{{ns}}}price").text = str(price_content)
            if record.get('url') and is_valid_url(record.get('url')):
                etree.SubElement(ad_element, f"{{{ns}}}url").text = clean_text(record.get('url'))
            
            # --- Images ---
            all_images = [clean_text(record.get('image_link'))] if record.get('image_link') and is_valid_url(record.get('image_link')) else []
            for img_col in IMAGE_COLUMNS:
                img_url = record.get(img_col)
                if img_url and is_valid_url(img_url):
                    all_images.append(clean_text(img_url))
            
            if all_images:
                media_element = etree.SubElement(ad_element, f"{{{ns}}}media")
                for img_url in all_images:
                    etree.SubElement(media_element, f"{{{ns}}}image", url=img_url)

            # --- Attributes ---
            found_attributes = []
            for attr_key in ATTRIBUTE_COLUMNS:
                # Check that such column exists in table data
                if record.get(attr_key) is not None:
                    original_value = clean_text(record.get(attr_key))
                    
                    # Apply our new fallback logic to the value
                    final_value = get_attribute_value_with_fallback(attr_key, original_value)
                    
                    # Add attribute only if it has a final value
                    if final_value:
                        found_attributes.append((attr_key, final_value))

            # If attributes were found, create XML block
            if found_attributes:
                attrs_element = etree.SubElement(ad_element, f"{{{ns}}}attributes")
                for key, value in found_attributes:
                    attr = etree.SubElement(attrs_element, f"{{{ns}}}attribute")
                    etree.SubElement(attr, f"{{{ns}}}attributeName").text = key
                    etree.SubElement(attr, f"{{{ns}}}attributeValue").text = value
            
            processed_count += 1
        except (ValueError, TypeError) as e:
            reason = f"Row {row_num} (ID: {vendor_id}): skipped due to data error - {e}. Check number format."
            logger.error(reason)
            error_details.append({"vendorId": vendor_id, "reason": reason})
            skipped_count += 1
            
    logger.info(f"XML generation completed. Added: {processed_count}, Skipped: {skipped_count}")
    
    return {
        "xml_content": etree.tostring(root, pretty_print=True, xml_declaration=True, encoding='UTF-8'),
        "processed_count": processed_count,
        "skipped_count": skipped_count,
        "error_details": error_details
    }

def save_feed_locally(xml_content):
    """Saves XML feed to local file system, replacing previous version."""
    try:
        # Save as latest.xml (overwrites previous version)
        latest_path = os.path.join(XML_STORAGE_DIR, "latest.xml")
        with open(latest_path, 'wb') as f:
            f.write(xml_content)
        
        logger.info(f"Success: feed saved locally as latest.xml (replaced previous version)")
        return {
            "filename": "latest.xml",
            "local_path": latest_path
        }
    except Exception as e:
        logger.error(f"Local file save error: {e}")
        raise

def upload_feed_to_cloudinary(xml_content):
    """Uploads XML feed to Cloudinary, replacing previous version."""
    if not all([cloudinary_cloud_name, cloudinary_api_key, cloudinary_api_secret]):
        raise ConnectionError("Cloudinary credentials not configured.")
    
    try:
        upload_result = cloudinary.uploader.upload(
            file=xml_content,
            resource_type="raw",
            public_id="marktplaats_latest",  # Static filename
            folder="XMLs/Netherlands/Marktplaats",
            overwrite=True  # Always overwrite previous version
        )
        logger.info(f"Success: feed uploaded to Cloudinary (replaced previous version). URL: {upload_result.get('secure_url')}")
        return upload_result
    except Exception as e:
        logger.error(f"Cloudinary upload error: {e}")
        raise

@app.route('/generate-feed')
def generate_and_upload_feed():
    """Main endpoint for feed generation and upload."""
    try:
        records = get_sheet_data()
        generation_result = generate_xml_feed(records)
        
        # Save XML locally first
        local_save_result = save_feed_locally(generation_result["xml_content"])
        
        # Upload to Cloudinary (optional, may fail if credentials not configured)
        cloudinary_result = None
        cloudinary_error = None
        try:
            cloudinary_result = upload_feed_to_cloudinary(generation_result["xml_content"])
        except Exception as e:
            cloudinary_error = str(e)
            logger.warning(f"Cloudinary upload failed: {cloudinary_error}")
        
        response_data = {
            "status": "success",
            "message": "Feed successfully generated and saved locally.",
            "local_feed_url": "/xml",  # Static URL that never changes
            "stats": {
                "total_rows_found_in_sheet": len(records),
                "rows_added_to_xml": generation_result["processed_count"],
                "rows_skipped": generation_result["skipped_count"],
                "errors": generation_result["error_details"]
            }
        }
        
        # Add Cloudinary info if upload was successful
        if cloudinary_result:
            response_data["cloudinary_feed_url"] = cloudinary_result.get('secure_url')
            response_data["message"] += " Also uploaded to Cloudinary."
        elif cloudinary_error:
            response_data["cloudinary_error"] = cloudinary_error
        
        return jsonify(response_data), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/xml')
def serve_xml():
    """Serves the current XML file."""
    try:
        return send_from_directory(XML_STORAGE_DIR, "latest.xml", as_attachment=False)
    except FileNotFoundError:
        return jsonify({"error": "No XML file available. Generate a feed first."}), 404

@app.route('/')
def index():
    """Main page."""
    return '''
    <h1>Marktplaats feed generation service is active.</h1>
    <p>Available endpoints:</p>
    <ul>
        <li><a href="/generate-feed">/generate-feed</a> - Generate and upload feed</li>
        <li><a href="/xml">/xml</a> - View current XML feed (static URL)</li>
    </ul>
    <p><strong>Note:</strong> Each sync replaces the previous XML file. The URL remains static.</p>
    '''

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(debug=True, host='0.0.0.0', port=port)
