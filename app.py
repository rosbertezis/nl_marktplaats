import gspread
from google.oauth2.service_account import Credentials
from flask import Flask, jsonify, send_from_directory
from lxml import etree
import os
import logging
from dotenv import load_dotenv
import json

# --- Load environment variables ---
load_dotenv()

# --- Settings ---
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
CREDENTIALS_FILE = os.getenv('GOOGLE_CREDENTIALS_PATH', 'credentials.json')
SPREADSHEET_NAME = os.getenv('SPREADSHEET_NAME')
WORKSHEET_NAME = os.getenv('WORKSHEET_NAME')

# Validate required environment variables
if not SPREADSHEET_NAME:
    raise ValueError("SPREADSHEET_NAME environment variable is required")
if not WORKSHEET_NAME:
    raise ValueError("WORKSHEET_NAME environment variable is required")

# --- Constants ---
XSD_VENDOR_ID_MAX_LENGTH = 64
XSD_ATTRIBUTE_NAME_MAX_LENGTH = 32
XSD_ATTRIBUTE_VALUE_MAX_LENGTH = 32
XSD_ALLOWED_PRICE_TYPES = {
    "FIXED_PRICE", "BIDDING", "NEGOTIABLE", "NOT_APPLICABLE",
    "CREDIBLE_BID", "SWAP", "FREE", "RESERVED",
    "SEE_DESCRIPTION", "ON_DEMAND", "BIDDING_FROM"
}

ATTRIBUTE_COLUMNS = ['area_sqm', 'property_type', 'deal_type']
IMAGE_COLUMNS = ['img_2', 'img_3', 'img_4', 'img_5', 'img_6', 'img_7', 'img_8', 'img_9', 'img_10']

# --- Logging setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Cloudinary configuration ---
cloudinary_cloud_name = os.getenv('CLOUDINARY_CLOUD_NAME')
cloudinary_api_key = os.getenv('CLOUDINARY_API_KEY')
cloudinary_api_secret = os.getenv('CLOUDINARY_API_SECRET')

if cloudinary_cloud_name and cloudinary_api_key and cloudinary_api_secret:
    try:
        import cloudinary
        import cloudinary.uploader
        cloudinary.config(
            cloud_name=cloudinary_cloud_name,
            api_key=cloudinary_api_key,
            api_secret=cloudinary_api_secret,
            secure=True
        )
        logger.info("Cloudinary configuration successful")
    except Exception as e:
        logger.error(f"Cloudinary configuration failed: {e}")
        cloudinary_cloud_name = None
else:
    logger.warning("Cloudinary not configured. Upload will be unavailable.")

# --- Flask app ---
app = Flask(__name__)

# --- Local storage ---
XML_STORAGE_DIR = 'xml_files'
os.makedirs(XML_STORAGE_DIR, exist_ok=True)


def get_sheet_data():
    """Connects to Google Sheets and retrieves data."""
    try:
        logger.info("Connecting to Google Sheets...")
        
        if not os.path.exists(CREDENTIALS_FILE):
            raise FileNotFoundError(f"credentials.json not found at: {CREDENTIALS_FILE}")
        
        # Validate credentials file
        with open(CREDENTIALS_FILE, 'r') as f:
            creds_data = json.load(f)
            if creds_data.get('type') != 'service_account':
                raise ValueError("Invalid service account credentials format")
        
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPE)
        client = gspread.authorize(creds)
        sheet = client.open(SPREADSHEET_NAME).worksheet(WORKSHEET_NAME)
        records = sheet.get_all_records()
        
        logger.info(f"Retrieved {len(records)} records")
        return records
        
    except Exception as e:
        logger.error(f"Google Sheets error: {e}")
        raise


def validate_record(record):
    """Checks required fields."""
    required_fields = ['vendorId', 'title', 'description', 'categoryId', 'priceType']
    
    for field in required_fields:
        if not record.get(field):
            return False, f"missing '{field}'"
    
    return True, None


def validate_xsd_constraints(record):
    """Validates XSD constraints."""
    vendor_id = str(record.get('vendorId', ''))
    if len(vendor_id) > XSD_VENDOR_ID_MAX_LENGTH:
        return False, f"vendorId too long ({len(vendor_id)} > {XSD_VENDOR_ID_MAX_LENGTH})"
    
    price_type = str(record.get('priceType', '')).upper()
    if price_type and price_type not in XSD_ALLOWED_PRICE_TYPES:
        return False, f"invalid priceType: '{price_type}'"
    
    try:
        category_id = int(float(record.get('categoryId')))
        if category_id <= 0:
            return False, f"categoryId must be positive, got: {category_id}"
    except (ValueError, TypeError):
        return False, f"invalid categoryId: '{record.get('categoryId')}'"
    
    return True, None


def clean_text(text):
    """Cleans text for XML."""
    if text is None or str(text).strip() == '':
        return ""
    return str(text).strip()


def format_text_for_marktplaats(text):
    """Formats text with HTML tags."""
    if not text:
        return ""
    
    s = str(text).replace('\r\n', '\n').replace('\r', '\n').strip()
    if not s:
        return ""
    
    # Check if already contains HTML
    contains_html = ('<' in s and '>' in s)
    
    if contains_html:
        # Collapse multiple newlines
        while '\n\n\n' in s:
            s = s.replace('\n\n\n', '\n\n')
        s = s.replace('\n\n', '<br><br>').replace('\n', '<br>')
        return s
    
    # Handle bullet points
    s = s.replace('\n•', '<br>•')
    
    # Split into paragraphs
    paragraphs = [p.strip() for p in s.split('\n\n') if p.strip()]
    html_parts = []
    
    for para in paragraphs:
        html_para = para.replace('\n', '<br>')
        html_parts.append(f"<p>{html_para}</p>")
    
    return ''.join(html_parts)


def is_valid_url(url):
    """Validates URL format."""
    if not url:
        return False
    url_str = str(url).strip()
    return url_str.startswith(('http://', 'https://'))


def get_attribute_value_with_fallback(key, value):
    """Applies fallback for specific attributes."""
    if key == 'area_sqm':
        if not value or str(value).strip() == '':
            return '1'
        try:
            numeric_value = int(float(value))
            return '1' if numeric_value <= 0 else str(numeric_value)
        except (ValueError, TypeError):
            return '1'
    
    return str(value) if value else ''


def get_price_with_fallback(price_value, price_type):
    """Handles price with fallback logic."""
    types_requiring_price = ['FIXED_PRICE', 'BIDDING_FROM']
    
    if price_type in types_requiring_price:
        if not price_value:
            return 1
        try:
            numeric_price = int(float(price_value))
            return 1 if numeric_price <= 0 else numeric_price
        except (ValueError, TypeError):
            return 1
    else:
        return 0


def replace_text_tags(record):
    """Replaces template tags in text fields."""
    memory = {}
    
    tag_mapping = {
        '{{Centre_description}}': lambda: memory.get('Centre_description', ''),
        '{{center_name}}': lambda: clean_text(record.get('center_name', '')),
        '{{price}}': lambda: clean_text(record.get('price (mirror)', '')),
        '{{currency}}': lambda: 'EUR',
        '{{area_Min}}': lambda: clean_text(record.get('area_sqm', '')),
        '{{area_Max}}': lambda: clean_text(record.get('area_max', ''))
    }
    
    def replace_tags_in_text(text):
        if not text:
            return text, []
        
        result = str(text)
        replacements = []
        
        for tag, get_value in tag_mapping.items():
            if tag in result:
                value = get_value()
                result = result.replace(tag, value)
                replacements.append(f"{tag} -> {value}")
        
        return result, replacements
    
    # Process fields in order
    title, title_rep = replace_tags_in_text(record.get('title', ''))
    memory['title'] = title
    
    centre_desc, centre_rep = replace_tags_in_text(record.get('Centre_description', ''))
    memory['Centre_description'] = centre_desc
    
    # New: process preheader after Centre_description
    preheader, preheader_rep = replace_tags_in_text(record.get('preheader', ''))
    memory['preheader'] = preheader

    description, desc_rep = replace_tags_in_text(record.get('description', ''))
    memory['description'] = description
    
    # Log replacements
    all_rep = title_rep + centre_rep + preheader_rep + desc_rep
    if all_rep:
        vendor_id = record.get('vendorId', 'Unknown')
        logger.info(f"Replacements for {vendor_id}: {', '.join(all_rep)}")
    
    updated_record = record.copy()
    updated_record['title'] = title
    updated_record['Centre_description'] = centre_desc
    updated_record['preheader'] = preheader
    updated_record['description'] = description
    
    return updated_record


def generate_xml_feed(records):
    """Generates XML feed."""
    ns = "http://admarkt.marktplaats.nl/schemas/1.0"
    root = etree.Element(f"{{{ns}}}ads", nsmap={'admarkt': ns})
    
    processed_count = 0
    skipped_count = 0
    error_details = []
    
    for i, record in enumerate(records):
        row_num = i + 2
        vendor_id = record.get('vendorId') or f'ROW-{row_num}'
        
        try:
            # Skip inactive
            available = str(record.get('Available', '')).strip().upper()
            if available not in ['TRUE', 'YES', '1']:
                continue
            
            # Replace tags
            record = replace_text_tags(record)
            
            # Validate
            is_valid, error_msg = validate_record(record)
            if not is_valid:
                reason = f"Row {row_num} ({vendor_id}): {error_msg}"
                logger.warning(reason)
                error_details.append({"vendorId": vendor_id, "reason": reason})
                skipped_count += 1
                continue
            
            is_xsd_valid, xsd_error = validate_xsd_constraints(record)
            if not is_xsd_valid:
                reason = f"Row {row_num} ({vendor_id}): XSD error - {xsd_error}"
                logger.warning(reason)
                error_details.append({"vendorId": vendor_id, "reason": reason})
                skipped_count += 1
                continue
            
            # Build XML
            ad = etree.SubElement(root, f"{{{ns}}}ad")
            
            etree.SubElement(ad, f"{{{ns}}}vendorId").text = clean_text(record.get('vendorId'))
            etree.SubElement(ad, f"{{{ns}}}title").text = clean_text(record.get('title'))
            
            # Combine preheader + description: preheader first, then a blank paragraph, then description
            combined_description_source = record.get('description')
            preheader_text = record.get('preheader')
            if preheader_text and str(preheader_text).strip():
                base_desc = str(combined_description_source or '')
                combined_description_source = f"{str(preheader_text).strip()}\n\n{base_desc.strip()}" if base_desc.strip() else str(preheader_text).strip()
                logger.debug("Prepended preheader to description for vendorId=%s", record.get('vendorId'))

            formatted_desc = format_text_for_marktplaats(combined_description_source)
            etree.SubElement(ad, f"{{{ns}}}description").text = etree.CDATA(formatted_desc)
            
            etree.SubElement(ad, f"{{{ns}}}categoryId").text = str(int(float(record.get('categoryId'))))
            
            price_type = clean_text(record.get('priceType')).upper()
            etree.SubElement(ad, f"{{{ns}}}priceType").text = price_type
            
            price = get_price_with_fallback(record.get('price'), price_type)
            etree.SubElement(ad, f"{{{ns}}}price").text = str(price)
            
            if record.get('url') and is_valid_url(record.get('url')):
                etree.SubElement(ad, f"{{{ns}}}url").text = clean_text(record.get('url'))
            
            # Images
            all_images = []
            if record.get('image_link') and is_valid_url(record.get('image_link')):
                all_images.append(clean_text(record.get('image_link')))
            
            for img_col in IMAGE_COLUMNS:
                img_url = record.get(img_col)
                if img_url and is_valid_url(img_url):
                    all_images.append(clean_text(img_url))
            
            if all_images:
                media = etree.SubElement(ad, f"{{{ns}}}media")
                for img_url in all_images:
                    etree.SubElement(media, f"{{{ns}}}image", url=img_url)
            
            # Attributes
            found_attrs = []
            for attr_key in ATTRIBUTE_COLUMNS:
                if record.get(attr_key) is not None:
                    original = clean_text(record.get(attr_key))
                    final = get_attribute_value_with_fallback(attr_key, original)
                    if final:
                        found_attrs.append((attr_key, final))
            
            if found_attrs:
                attrs = etree.SubElement(ad, f"{{{ns}}}attributes")
                for key, value in found_attrs:
                    attr = etree.SubElement(attrs, f"{{{ns}}}attribute")
                    etree.SubElement(attr, f"{{{ns}}}attributeName").text = key
                    etree.SubElement(attr, f"{{{ns}}}attributeValue").text = value
            
            processed_count += 1
            
        except Exception as e:
            reason = f"Row {row_num} ({vendor_id}): {type(e).__name__} - {str(e)}"
            logger.error(reason)
            error_details.append({"vendorId": vendor_id, "reason": reason})
            skipped_count += 1
    
    logger.info(f"XML generation complete. Processed: {processed_count}, Skipped: {skipped_count}")
    
    return {
        "xml_content": etree.tostring(root, pretty_print=True, xml_declaration=True, encoding='UTF-8'),
        "processed_count": processed_count,
        "skipped_count": skipped_count,
        "error_details": error_details
    }


def save_feed_locally(xml_content):
    """Saves XML locally."""
    try:
        latest_path = os.path.join(XML_STORAGE_DIR, "latest.xml")
        with open(latest_path, 'wb') as f:
            f.write(xml_content)
        
        logger.info("Feed saved locally as latest.xml")
        return {"filename": "latest.xml", "local_path": latest_path}
    except Exception as e:
        logger.error(f"Local save error: {e}")
        raise


def upload_feed_to_cloudinary(xml_content):
    """Uploads to Cloudinary."""
    if not all([cloudinary_cloud_name, cloudinary_api_key, cloudinary_api_secret]):
        raise ConnectionError("Cloudinary not configured")
    
    try:
        result = cloudinary.uploader.upload(
            file=xml_content,
            resource_type="raw",
            public_id="marktplaats_latest",
            folder="XMLs/Netherlands/Marktplaats",
            overwrite=True
        )
        logger.info(f"Uploaded to Cloudinary: {result.get('secure_url')}")
        return result
    except Exception as e:
        logger.error(f"Cloudinary upload error: {e}")
        raise


@app.route('/generate-feed')
def generate_and_upload_feed():
    """Main endpoint."""
    try:
        logger.info("Starting feed generation...")
        records = get_sheet_data()
        
        generation_result = generate_xml_feed(records)
        local_save = save_feed_locally(generation_result["xml_content"])
        
        cloudinary_result = None
        cloudinary_error = None
        
        try:
            cloudinary_result = upload_feed_to_cloudinary(generation_result["xml_content"])
        except Exception as e:
            cloudinary_error = str(e)
            logger.warning(f"Cloudinary upload failed: {cloudinary_error}")
        
        response = {
            "status": "success",
            "message": "Feed generated and saved locally.",
            "local_feed_url": "/xml",
            "stats": {
                "total_rows": len(records),
                "added_to_xml": generation_result["processed_count"],
                "skipped": generation_result["skipped_count"],
                "errors": generation_result["error_details"]
            }
        }
        
        if cloudinary_result:
            response["cloudinary_feed_url"] = cloudinary_result.get('secure_url')
            response["message"] += " Uploaded to Cloudinary."
        elif cloudinary_error:
            response["cloudinary_error"] = cloudinary_error
        
        return jsonify(response), 200
        
    except Exception as e:
        logger.error(f"Feed generation failed: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/xml')
def serve_xml():
    """Serves XML file."""
    try:
        return send_from_directory(XML_STORAGE_DIR, "latest.xml", as_attachment=False)
    except FileNotFoundError:
        return jsonify({"error": "No XML available. Generate feed first."}), 404


@app.route('/xml-debug')
def serve_xml_debug():
    """Debug XML endpoint."""
    return serve_xml()


def validate_xml_against_schema(xml_path, xsd_path):
    """Validates XML against XSD."""
    try:
        schema_doc = etree.parse(xsd_path)
        schema = etree.XMLSchema(schema_doc)
        xml_doc = etree.parse(xml_path)
        
        is_valid = schema.validate(xml_doc)
        
        results = {
            "is_valid": is_valid,
            "errors": [],
            "warnings": []
        }
        
        if not is_valid:
            for error in schema.error_log:
                results["errors"].append({
                    "line": error.line,
                    "column": error.column,
                    "message": error.message,
                    "domain": error.domain_name,
                    "type": error.type_name
                })
        
        return results
        
    except Exception as e:
        return {
            "is_valid": False,
            "errors": [{"message": f"Validation failed: {str(e)}"}],
            "warnings": []
        }


@app.route('/validate-xml')
def validate_current_xml():
    """Validates current XML."""
    try:
        xml_path = os.path.join(XML_STORAGE_DIR, "latest.xml")
        xsd_path = "schema.xsd"
        
        if not os.path.exists(xml_path):
            return jsonify({"error": "No XML file. Generate feed first."}), 404
        
        if not os.path.exists(xsd_path):
            return jsonify({"error": "XSD schema not found."}), 404
        
        results = validate_xml_against_schema(xml_path, xsd_path)
        
        return jsonify({
            "status": "success",
            "validation_results": results,
            "xml_file": "latest.xml",
            "schema_file": "schema.xsd"
        }), 200
        
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/')
def index():
    """Main page."""
    return '''
    <h1>Marktplaats Feed Generator</h1>
    <p><em>Updated: October 2025</em></p>
    <h3>Endpoints:</h3>
    <ul>
        <li><a href="/generate-feed">/generate-feed</a> - Generate and upload</li>
        <li><a href="/xml">/xml</a> - View XML (static URL)</li>
        <li><a href="/xml-debug">/xml-debug</a> - Debug XML</li>
        <li><a href="/validate-xml">/validate-xml</a> - Validate XSD</li>
    </ul>
    '''


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080

))
    app.run(debug=True, host='0.0.0.0', port=port)
