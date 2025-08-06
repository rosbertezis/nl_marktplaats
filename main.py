import gspread
from oauth2client.service_account import ServiceAccountCredentials
from flask import Flask, Response, jsonify
from lxml import etree
import os
import logging

# --- Настройки ---
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
CREDENTIALS_FILE = os.getenv('GOOGLE_CREDENTIALS_PATH', 'credentials.json')
SPREADSHEET_NAME = 'Netherlands inventory'
WORKSHEET_NAME = 'Marktplaats'

# --- Настройка логирования ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Конфигурация Cloudinary из переменных окружения ---
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
    logger.info("Cloudinary configured successfully.")
else:
    logger.warning("Cloudinary credentials are not fully set in environment variables.")


# --- Инициализация приложения Flask ---
app = Flask(__name__)

# --- Список колонок с дополнительными изображениями ---
IMAGE_COLUMNS = ['img_2', 'img_3', 'img_4', 'img_5', 'img_6', 'img_7', 'img_8', 'img_9', 'img_10']

def get_sheet_data():
    """Подключается к Google Sheets и получает данные."""
    logger.info("Attempting to access Google Sheets...")
    try:
        if not os.path.exists(CREDENTIALS_FILE):
            raise FileNotFoundError(f"Credentials file not found: {CREDENTIALS_FILE}")
            
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, SCOPE)
        client = gspread.authorize(creds)
        sheet = client.open(SPREADSHEET_NAME).worksheet(WORKSHEET_NAME)
        records = sheet.get_all_records()
        logger.info(f"Successfully retrieved {len(records)} records from Google Sheets.")
        return records
    except Exception as e:
        logger.error(f"Error accessing Google Sheets: {e}", exc_info=True)
        raise

def validate_record(record):
    """Проверяет, что запись содержит обязательные поля."""
    required_fields = ['vendorId', 'title', 'description', 'categoryId', 'priceType']
    
    for field in required_fields:
        if not record.get(field):
            return False, f"Missing required field: {field}"
    
    price_type = str(record.get('priceType', '')).upper()
    if price_type in ['FIXED_PRICE', 'BIDDING_FROM']:
        if not record.get('price'):
            return False, f"Price required for priceType: {price_type}"
    
    return True, None

def clean_text(text):
    """Очищает и подготавливает текст для XML."""
    if text is None:
        return ""
    return str(text).strip()

def is_valid_url(url):
    """Проверяет, что URL корректный."""
    if not url:
        return False
    url_str = str(url).strip()
    return url_str.startswith(('http://', 'https://'))

def collect_additional_images(record):
    """Собирает дополнительные изображения из колонок img_2...img_10."""
    additional_images = []
    
    for img_col in IMAGE_COLUMNS:
        img_url = record.get(img_col)
        if img_url and is_valid_url(img_url):
            additional_images.append(clean_text(img_url))
    
    return additional_images

def generate_xml_feed(records):
    """Генерирует XML-фид из записей таблицы."""
    logger.info("Starting XML feed generation...")
    # Создаем корневой элемент XML согласно документации Marktplaats
    ns = "http://admarkt.marktplaats.nl/schemas/1.0"
    root = etree.Element(f"{{{ns}}}ads", nsmap={'admarkt': ns})
    
    processed_count = 0
    skipped_count = 0
    
    for i, record in enumerate(records):
        row_num = i + 2 # +2 потому что get_all_records() не включает заголовок, а нумерация в таблице с 1
        try:
            # Фильтруем только активные объявления
            available = str(record.get('Available', '')).upper()
            if available not in ['TRUE', 'YES', '1']:
                skipped_count += 1
                continue
            
            # Валидация записи
            is_valid, error_msg = validate_record(record)
            if not is_valid:
                logger.warning(f"Skipping invalid record on row {row_num}: {error_msg}")
                skipped_count += 1
                continue
            
            ad_element = etree.SubElement(root, f"{{{ns}}}ad")
            
            # --- Основные поля ---
            etree.SubElement(ad_element, f"{{{ns}}}vendorId").text = clean_text(record.get('vendorId'))
            etree.SubElement(ad_element, f"{{{ns}}}title").text = clean_text(record.get('title'))
            etree.SubElement(ad_element, f"{{{ns}}}description").text = etree.CDATA(clean_text(record.get('description')))
            etree.SubElement(ad_element, f"{{{ns}}}categoryId").text = str(int(float(record.get('categoryId'))))
            etree.SubElement(ad_element, f"{{{ns}}}priceType").text = clean_text(record.get('priceType')).upper()
            
            if record.get('price'):
                etree.SubElement(ad_element, f"{{{ns}}}price").text = str(int(float(record.get('price'))))
            if record.get('url') and is_valid_url(record.get('url')):
                etree.SubElement(ad_element, f"{{{ns}}}url").text = clean_text(record.get('url'))
            
            # --- Изображения ---
            all_images = []
            if record.get('image_link') and is_valid_url(record.get('image_link')):
                all_images.append(clean_text(record.get('image_link')))
            all_images.extend(collect_additional_images(record))
            
            if all_images:
                media_element = etree.SubElement(ad_element, f"{{{ns}}}media")
                for img_url in all_images:
                    etree.SubElement(media_element, f"{{{ns}}}image", url=img_url)

            # --- Атрибуты ---
            if record.get('attributes'):
                attrs_element = etree.SubElement(ad_element, f"{{{ns}}}attributes")
                for attr_pair in record.get('attributes').split(','):
                    attr_pair = attr_pair.strip()
                    if ':' in attr_pair:
                        key, value = attr_pair.split(':', 1)
                        if key.strip() and value.strip():
                            attr = etree.SubElement(attrs_element, f"{{{ns}}}attribute")
                            etree.SubElement(attr, f"{{{ns}}}attributeName").text = key.strip()
                            etree.SubElement(attr, f"{{{ns}}}attributeValue").text = value.strip()
            
            processed_count += 1
        except Exception as e:
            logger.error(f"Error processing record on row {row_num}: {e}")
            skipped_count += 1
            continue
            
    logger.info(f"Feed generation completed. Processed: {processed_count}, Skipped: {skipped_count}")
    return etree.tostring(root, pretty_print=True, xml_declaration=True, encoding='UTF-8')

def upload_feed_to_cloudinary(xml_content):
    """Загружает XML-фид в Cloudinary."""
    if not (cloudinary_cloud_name and cloudinary_api_key and cloudinary_api_secret):
        raise ConnectionError("Cloudinary credentials are not configured.")

    logger.info("Starting upload to Cloudinary...")
    try:
        upload_result = cloudinary.uploader.upload(
            file=xml_content,
            resource_type="raw",
            public_id="marktplaats_feed",
            folder="XMLs/Netherlands/Marktplaats",
            upload_preset="nl-marktplaats-feed-uploader",
            overwrite=True
        )
        logger.info(f"Successfully uploaded to Cloudinary. Public ID: {upload_result.get('public_id')}")
        return upload_result
    except Exception as e:
        logger.error(f"Error uploading to Cloudinary: {e}", exc_info=True)
        raise

@app.route('/generate-feed')
def generate_and_upload_feed():
    """
    Запускает процесс: чтение из GSheets -> генерация XML -> загрузка в Cloudinary.
    Возвращает JSON с результатом.
    """
    try:
        logger.info("Feed generation and upload process started by accessing the URL.")
        records = get_sheet_data()
        xml_feed_content = generate_xml_feed(records)
        upload_result = upload_feed_to_cloudinary(xml_feed_content)
        
        final_url = upload_result.get('secure_url')
        response_data = {
            "status": "success",
            "message": "Feed has been successfully generated and uploaded to Cloudinary.",
            "cloudinary_feed_url": final_url,
            "version": upload_result.get('version'),
            "processed_at": upload_result.get('created_at')
        }
        logger.info(f"Process finished successfully. Feed URL: {final_url}")
        return jsonify(response_data), 200

    except Exception as e:
        logger.error(f"The process failed: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/health')
def health_check():
    """Простая проверка состояния сервиса."""
    return Response("OK", status=200)

@app.route('/')
def index():
    """Главная страница с информацией о сервисе."""
    return """
    <html>
        <head><title>Marktplaats Feed Generator</title></head>
        <body>
            <h1>Marktplaats XML Feed Generator</h1>
            <p>Сервис активен. Для запуска синхронизации перейдите по ссылке:</p>
            <p><a href="/generate-feed">/generate-feed</a></p>
        </body>
    </html>
    """

if __name__ == '__main__':
    # Эта часть нужна для локального тестирования
    # Render использует gunicorn и переменную окружения PORT, поэтому этот блок не будет выполняться на сервере
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=port)
