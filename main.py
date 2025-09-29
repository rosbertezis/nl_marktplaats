import gspread
from flask import Flask, jsonify, send_from_directory
from lxml import etree
import os
import logging
from dotenv import load_dotenv

# --- Загрузка переменных окружения для локальной разработки ---
load_dotenv()

# --- Настройки ---
SPREADSHEET_NAME = os.getenv('SPREADSHEET_NAME')
WORKSHEET_NAME = os.getenv('WORKSHEET_NAME')

# --- Константы с ограничениями из XSD-схемы ---
XSD_VENDOR_ID_MAX_LENGTH = 64
XSD_ALLOWED_PRICE_TYPES = {
    "FIXED_PRICE", "BIDDING", "NEGOTIALE", "NOT_APPLICABLE",
    "CREDIBLE_BID", "SWAP", "FREE", "RESERVED",
    "SEE_DESCRIPTION", "ON_DEMAND", "BIDDING_FROM"
}

# --- Список колонок, которые будут преобразованы в атрибуты XML ---
ATTRIBUTE_COLUMNS = [
    'area_sqm',
    'property_type',
    'deal_type',
]

# --- Настройка логирования ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Конфигурация Cloudinary ---
cloudinary_cloud_name = os.getenv('CLOUDINARY_CLOUD_NAME')
cloudinary_api_key = os.getenv('CLOUDINARY_API_KEY')
cloudinary_api_secret = os.getenv('CLOUDINARY_API_SECRET')

if all([cloudinary_cloud_name, cloudinary_api_key, cloudinary_api_secret]):
    import cloudinary
    import cloudinary.uploader
    cloudinary.config(
        cloud_name=cloudinary_cloud_name,
        api_key=cloudinary_api_key,
        api_secret=cloudinary_api_secret,
        secure=True
    )
    logger.info("Cloudinary настроен успешно.")
else:
    logger.warning("Переменные окружения для Cloudinary не настроены. Загрузка фида будет недоступна.")

# --- Инициализация приложения Flask ---
app = Flask(__name__)

# --- Конфигурация локального хранилища для XML ---
XML_STORAGE_DIR = 'xml_storage'
XML_FILENAME = 'marktplaats_feed.xml'
# Создаем папку при старте приложения, если ее нет
os.makedirs(XML_STORAGE_DIR, exist_ok=True)


# --- Список колонок с дополнительными изображениями ---
IMAGE_COLUMNS = ['img_2', 'img_3', 'img_4', 'img_5', 'img_6', 'img_7', 'img_8', 'img_9', 'img_10']

def get_sheet_data():
    """Подключается к Google Sheets и получает данные, используя переменные окружения."""
    try:
        creds_dict = {
            "type": "service_account",
            "project_id": os.getenv('GCP_PROJECT_ID'),
            "private_key_id": os.getenv('GCP_PRIVATE_KEY_ID'),
            "private_key": os.getenv('GCP_PRIVATE_KEY', '').replace('\\n', '\n'),
            "client_email": os.getenv('GCP_CLIENT_EMAIL'),
            "client_id": os.getenv('GCP_CLIENT_ID'),
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{os.getenv('GCP_CLIENT_EMAIL', '').replace('@', '%40')}",
            "universe_domain": "googleapis.com"
        }
        
        if not all([creds_dict['project_id'], creds_dict['private_key'], creds_dict['client_email']]):
            raise ValueError("Одна или несколько GCP переменных окружения не установлены.")

        client = gspread.service_account_from_dict(creds_dict)
        sheet = client.open(SPREADSHEET_NAME).worksheet(WORKSHEET_NAME)
        records = sheet.get_all_records()
        logger.info(f"Успех: получено {len(records)} записей из таблицы '{SPREADSHEET_NAME}'.")
        return records
        
    except Exception as e:
        logger.error(f"Ошибка доступа к Google Sheets: {e}")
        raise

def validate_record(record):
    """Проверяет наличие обязательных полей."""
    required_fields = ['vendorId', 'title', 'description', 'categoryId', 'priceType']
    for field in required_fields:
        if not record.get(field):
            return False, f"отсутствует обязательное поле '{field}'"
    return True, None

def validate_xsd_constraints(record):
    """Проверяет данные на соответствие ограничениям XSD."""
    vendor_id = str(record.get('vendorId', ''))
    if len(vendor_id) > XSD_VENDOR_ID_MAX_LENGTH:
        return False, f"поле 'vendorId' превышает макс. длину {XSD_VENDOR_ID_MAX_LENGTH} символов"

    price_type = str(record.get('priceType', '')).upper()
    if price_type and price_type not in XSD_ALLOWED_PRICE_TYPES:
        return False, f"значение '{price_type}' в поле 'priceType' не разрешено схемой"

    try:
        category_id = int(float(record.get('categoryId')))
        if category_id <= 0:
            return False, f"поле 'categoryId' должно быть положительным числом, а получено: {category_id}"
    except (ValueError, TypeError, AttributeError):
        return False, f"поле 'categoryId' ('{record.get('categoryId')}') не является числом"
    return True, None

def clean_text(text):
    return str(text).strip() if text is not None else ""

def get_attribute_value_with_fallback(key, value):
    """Применяет фоллбэк-логику для area_sqm."""
    if key == 'area_sqm':
        try:
            numeric_value = int(float(value))
            return '1' if numeric_value <= 0 else str(numeric_value)
        except (ValueError, TypeError):
            return '1'
    return value

def get_price_with_fallback(price_value, price_type):
    """Применяет фоллбэк-логику для цены."""
    types_requiring_price = ['FIXED_PRICE', 'BIDDING_FROM']
    if price_type in types_requiring_price:
        try:
            numeric_price = int(float(price_value))
            return 1 if numeric_price <= 0 else numeric_price
        except (ValueError, TypeError, AttributeError):
            return 1
    return 0

def generate_xml_feed(records):
    ns = "http://admarkt.marktplaats.nl/schemas/1.0"
    root = etree.Element(f"{{{ns}}}ads", nsmap={'admarkt': ns})
    
    processed_count, skipped_count = 0, 0
    error_details = []
    
    for i, record in enumerate(records):
        row_num = i + 2
        vendor_id = record.get('vendorId') or f'ROW-{row_num}'
        
        try:
            if str(record.get('Available', '')).upper() not in ['TRUE', 'YES', '1']:
                continue

            is_valid, error_msg = validate_record(record)
            if not is_valid:
                raise ValueError(f"Ошибка валидации - {error_msg}.")
            
            is_xsd_valid, xsd_error_msg = validate_xsd_constraints(record)
            if not is_xsd_valid:
                raise ValueError(f"Несоответствие XSD - {xsd_error_msg}.")

            ad_element = etree.SubElement(root, f"{{{ns}}}ad")
            
            etree.SubElement(ad_element, f"{{{ns}}}vendorId").text = clean_text(record.get('vendorId'))
            etree.SubElement(ad_element, f"{{{ns}}}title").text = clean_text(record.get('title'))
            etree.SubElement(ad_element, f"{{{ns}}}description").text = etree.CDATA(clean_text(record.get('description')))
            etree.SubElement(ad_element, f"{{{ns}}}categoryId").text = str(int(float(record.get('categoryId'))))
            
            price_type_val = clean_text(record.get('priceType')).upper()
            etree.SubElement(ad_element, f"{{{ns}}}priceType").text = price_type_val
            
            price_content = get_price_with_fallback(record.get('price'), price_type_val)
            etree.SubElement(ad_element, f"{{{ns}}}price").text = str(price_content)
            
            # --- Изображения ---
            all_images = [clean_text(record.get('image_link'))] if str(record.get('image_link')).startswith('http') else []
            all_images.extend(clean_text(record.get(img_col)) for img_col in IMAGE_COLUMNS if str(record.get(img_col)).startswith('http'))
            
            if all_images:
                media_element = etree.SubElement(ad_element, f"{{{ns}}}media")
                for img_url in all_images:
                    etree.SubElement(media_element, f"{{{ns}}}image", url=img_url)

            # --- Атрибуты ---
            found_attributes = []
            for attr_key in ATTRIBUTE_COLUMNS:
                original_value = clean_text(record.get(attr_key))
                if original_value:
                    final_value = get_attribute_value_with_fallback(attr_key, original_value)
                    found_attributes.append((attr_key, final_value))
            
            if found_attributes:
                attrs_element = etree.SubElement(ad_element, f"{{{ns}}}attributes")
                for key, value in found_attributes:
                    attr = etree.SubElement(attrs_element, f"{{{ns}}}attribute")
                    etree.SubElement(attr, f"{{{ns}}}attributeName").text = key
                    etree.SubElement(attr, f"{{{ns}}}attributeValue").text = value
            
            processed_count += 1
        except Exception as e:
            reason = f"Строка {row_num} (ID: {vendor_id}): пропущена из-за ошибки - {e}."
            logger.warning(reason)
            error_details.append({"vendorId": vendor_id, "reason": reason})
            skipped_count += 1
            
    logger.info(f"Генерация XML завершена. Добавлено: {processed_count}, Пропущено: {skipped_count}")
    return {
        "xml_content": etree.tostring(root, pretty_print=True, xml_declaration=True, encoding='UTF-8'),
        "processed_count": processed_count, "skipped_count": skipped_count, "error_details": error_details
    }

@app.route('/generate-feed')
def trigger_feed_generation():
    """Основной эндпоинт для генерации и загрузки фида."""
    try:
        records = get_sheet_data()
        result = generate_xml_feed(records)
        xml_content = result["xml_content"]

        # Сохраняем файл локально на сервере Render
        local_filepath = os.path.join(XML_STORAGE_DIR, XML_FILENAME)
        with open(local_filepath, 'wb') as f:
            f.write(xml_content)
        logger.info(f"Фид сохранен локально: {local_filepath}")

        # Пытаемся загрузить в Cloudinary
        cloudinary_url, cloudinary_error = None, None
        if all([cloudinary_cloud_name, cloudinary_api_key, cloudinary_api_secret]):
            try:
                upload_result = cloudinary.uploader.upload(
                    xml_content, resource_type="raw", public_id="marktplaats_feed",
                    folder="XMLs/Netherlands/Marktplaats", overwrite=True
                )
                cloudinary_url = upload_result.get('secure_url')
                logger.info(f"Фид загружен в Cloudinary: {cloudinary_url}")
            except Exception as e:
                cloudinary_error = str(e)
                logger.error(f"Ошибка загрузки в Cloudinary: {e}")
        
        # Формируем итоговый JSON-ответ
        response = {
            "status": "success",
            "message": "Фид успешно сгенерирован и сохранен.",
            "local_feed_url": f"https://{os.getenv('RENDER_EXTERNAL_HOSTNAME', 'localhost')}/xml-raw",
            "stats": {
                "total_rows_in_sheet": len(records),
                "rows_processed": result["processed_count"],
                "rows_skipped": result["skipped_count"],
                "errors": result["error_details"]
            },
            "cloudinary_info": {"url": cloudinary_url, "error": cloudinary_error}
        }
        return jsonify(response), 200

    except Exception as e:
        logger.error(f"Полный провал процесса генерации фида: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/xml-raw')
def serve_raw_xml():
    """Отдает локально сохраненный XML файл."""
    try:
        return send_from_directory(XML_STORAGE_DIR, XML_FILENAME, mimetype='application/xml')
    except FileNotFoundError:
        return jsonify({"error": "Файл не найден. Сначала сгенерируйте фид через /generate-feed."}), 404

@app.route('/')
def index():
    """Главная страница с ссылками на эндпоинты."""
    hostname = os.getenv('RENDER_EXTERNAL_HOSTNAME', 'localhost:8080')
    return f'''
    <h1>Генератор фида для Marktplaats</h1>
    <p>Сервис активен. Доступные эндпоинты:</p>
    <ul>
        <li><a href="/generate-feed">/generate-feed</a> - Запустить генерацию и выгрузку фида.</li>
        <li><a href="/xml-raw">/xml-raw</a> - Посмотреть текущий XML фид.</li>
    </ul>
    <p><strong>Примечание:</strong> Ссылка на <code>/xml-raw</code> всегда отдает последнюю сгенерированную версию.</p>
    '''

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(debug=True, host='0.0.0.0', port=port)
