import gspread
from oauth2client.service_account import ServiceAccountCredentials
from flask import Flask, Response
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

# --- Инициализация приложения Flask ---
app = Flask(__name__)

# --- Маппинг колонок согласно требованиям ---
COLUMN_MAPPING = {
    # Поля для фильтрации (не попадают в XML)
    'Available': 'filter_available',
    'Centre_description': 'ignore',
    'price (mirror)': 'ignore',
    
    # Основные обязательные поля (согласно документации Marktplaats)
    'vendorId': 'vendorId',
    'title': 'title', 
    'description': 'description',
    'categoryId': 'categoryId',
    'priceType': 'priceType',
    'price': 'price',
    
    # Опциональные поля
    'sellerName': 'sellerName',
    'phoneNumber': 'phoneNumber',
    'url': 'url',
    'vanityUrl': 'vanityUrl',
    'emailAdvertiser': 'emailAdvertiser',
    'status': 'status',
    
    # Изображения
    'image_link': 'image_link',  # главное изображение
    
    # Поля для атрибутов
    'area_sqm': 'attr_area_sqm',
    'property_type': 'attr_property_type', 
    'deal_type': 'attr_deal_type'
}

# Список колонок для игнорирования
IGNORE_COLUMNS = ['Centre_description', 'price (mirror)']

# Список колонок с дополнительными изображениями
IMAGE_COLUMNS = ['img_2', 'img_3', 'img_4', 'img_5', 'img_6', 'img_7', 'img_8', 'img_9', 'img_10']

def get_sheet_data():
    """Подключается к Google Sheets и получает данные."""
    try:
        if not os.path.exists(CREDENTIALS_FILE):
            raise FileNotFoundError(f"Credentials file not found: {CREDENTIALS_FILE}")
            
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, SCOPE)
        client = gspread.authorize(creds)
        sheet = client.open(SPREADSHEET_NAME).worksheet(WORKSHEET_NAME)
        records = sheet.get_all_records()
        logger.info(f"Successfully retrieved {len(records)} records from Google Sheets")
        return records
    except Exception as e:
        logger.error(f"Error accessing Google Sheets: {e}")
        raise

def validate_record(record):
    """Проверяет, что запись содержит обязательные поля."""
    required_fields = ['vendorId', 'title', 'description', 'categoryId', 'priceType']
    
    for field in required_fields:
        if not record.get(field):
            return False, f"Missing required field: {field}"
    
    # Проверяем, что для FIXED_PRICE и BIDDING_FROM есть цена
    price_type = str(record.get('priceType', '')).upper()
    if price_type in ['FIXED_PRICE', 'BIDDING_FROM']:
        if not record.get('price'):
            return False, f"Price required for priceType: {price_type}"
    
    return True, None

def clean_text(text):
    """Очищает и подготавливает текст для XML."""
    if not text:
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

def build_attributes_string(record):
    """Создает строку атрибутов из специальных полей."""
    attributes = []
    
    # Собираем атрибуты из специальных полей
    attr_mapping = {
        'area_sqm': 'Area',
        'property_type': 'Property Type', 
        'deal_type': 'Deal Type'
    }
    
    for field, label in attr_mapping.items():
        value = record.get(field)
        if value:
            attributes.append(f"{label}:{clean_text(value)}")
    
    return ','.join(attributes) if attributes else None

def generate_xml_feed(records):
    """Генерирует XML-фид из записей таблицы."""
    try:
        # Создаем корневой элемент XML согласно документации Marktplaats
        ns = "http://admarkt.marktplaats.nl/schemas/1.0"
        root = etree.Element(f"{{{ns}}}ads", nsmap={'admarkt': ns})
        
        processed_count = 0
        skipped_count = 0
        
        for i, record in enumerate(records):
            try:
                # Фильтруем только активные объявления (Available = TRUE)
                available = str(record.get('Available', '')).upper()
                if available not in ['TRUE', 'YES', '1', 'ACTIVE']:
                    logger.info(f"Skipping inactive record {i+1}: Available = {available}")
                    skipped_count += 1
                    continue
                
                # Валидация записи
                is_valid, error_msg = validate_record(record)
                if not is_valid:
                    logger.warning(f"Skipping invalid record {i+1}: {error_msg}")
                    skipped_count += 1
                    continue
                
                ad_element = etree.SubElement(root, f"{{{ns}}}ad")
                
                # --- Обязательные поля ---
                etree.SubElement(ad_element, f"{{{ns}}}vendorId").text = clean_text(record.get('vendorId'))
                etree.SubElement(ad_element, f"{{{ns}}}title").text = clean_text(record.get('title'))
                
                # Описание с поддержкой HTML
                description_text = clean_text(record.get('description'))
                if description_text:
                    desc_elem = etree.SubElement(ad_element, f"{{{ns}}}description")
                    desc_elem.text = etree.CDATA(description_text)
                
                # CategoryId как число
                category_id = record.get('categoryId')
                try:
                    category_id_num = int(float(category_id)) if category_id else 0
                    etree.SubElement(ad_element, f"{{{ns}}}categoryId").text = str(category_id_num)
                except (ValueError, TypeError):
                    logger.warning(f"Invalid categoryId for record {i+1}: {category_id}")
                    skipped_count += 1
                    continue
                
                etree.SubElement(ad_element, f"{{{ns}}}priceType").text = clean_text(record.get('priceType')).upper()
                
                # --- Условно обязательные поля ---
                price = record.get('price')
                if price:
                    try:
                        price_value = int(float(price))
                        if price_value > 0:
                            etree.SubElement(ad_element, f"{{{ns}}}price").text = str(price_value)
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid price value for record {i+1}: {price}")
                
                # --- Необязательные поля ---
                if record.get('url') and is_valid_url(record.get('url')):
                    etree.SubElement(ad_element, f"{{{ns}}}url").text = clean_text(record.get('url'))
                
                if record.get('vanityUrl'):
                    etree.SubElement(ad_element, f"{{{ns}}}vanityUrl").text = clean_text(record.get('vanityUrl'))
                
                if record.get('phoneNumber'):
                    etree.SubElement(ad_element, f"{{{ns}}}phoneNumber").text = clean_text(record.get('phoneNumber'))
                
                if record.get('sellerName'):
                    etree.SubElement(ad_element, f"{{{ns}}}sellerName").text = clean_text(record.get('sellerName'))
                
                if record.get('emailAdvertiser'):
                    email_value = str(record.get('emailAdvertiser')).upper()
                    if email_value in ['TRUE', 'FALSE']:
                        etree.SubElement(ad_element, f"{{{ns}}}emailAdvertiser").text = email_value.lower()
                
                if record.get('status'):
                    status_value = clean_text(record.get('status')).upper()
                    if status_value in ['ACTIVE', 'PAUSED']:
                        etree.SubElement(ad_element, f"{{{ns}}}status").text = status_value
                
                # --- Изображения ---
                media_element = None
                
                # Основное изображение
                main_image = record.get('image_link')
                if main_image and is_valid_url(main_image):
                    media_element = etree.SubElement(ad_element, f"{{{ns}}}media")
                    etree.SubElement(media_element, f"{{{ns}}}image", url=clean_text(main_image))
                
                # Дополнительные изображения из img_2...img_10
                additional_images = collect_additional_images(record)
                for img_url in additional_images:
                    if media_element is None:
                        media_element = etree.SubElement(ad_element, f"{{{ns}}}media")
                    etree.SubElement(media_element, f"{{{ns}}}image", url=img_url)
                
                # --- Атрибуты ---
                # Собираем атрибуты из специальных полей
                attributes_string = build_attributes_string(record)
                
                # Добавляем пользовательские атрибуты если есть
                if record.get('attributes'):
                    user_attributes = clean_text(record.get('attributes'))
                    if attributes_string:
                        attributes_string += f",{user_attributes}"
                    else:
                        attributes_string = user_attributes
                
                if attributes_string:
                    attrs_element = etree.SubElement(ad_element, f"{{{ns}}}attributes")
                    
                    for attr_pair in attributes_string.split(','):
                        attr_pair = attr_pair.strip()
                        if ':' in attr_pair:
                            key, value = attr_pair.split(':', 1)
                            key = key.strip()
                            value = value.strip()
                            
                            if key and value:
                                attr = etree.SubElement(attrs_element, f"{{{ns}}}attribute")
                                etree.SubElement(attr, f"{{{ns}}}attributeName").text = key
                                etree.SubElement(attr, f"{{{ns}}}attributeValue").text = value
                
                processed_count += 1
                
            except Exception as e:
                logger.error(f"Error processing record {i+1}: {e}")
                skipped_count += 1
                continue
        
        logger.info(f"Feed generation completed. Processed: {processed_count}, Skipped: {skipped_count}")
        
        # Преобразуем XML-дерево в строку
        return etree.tostring(root, pretty_print=True, xml_declaration=True, encoding='UTF-8')
        
    except Exception as e:
        logger.error(f"Error generating XML feed: {e}")
        raise

@app.route('/generate-feed')
def serve_feed():
    """Основной URL, который будет запрашивать Marktplaats."""
    try:
        logger.info("Feed generation request received")
        records = get_sheet_data()
        xml_feed = generate_xml_feed(records)
        
        response = Response(xml_feed, mimetype='application/xml')
        response.headers['Content-Type'] = 'application/xml; charset=utf-8'
        
        logger.info("Feed successfully generated and served")
        return response
        
    except FileNotFoundError as e:
        error_msg = f"Configuration error: {e}"
        logger.error(error_msg)
        return Response(
            f"<?xml version='1.0' encoding='UTF-8'?><error>{error_msg}</error>", 
            status=500, 
            mimetype='application/xml'
        )
    except Exception as e:
        error_msg = f"Failed to generate feed: {e}"
        logger.error(error_msg)
        return Response(
            f"<?xml version='1.0' encoding='UTF-8'?><error>{error_msg}</error>", 
            status=500, 
            mimetype='application/xml'
        )

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
        <p>Сервис для генерации XML фидов из Google Sheets для Marktplaats</p>
        <ul>
            <li><a href="/generate-feed">Generate Feed</a> - XML фид для Marktplaats</li>
            <li><a href="/health">Health Check</a> - Проверка состояния сервиса</li>
        </ul>
        <hr>
        <p><strong>Настройки:</strong></p>
        <ul>
            <li>Таблица: {}</li>
            <li>Лист: {}</li>
        </ul>
    </body>
    </html>
    """.format(SPREADSHEET_NAME, WORKSHEET_NAME)

if __name__ == '__main__':
    # Для локального тестирования
    app.run(debug=True, host='0.0.0.0', port=5000)
