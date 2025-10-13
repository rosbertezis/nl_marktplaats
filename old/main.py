import gspread
from oauth2client.service_account import ServiceAccountCredentials
from flask import Flask, jsonify
from lxml import etree
import os
import logging
from dotenv import load_dotenv

# --- Загрузка переменных окружения из .env файла ---
load_dotenv()

# --- Настройки ---
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
CREDENTIALS_FILE = os.getenv('GOOGLE_CREDENTIALS_PATH', 'credentials.json')
SPREADSHEET_NAME = os.getenv('SPREADSHEET_NAME')
WORKSHEET_NAME = os.getenv('WORKSHEET_NAME')

# --- НОВЫЙ БЛОК: Константы с ограничениями из XSD-схемы ---
XSD_VENDOR_ID_MAX_LENGTH = 64
XSD_ATTRIBUTE_NAME_MAX_LENGTH = 32
XSD_ATTRIBUTE_VALUE_MAX_LENGTH = 32
XSD_ALLOWED_PRICE_TYPES = {
    "FIXED_PRICE", "BIDDING", "NEGOTIABLE", "NOT_APPLICABLE",
    "CREDIBLE_BID", "SWAP", "FREE", "RESERVED",
    "SEE_DESCRIPTION", "ON_DEMAND", "BIDDING_FROM"
    
}
# --- КОНЕЦ НОВОГО БЛОКА ---
# --- НОВЫЙ БЛОК: Список колонок, которые будут преобразованы в атрибуты XML ---
ATTRIBUTE_COLUMNS = [
    'area_sqm',
    'property_type',
    'deal_type',
    # Добавьте сюда другие колонки-атрибуты, если они появятся
]
# --- КОНЕЦ НОВОГО БЛОКА ---

# --- Настройка логирования (упрощенный формат) ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Конфигурация Cloudinary ---
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
    logger.warning("Переменные окружения для Cloudinary не настроены. Загрузка фида будет недоступна.")


# --- Инициализация приложения Flask ---
app = Flask(__name__)

# --- Список колонок с дополнительными изображениями ---
IMAGE_COLUMNS = ['img_2', 'img_3', 'img_4', 'img_5', 'img_6', 'img_7', 'img_8', 'img_9', 'img_10']

def get_sheet_data():
    """Подключается к Google Sheets и получает данные."""
    try:
        if not os.path.exists(CREDENTIALS_FILE):
            raise FileNotFoundError(f"Файл credentials.json не найден по пути: {CREDENTIALS_FILE}")
            
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, SCOPE)
        client = gspread.authorize(creds)
        sheet = client.open(SPREADSHEET_NAME).worksheet(WORKSHEET_NAME)
        records = sheet.get_all_records()
        logger.info(f"Успех: получено {len(records)} записей из таблицы '{SPREADSHEET_NAME}'.")
        return records
    except Exception as e:
        logger.error(f"Ошибка доступа к Google Sheets: {e}")
        raise

def validate_record(record):
    """Проверяет, что запись содержит обязательные поля."""
    required_fields = ['vendorId', 'title', 'description', 'categoryId', 'priceType']
    
    for field in required_fields:
        if not record.get(field):
            return False, f"отсутствует обязательное поле '{field}'"
    
    # Блок проверки цены был удален, так как теперь мы обрабатываем пустую цену с помощью фоллбэка.

    
    return True, None

    # --- НОВЫЙ БЛОК: Функция для валидации по XSD-ограничениям ---
def validate_xsd_constraints(record):
    """Проверяет данные записи на соответствие XSD-схеме (длины, типы, перечисления)."""
    # Проверка длины vendorId
    vendor_id = str(record.get('vendorId', ''))
    if len(vendor_id) > XSD_VENDOR_ID_MAX_LENGTH:
        return False, f"поле 'vendorId' ('{vendor_id[:10]}...') превышает макс. длину {XSD_VENDOR_ID_MAX_LENGTH} символов"

    # Проверка допустимых значений для priceType
    price_type = str(record.get('priceType', '')).upper()
    if price_type and price_type not in XSD_ALLOWED_PRICE_TYPES:
        return False, f"значение '{price_type}' в поле 'priceType' не разрешено схемой"

    # Проверка, что categoryId является положительным целым числом
    try:
        category_id = int(float(record.get('categoryId')))
        if category_id <= 0:
            return False, f"поле 'categoryId' должно быть положительным числом, а получено: {category_id}"
    except (ValueError, TypeError, AttributeError):
        return False, f"поле 'categoryId' ('{record.get('categoryId')}') не может быть преобразовано в число"

    # Старая проверка атрибутов удалена, так как теперь они собираются из отдельных колонок.

    return True, None

def clean_text(text):
    """Очищает и подготавливает текст для XML."""
    return str(text).strip() if text is not None else ""

def is_valid_url(url):
    """Проверяет, что URL корректный."""
    url_str = str(url).strip()
    return url_str.startswith(('http://', 'https://'))

def get_attribute_value_with_fallback(key, value):
    """Применяет фоллбэк-логику для определенных атрибутов."""
    if key == 'area_sqm':
        try:
            # Пытаемся преобразовать значение в число
            numeric_value = int(float(value))
            # Если значение 0 или меньше, возвращаем '1'
            if numeric_value <= 0:
                return '1'
        except (ValueError, TypeError):
            # Если значение пустое или не является числом (например, "N/A"), возвращаем '1'
            return '1'
    
    # Для всех остальных атрибутов возвращаем их оригинальное значение без изменений
    return value

def get_price_with_fallback(price_value, price_type):
    """
    Преобразует значение цены в целое число с учетом priceType.
    - Для типов, требующих цену, возвращает 1, если цена некорректна, пуста или 0.
    - Для остальных типов возвращает 0, чтобы тег <price> просто присутствовал в XML.
    """
    # Типы цен, для которых цена является обязательной
    types_requiring_price = ['FIXED_PRICE', 'BIDDING_FROM']

    if price_type in types_requiring_price:
        if price_value is None:
            return 1  # Фоллбэк для отсутствующей цены
        try:
            numeric_price = int(float(price_value))
            return 1 if numeric_price <= 0 else numeric_price
        except (ValueError, TypeError):
            return 1  # Фоллбэк для нечисловых значений ("N/A")
    else:
        # Для типов NEGOTIABLE, FREE и т.д. цена не нужна.
        # Возвращаем 0, чтобы удовлетворить требование схемы о наличии тега <price>.
        return 0

def generate_xml_feed(records):
    """Генерирует XML-фид и возвращает словарь со статистикой."""
    ns = "http://admarkt.marktplaats.nl/schemas/1.0"
    root = etree.Element(f"{{{ns}}}ads", nsmap={'admarkt': ns})
    
    processed_count = 0
    skipped_count = 0
    error_details = []
    
    for i, record in enumerate(records):
        # --- БЛОК ДЛЯ ОТЛАДКИ ---
        if i == 0: # Печатаем информацию только для самой первой строки данных
            print("\n\n--- НАЧАЛО ОТЛАДКИ ---")
            print("ОТЛАДКА: Все заголовки, которые видит Python из вашей таблицы:")
            print(list(record.keys()))
            print("--- КОНЕЦ ОТЛАДКИ ---\n\n")
        # --- КОНЕЦ БЛОКА ---
        row_num = i + 2  # Нумерация строк в Google Sheets начинается с 1, +1 для заголовка
        vendor_id = record.get('vendorId') or f'ROW-{row_num}'
        
        try:
            # Пропускаем неактивные объявления
            if str(record.get('Available', '')).upper() not in ['TRUE', 'YES', '1']:
                continue

            # Этап 1: Проверка на наличие обязательных полей
            is_valid, error_msg = validate_record(record)
            if not is_valid:
                reason = f"Строка {row_num} (ID: {vendor_id}): пропущена из-за ошибки валидации - {error_msg}."
                logger.warning(reason)
                error_details.append({"vendorId": vendor_id, "reason": reason})
                skipped_count += 1
                continue # <-- continue теперь внутри if

            # Этап 2: Проверка на соответствие XSD-ограничениям
            is_xsd_valid, xsd_error_msg = validate_xsd_constraints(record)
            if not is_xsd_valid:
                reason = f"Строка {row_num} (ID: {vendor_id}): пропущена из-за несоответствия XSD - {xsd_error_msg}."
                logger.warning(reason)
                error_details.append({"vendorId": vendor_id, "reason": reason})
                skipped_count += 1
                continue # <-- и этот continue тоже внутри своего if
            
            ad_element = etree.SubElement(root, f"{{{ns}}}ad")
            
            # --- Основные поля ---
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
            
            # --- Изображения ---
            all_images = [clean_text(record.get('image_link'))] if record.get('image_link') and is_valid_url(record.get('image_link')) else []
            for img_col in IMAGE_COLUMNS:
                img_url = record.get(img_col)
                if img_url and is_valid_url(img_url):
                    all_images.append(clean_text(img_url))
            
            if all_images:
                media_element = etree.SubElement(ad_element, f"{{{ns}}}media")
                for img_url in all_images:
                    etree.SubElement(media_element, f"{{{ns}}}image", url=img_url)

            # --- Атрибуты ---
            found_attributes = []
            for attr_key in ATTRIBUTE_COLUMNS:
                # Проверяем, что такая колонка есть в данных из таблицы
                if record.get(attr_key) is not None:
                    original_value = clean_text(record.get(attr_key))
                    
                    # Применяем нашу новую фоллбэк-логику к значению
                    final_value = get_attribute_value_with_fallback(attr_key, original_value)
                    
                    # Добавляем атрибут, только если у него есть итоговое значение
                    if final_value:
                        found_attributes.append((attr_key, final_value))

            # Если были найдены атрибуты, создаем XML-блок
            if found_attributes:
                attrs_element = etree.SubElement(ad_element, f"{{{ns}}}attributes")
                for key, value in found_attributes:
                    attr = etree.SubElement(attrs_element, f"{{{ns}}}attribute")
                    etree.SubElement(attr, f"{{{ns}}}attributeName").text = key
                    etree.SubElement(attr, f"{{{ns}}}attributeValue").text = value
            
            processed_count += 1
        except (ValueError, TypeError) as e:
            reason = f"Строка {row_num} (ID: {vendor_id}): пропущена из-за ошибки данных - {e}. Проверьте формат чисел."
            logger.error(reason)
            error_details.append({"vendorId": vendor_id, "reason": reason})
            skipped_count += 1
            
    logger.info(f"Генерация XML завершена. Добавлено: {processed_count}, Пропущено: {skipped_count}")
    
    return {
        "xml_content": etree.tostring(root, pretty_print=True, xml_declaration=True, encoding='UTF-8'),
        "processed_count": processed_count,
        "skipped_count": skipped_count,
        "error_details": error_details
    }

def upload_feed_to_cloudinary(xml_content):
    """Загружает XML-фид в Cloudinary."""
    if not all([cloudinary_cloud_name, cloudinary_api_key, cloudinary_api_secret]):
        raise ConnectionError("Учетные данные Cloudinary не настроены.")
    
    try:
        upload_result = cloudinary.uploader.upload(
            file=xml_content,
            resource_type="raw",
            public_id="marktplaats_feed.xml",
            folder="XMLs/Netherlands/Marktplaats",
            overwrite=True
        )
        logger.info(f"Успех: фид загружен в Cloudinary. URL: {upload_result.get('secure_url')}")
        return upload_result
    except Exception as e:
        logger.error(f"Ошибка загрузки в Cloudinary: {e}")
        raise

@app.route('/generate-feed')
def generate_and_upload_feed():
    """Основной эндпоинт для генерации и загрузки фида."""
    try:
        records = get_sheet_data()
        generation_result = generate_xml_feed(records)
        upload_result = upload_feed_to_cloudinary(generation_result["xml_content"])
        
        return jsonify({
            "status": "success",
            "message": "Фид успешно сгенерирован и загружен.",
            "cloudinary_feed_url": upload_result.get('secure_url'),
            "stats": {
                "total_rows_found_in_sheet": len(records),
                "rows_added_to_xml": generation_result["processed_count"],
                "rows_skipped": generation_result["skipped_count"],
                "errors": generation_result["error_details"]
            }
        }), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/')
def index():
    """Главная страница."""
    return '<h1>Сервис для генерации фида Marktplaats активен.</h1><p>Перейдите на <a href="/generate-feed">/generate-feed</a> для запуска.</p>'

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(debug=True, host='0.0.0.0', port=port)