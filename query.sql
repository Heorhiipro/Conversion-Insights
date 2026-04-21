/* Крок 1: Підготовка базових подій.
Використовуємо СТЕ (Common Table Expression) для фільтрації та очищення даних.
*/
WITH base_events AS (
  SELECT
    -- Перетворюємо рядок дати у формат DATE для зручної фільтрації в Tableau
    PARSE_DATE('%Y%m%d', event_date) AS date,
    -- Конвертуємо мікросекунди у зрозумілий формат часу
    TIMESTAMP_MICROS(event_timestamp) AS event_time,
    event_name,
    user_pseudo_id,
    
    -- Витягуємо ID сесії з масиву параметрів (ключовий елемент для групування подій)
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
    
    -- Технічні характеристики пристрою (допоможуть знайти баги на певних OS)
    device.category AS device_category,
    device.language AS device_language,
    device.operating_system AS device_os,
    
    -- Атрибуція трафіку: звідки прийшов користувач
    traffic_source.source AS source,
    traffic_source.medium AS medium,
    traffic_source.name AS campaign,
    
    -- URL сторінки, на якій сталася подія (для аналізу Landing Pages)
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS landing_page
    
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  -- Обмежуємо період (листопад 2020 - січень 2021), що охоплює період святкових розпродажів
  WHERE _TABLE_SUFFIX BETWEEN '20201101' AND '20210131'
)

/* Крок 2: Фінальна вибірка та агрегація унікальних сесій.
*/
SELECT
  date,
  event_time,
  event_name,
  -- Створюємо глобально унікальний ID сесії. 
  -- Сама по собі ga_session_id не є унікальною в межах всього датасету, 
  -- тому комбінуємо її з ID користувача.
  CAST(CONCAT(user_pseudo_id, CAST(ga_session_id AS STRING)) AS STRING) AS user_session_id,
  
  device_category,
  device_language,
  device_os,
  source,
  medium,
  campaign,
  landing_page
FROM base_events
-- Виключаємо події без ID сесії (зазвичай це технічні помилки або фонові процеси)
WHERE ga_session_id IS NOT NULL 
  -- Залишаємо лише ті івенти, що формують шлях до покупки (E-commerce Funnel)
  AND event_name IN (
    'session_start',     -- Початок візиту
    'view_item',         -- Перегляд картки товару
    'add_to_cart',       -- Додавання в кошик
    'begin_checkout',    -- Початок оформлення
    'add_shipping_info', -- Крок доставки
    'add_payment_info',  -- Крок оплати
    'purchase'           -- Успішна транзакція
  )