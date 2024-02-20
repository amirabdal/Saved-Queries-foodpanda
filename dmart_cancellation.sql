WITH t1 AS (
  SELECT DISTINCT
    a.order_date AS _date_,
    a.order_code AS ORDER_CODE,
    a.vendor_code,
    a.vendor_name,
    b.total_refund_amount
  FROM
    `fulfillment-dwh-production.pandata_report.country_BD_bd_analytics` a
    LEFT JOIN `fulfillment-dwh-production.pandata_report.country_BD_fraud_refunds` b
      ON a.order_code = b.order_code
  WHERE
    a.order_date >= CURRENT_DATE() - 14
    AND a.is_gross_order
    AND LOWER(a.vertical_type) = 'darkstores'
),
t2 AS (
  SELECT DISTINCT
    order_code,
    STRING_AGG(sku, ', ') AS SKU
  FROM
    `fulfillment-dwh-production.pandata_report.dmart__category_sales_dashboard`
  WHERE
    order_date_local >= CURRENT_DATE() - 20
  GROUP BY
    order_code
),
t3 AS (
  SELECT
    x.chat_id AS ID,
    x.contact_reason_l3 AS CCR3,
    x.content AS CHAT_SUBJECT,
    order_id AS ORDER_CODE,
    STRING_AGG(i.content, ' ') AS CHAT,
    REGEXP_EXTRACT_ALL(x.content, r'(https?:\/\/[^\s]+)') AS PICTURE_LINK_SUBJECT,
    STRING_AGG(
      CASE WHEN i.content LIKE "%storage.googleapis.com%" THEN i.content END,
      ' , '
    ) AS PICTURE_LINK_CONTENT
  FROM
    `fulfillment-dwh-production.curated_data_shared.herocare_chats` x
    LEFT JOIN UNNEST(message_history) i
  WHERE
    x.created_date >= CURRENT_DATE() - 14
    AND global_entity_id = "FP_BD"
  GROUP BY
    1, 2, 3, 4
)
SELECT
  t1.*,
  t2.SKU,
  t3.ID,
  t3.CHAT_SUBJECT,
  t3.CCR3,
  t3.PICTURE_LINK_SUBJECT,
  t3.PICTURE_LINK_CONTENT,
  t3.CHAT
FROM
  t1
  LEFT JOIN t2 ON t1.ORDER_CODE = t2.order_code
  LEFT JOIN t3 ON t1.ORDER_CODE = t3.ORDER_CODE;
