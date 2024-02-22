select 
distinct
order_date,
rating_date,
order_id,
vendor_code,
vendor_name,
chain_code,
chain_name,
business_type_apac,
expedition_type,
avg_rating,
restaurant_food_rating,
vendor_delivery_rating,
quality_rating,
service_rating,
rider_rating,
packaging_rating,
status_code,
rating_comments
from `fulfillment-dwh-production.pandata_report.country_BD_cx_order_rating` where global_entity_id='FP_BD' AND
order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  AND is_active
  AND TRIM(rating_comments) IS NOT NULL
  AND rating_comments != '';
