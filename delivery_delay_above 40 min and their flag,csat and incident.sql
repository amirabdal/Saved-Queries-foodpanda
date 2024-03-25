WITH base AS(
SELECT DISTINCT
    o.created_date,
    o.platform_order_code,
    (d.timings.actual_delivery_time/60.0 - o.timings.promised_delivery_time/60.0) as delay_time,
    CASE 
    WHEN vendor.vertical_type='darkstores' THEN 'dmart'
    WHEN vendor.vertical_type in ('butchery','fruits_and_vegetables','supermarket','fishery','snacks_and_sweets','beauty','stationery_and_books','bakery','flowers_and_plants','health_and_wellness','pets','mother_and_baby','mini_market','home_and_gifts','courier_business','convenience','electronics') THEN 'shops'
    ELSE vendor.vertical_type
  END AS vertical,
  o.order_status

FROM 
    `fulfillment-dwh-production.curated_data_shared.orders` o
CROSS JOIN 
    UNNEST(o.deliveries) AS d
WHERE 
    o.entity.id = 'FP_BD' AND vendor.vertical_type in ('darkstores','butchery','fruits_and_vegetables','supermarket','fishery','snacks_and_sweets','beauty','stationery_and_books','bakery','flowers_and_plants','health_and_wellness','pets','mother_and_baby','mini_market','home_and_gifts','courier_business','convenience','electronics')
    AND o.created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
    AND  (d.timings.actual_delivery_time/60.0 - o.timings.promised_delivery_time/60.0)>40.0

),
incident AS (

SELECT DISTINCT b.created_date,a.order_id,b.vertical,a.contact_reason_l3

FROM base b JOIN `fulfillment-dwh-production.curated_data_shared.all_contacts` a ON 
b.platform_order_code=a.order_id
WHERE 
a.global_entity_id='FP_BD' 
AND a.created_date  >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
AND a.stakeholder='Customer'),

flag_base AS(

SELECT DISTINCT 
    i.*,
    f.exploit_status 
FROM 
    incident i 
LEFT JOIN 
    (
        SELECT 
            order_id,
            FIRST_VALUE(exploit_status) OVER (PARTITION BY order_id ORDER BY created_date) AS exploit_status
        FROM 
            `fulfillment-dwh-production.curated_data_shared.exploit_claim_intents_new` 
            WHERE global_entity_id='FP_BD' AND created_date  >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
    ) f 
ON 
    i.order_id = f.order_id)

SELECT DISTINCT i.created_date,i.order_id,c.csat_score 
FROM flag_base i LEFT JOIN `fulfillment-dwh-production.pandata_report.regional_apac_cx_ac_csat_responses` c 
ON i.order_id=c.pd_order_code
AND c.global_entity_id='FP_BD'
AND c.created_date_local >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)




