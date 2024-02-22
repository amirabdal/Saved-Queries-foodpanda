WITH red_flag_customers AS (
  SELECT DISTINCT
    created_date_localtime,
    order_id,
    customer_id,
    exploit_status
  FROM 
    `fulfillment-dwh-production.curated_data_shared.exploit_claim_intents_new` x,
    UNNEST(claim_attempts) c,
    UNNEST(filter_results) f
  WHERE 
    global_entity_id = 'FP_BD'
    AND contact_reason_l1 IN ('Post-Delivery')
    AND created_date_localtime >= CURRENT_DATE() - 7
    AND f.filter_result = 'true'
    AND f.filter_color = 'red'
),
false_positive_customer AS (
  SELECT DISTINCT
    rfc.created_date_localtime,
    rfc.order_id,
    rfc.customer_id,
    is_genuine
  FROM 
    red_flag_customers rfc 
    JOIN `fulfillment-dwh-production.pandata_report.country_BD_fraud_genuine_labels` fl
    ON rfc.customer_id = fl.customer_code
),
info AS (
  SELECT DISTINCT
    fpc.created_date_localtime,
    fpc.order_id,
    fpc.customer_id,
    cus.name,
    cus.mobile_number
  FROM 
     false_positive_customer fpc
     LEFT JOIN `fulfillment-dwh-production.pandata_curated.cus_customers` cus 
     ON fpc.customer_id = cus.code
  WHERE 
    global_entity_id = "FP_BD"
),
order_history AS (
  SELECT 
    i.customer_id,
    i.name,
    COALESCE(COUNT(DISTINCT bd.order_code), 0) AS last_6_months_orders,
    COUNT(IF(bd.business_type_apac='dmart', bd.order_code, NULL)) AS last_6_months_pandamart_orders,
    AVG(bd.gmv_gross_local) AS last_6_months_avg_gmv
  FROM 
    info i 
    LEFT JOIN `fulfillment-dwh-production.pandata_report.country_BD_bd_analytics` bd 
    ON i.customer_id = bd.customer_code  
  WHERE 
    order_date >= CURRENT_DATE() - 180 
    AND (status_code IN (621, 612, 68) OR decline_reason_title LIKE "%Customer never received the order%")
  GROUP BY 
    i.customer_id, i.name
),
associated_account AS (
  SELECT 
    i.customer_id,
    a.associated_customer_count  
  FROM 
    info i 
    LEFT JOIN `fulfillment-dwh-production.curated_data_shared_cdp.customers` a 
    ON i.customer_id = a.customer_id 
  WHERE 
    a.snapshot_date_utc = CURRENT_DATE() - 3
    AND a.global_entity_id = "FP_BD"
),
refunds AS (
  SELECT 
    i.customer_id,
    COUNT(DISTINCT r.order_id) AS last_6_months_refunds 
  FROM 
    info i 
    JOIN `fulfillment-dwh-production.curated_data_shared.comp_and_refund_events` r 
    ON i.customer_id = r.customer_id 
    AND r.global_entity_id = 'FP_BD' 
    AND r.order_created_date >= CURRENT_DATE() - 180
  GROUP BY 
    i.customer_id
),
incidents AS (
  SELECT 
    i.customer_id,
    COUNT(DISTINCT a.contact_id) AS last_6_months_incidents 
  FROM 
    info i 
    JOIN `fulfillment-dwh-production.curated_data_shared.all_contacts` a 
    ON i.customer_id = a.stakeholder_id 
  WHERE 
    a.global_entity_id = 'FP_BD' 
    AND a.created_date >= CURRENT_DATE() - 180
  GROUP BY 
    i.customer_id
),
order_basket AS (
  SELECT 
    fpc.order_id,
    pd.initial_gfv_local 
  FROM 
    false_positive_customer fpc 
    JOIN `fulfillment-dwh-production.pandata_curated.pd_orders` p 
    ON fpc.order_id = p.code 
    JOIN `fulfillment-dwh-production.pandata_report.regional_apac_pd_orders_agg_accounting` pd 
    ON p.uuid = pd.uuid 
  WHERE 
    p.global_entity_id = 'FP_BD' 
    AND p.created_date_utc >= CURRENT_DATE() - 7 
    AND pd.created_date_utc >= CURRENT_DATE() - 7 
),
csat_comments AS (
  SELECT 
    fpc.order_id,
    STRING_AGG(csat.customer_comments, ' ') AS csat_comments 
  FROM 
    false_positive_customer fpc 
    LEFT JOIN `fulfillment-dwh-production.pandata_report.regional_apac_cx_ac_csat_responses` csat 
    ON fpc.order_id = csat.pd_order_code 
    AND csat.global_entity_id = 'FP_BD' 
    AND csat.created_date_utc >= CURRENT_DATE() - 7 
  GROUP BY 
    fpc.order_id
)
SELECT 
  fpc.created_date_localtime AS order_date,
  fpc.order_id AS order_code,
  i.customer_id AS customer_code,
  i.name AS customer_name,
  i.mobile_number AS customer_mobile_number,
  oh.last_6_months_orders,
  safe_divide(oh.last_6_months_pandamart_orders, oh.last_6_months_orders) as last_6_months_pandamart_order_percentage,
  oh.last_6_months_avg_gmv,
  aa.associated_customer_count,
  safe_divide(r.last_6_months_refunds, oh.last_6_months_orders) as last_6_months_refund_perecentage,
  safe_divide(inc.last_6_months_incidents, oh.last_6_months_orders) as incident_rate,
  ob.initial_gfv_local,
  csc.csat_comments 
FROM 
  false_positive_customer fpc
  JOIN info i ON fpc.customer_id = i.customer_id
  LEFT JOIN order_history oh ON i.customer_id = oh.customer_id
  LEFT JOIN associated_account aa ON i.customer_id = aa.customer_id
  LEFT JOIN refunds r ON i.customer_id = r.customer_id
  LEFT JOIN incidents inc ON i.customer_id = inc.customer_id
  LEFT JOIN order_basket ob ON fpc.order_id = ob.order_id
  LEFT JOIN csat_comments csc ON fpc.order_id = csc.order_id;
