with red_flag_customers as (
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
false_positive_customer as (
  SELECT DISTINCT
    rfc.created_date_localtime,
    rfc.order_id,
    rfc.customer_id,
    is_genuine
FROM 
    red_flag_customers rfc 
    join 
    `fulfillment-dwh-production.pandata_report.country_BD_fraud_genuine_labels` fl
    on rfc.customer_id=fl.customer_code
),
info as (
   SELECT DISTINCT
    fpc.created_date_localtime,
    fpc.order_id,
    fpc.customer_id,
    cus.name,
   -- mobile_number.mobile_number_sha512
  FROM 
     false_positive_customer fpc
     left join 
    `fulfillment-dwh-production.pandata_curated.cus_customers` cus
    on fpc.customer_id=cus.code
  WHERE 
    global_entity_id = "FP_BD"
)
--order_history as(
select i.customer_id,i.name,COALESCE(COUNT(DISTINCT bd.order_code), 0) AS last_6_months_orders,Count(IF(bd.business_type_apac='dmart',bd.order_code,NULL)) as last_6_months_pandamart_orders,AVG(bd.gmv_gross_local) as last_6_months_avg_gmv from info i left join `fulfillment-dwh-production.pandata_report.country_BD_bd_analytics` bd on i.customer_id=bd.customer_code  where order_date >=current_date()-180 and  (status_code in (621,612,68) OR decline_reason_title like "%Customer never received the order%")
group by i.customer_id,i.name
),

