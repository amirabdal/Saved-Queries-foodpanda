with base as (
select format_date('%V',order_date) as WEEK,order_code,customer_code from `fulfillment-dwh-production.pandata_report.country_BD_bd_analytics` where order_date>= current_date()-80 and is_gross_order and status_name = '631 - Logistics Cancellation'
),
 incident as(
  select b.*,a.contact_id from base b join `fulfillment-dwh-production.curated_data_shared.all_contacts` a on b.order_code=a.order_id and  a.global_entity_id='FP_BD' and created_date >= current_date()-80 and a.contact_reason_l3 in ('Unable to contact','Address change','Wrong address/pinpoint')
)
SELECT DISTINCT
  i.*,
  c.mobile_country_code,
  c.mobile_number,

FROM 
incident i join
  `fulfillment-dwh-production.pandata_curated.cus_customers` c
  on i.customer_code=c.code
WHERE 
  global_entity_id = "FP_BD" 


