WITH ramadan_base AS (
    SELECT DISTINCT 
        order_date,
        order_code,
        status_name,
        payment_type_title,
        CASE
            WHEN business_type_apac = 'kitchens' THEN 'restaurants'
            WHEN business_type_apac = 'concepts' THEN 'restaurants'
            ELSE business_type_apac
        END AS business_type_apac
    FROM 
        `fulfillment-dwh-production.pandata_report.country_BD_bd_analytics` 
    WHERE 
        order_date BETWEEN '2023-03-23' AND '2023-04-21' 
        AND is_gross_order 
        AND is_subscriber_order = FALSE 
        AND is_online_payment
),
delivery_base_delay AS (
    SELECT DISTINCT 
        rb.*,
        d.timings.actual_delivery_time / 60.0 AS DT,
        o.timings.promised_delivery_time / 60.0 AS pdt,
        (d.timings.actual_delivery_time / 60.0 - o.timings.promised_delivery_time / 60.0) AS delay_in_min 
    FROM 
        ramadan_base rb 
    JOIN 
        `fulfillment-dwh-production.curated_data_shared.orders` o 
        ON rb.order_code = o.platform_order_code
        AND o.entity.id = 'FP_BD'
        AND o.created_date BETWEEN '2023-03-23' AND '2023-04-21'
    CROSS JOIN 
        UNNEST(o.deliveries) AS d
    WHERE 
        (d.timings.actual_delivery_time / 60.0 - o.timings.promised_delivery_time / 60.0) >= 20.0
        --AND d.is_primary
),
incident AS (
    SELECT DISTINCT 
        dd.*,
        a.contact_reason_l3 
    FROM 
        delivery_base_delay dd 
    JOIN 
        `fulfillment-dwh-production.curated_data_shared.all_contacts` a 
        ON dd.order_code = a.order_id 
        AND a.stakeholder = 'Customer' 
        AND a.created_date BETWEEN '2023-03-23' AND '2023-04-21' 
        AND (a.contact_reason_l3 IN ('Complain about late order','Request: order will take longer than expected','Request: order is late, does not want to wait','Moderate delay','Delayed delivery','Severe delay') OR lower(a.contact_reason_l3) like "%delay%")
)
SELECT DISTINCT 
    i.*,fo.refund_amount_local
FROM 
    incident i 
JOIN 
    `fulfillment-dwh-production.pandata_report.regional_apac_cx_all_refunds` fo 
    ON i.order_code = fo.order_code
WHERE 
    fo.order_date_local BETWEEN '2023-03-23' AND '2023-04-21' 
    AND fo.global_entity_id = 'FP_BD';
