WITH base AS (
  SELECT 
    DISTINCT ordered_at_date_local,
    code,
    status_updated_at_local,
    status_name
  FROM 
    `fulfillment-dwh-production.pandata_curated.pd_orders` 
  WHERE 
    created_date_utc >= CURRENT_DATE() - 10 
    AND global_entity_id = 'FP_BD' 
    AND is_gross_order 
    AND status_name IN ('64 - customer cancelled - vendor informed', '631 - Logistics Cancellation')
)
  SELECT 
    b.*, 
    TIME(DATETIME(d.rider_picked_up_at, 'Asia/Dhaka')) AS picked_up_time,
    ABS(TIMESTAMP_DIFF(TIMESTAMP(DATETIME(d.rider_picked_up_at, 'Asia/Dhaka')), TIMESTAMP(b.status_updated_at_local), MINUTE)) as time_difference 
  FROM 
    base b 
  JOIN 
    `fulfillment-dwh-production.curated_data_shared.orders` o 
  ON 
    b.code = o.platform_order_code 
    AND o.entity.id = 'FP_BD' 
    AND o.created_date >= CURRENT_DATE() - 10
  CROSS JOIN 
    UNNEST(o.deliveries) AS d
WHERE 
ABS(TIMESTAMP_DIFF(TIMESTAMP(DATETIME(d.rider_picked_up_at, 'Asia/Dhaka')), TIMESTAMP(b.status_updated_at_local), MINUTE)) <= 5
