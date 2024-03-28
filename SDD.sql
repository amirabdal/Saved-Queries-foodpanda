WITH contact_automation AS (
  SELECT
    global_entity_id,
    order_id AS order_code
  FROM `fulfillment-dwh-production.curated_data_shared.all_contacts`
  WHERE created_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH), MONTH)
    AND LOWER(stakeholder) LIKE "%customer%"
    AND global_entity_id = "FP_BD"

  UNION DISTINCT

  SELECT
    global_entity_id,
    order_id AS order_code
  FROM `fulfillment-dwh-production.curated_data_shared.helpcenter_automations_v2`
  WHERE created_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH), MONTH)
  AND global_entity_id = "FP_BD"
),

pd_basket_updates AS (
  SELECT
    global_entity_id,
    order_id
  FROM `fulfillment-dwh-production.pandata_curated.pd_basket_updates`
  WHERE created_date_utc >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH), MONTH)
  AND global_entity_id = "FP_BD"
  QUALIFY ROW_NUMBER() OVER(PARTITION BY global_entity_id, order_id ORDER BY created_at_utc DESC) = 1
),

stacked_orders_data AS (
  SELECT
    lg_orders.entity.id AS global_entity_id,
    lg_orders.platform_order_code AS order_code,
    SUM(deliveries.stacked_deliveries) AS stacked_deliveries_count
  FROM `fulfillment-dwh-production.curated_data_shared.orders` AS lg_orders
  LEFT JOIN UNNEST(lg_orders.deliveries) AS deliveries
  WHERE lg_orders.created_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH), MONTH)
    AND deliveries.is_primary
    AND entity.id IN ("FP_BD", "ODR_BD")
  GROUP BY
    global_entity_id,
    order_code
)

SELECT
  pd_orders.uuid,
  COALESCE(DATETIME(deliveries.rider_dropped_off_at,lg_orders.timezone), DATETIME(lg_orders.created_at,lg_orders.timezone)) AS ordered_at_local,
  COALESCE(DATE(deliveries.rider_dropped_off_at, lg_orders.timezone), DATE(lg_orders.created_at, lg_orders.timezone)) AS ordered_at_date_local,
  pd_orders.global_entity_id,
  zone_id AS lg_zone_id,
  shared_countries.name AS country,
  pd_orders.code AS order_code,
  COALESCE(regional_apac_pd_orders_agg_business_types.business_type_apac_incl_reclassified, "NULL") AS vertical_type,
  IF(pd_orders.payment_type.code_type IN ("COD", "invoice"), "COD", "OP") AS payment_type_code_type,
  IF(rider_deliveries.delivery_distance > (long_distance_thresholds.distance_threshold_in_km * 1000), "Long Distance", "Short Distance") AS delivery_distance,
  IF(stacked_orders_data.stacked_deliveries_count > 0, TRUE, FALSE) AS is_stacked_order,
  pd_orders.is_gross_order,
  pd_orders.is_valid_order,
  regional_apac_pd_orders_agg_sb_subscriptions.is_subscriber_order,
  regional_apac_pd_orders_agg_sb_subscriptions.is_subscription_benefit_order,
  pd_orders_agg_cp_orders.is_corporate_order,
  rider_deliveries.delivery_distance AS delivery_distance_in_meters,
  lg_orders.timings.actual_delivery_time/60 AS DT,
  lg_orders.timings.promised_delivery_time/60 AS PDT,
  SAFE_DIVIDE((lg_orders.timings.actual_delivery_time - lg_orders.timings.promised_delivery_time), 60) AS dt_from_pdt,
  IF(pd_customers_agg_orders.first_order_all_at_utc IS NOT NULL, TRUE, FALSE) AS is_new_customers,

  IF((pd_orders.is_failed_order
        OR contact_automation.order_code IS NOT NULL
        OR pd_basket_updates.order_id IS NOT NULL
        OR (SAFE_DIVIDE((lg_orders.timings.actual_delivery_time - lg_orders.timings.promised_delivery_time), 60) > 5)
        OR (SAFE_DIVIDE((lg_orders.timings.actual_delivery_time - lg_orders.timings.promised_delivery_time), 60) < -15)
        OR (rider_deliveries.delivery_distance <= (long_distance_thresholds.distance_threshold_in_km * 1000)
            AND NOT lg_orders.is_preorder
            AND SAFE_DIVIDE(lg_orders.timings.actual_delivery_time, 60) > 40)
            ), FALSE, TRUE) AS is_seamless_order,
  IF((pd_orders.is_failed_order
      OR contact_automation.order_code IS NOT NULL
      OR pd_basket_updates.order_id IS NOT NULL
      OR (SAFE_DIVIDE((lg_orders.timings.actual_delivery_time - lg_orders.timings.promised_delivery_time), 60) > 5)
      OR (SAFE_DIVIDE((lg_orders.timings.actual_delivery_time - lg_orders.timings.promised_delivery_time), 60) < -15)
      OR (rider_deliveries.delivery_distance <= (long_distance_thresholds.distance_threshold_in_km * 1000)
          AND NOT lg_orders.is_preorder
          AND SAFE_DIVIDE(lg_orders.timings.actual_delivery_time, 60) > 40)
          ), TRUE, FALSE) AS is_non_seamless_order,

  IF(pd_orders.is_failed_order, TRUE, FALSE) AS is_failed_order,
  IF(contact_automation.order_code IS NOT NULL, TRUE, FALSE) AS is_ticket_automation_order,
  IF(pd_basket_updates.order_id IS NOT NULL, TRUE, FALSE) AS is_basket_updated_order,
  IF(
    (
      rider_deliveries.delivery_distance <= (
        long_distance_thresholds.distance_threshold_in_km * 1000
      ) AND NOT lg_orders.is_preorder AND SAFE_DIVIDE(lg_orders.timings.actual_delivery_time, 60) > 40
    ),
    TRUE,
    FALSE
  ) AS is_sdd_dt_more,

  IF((((SAFE_DIVIDE((lg_orders.timings.actual_delivery_time - lg_orders.timings.promised_delivery_time), 60) > 5) OR
      (SAFE_DIVIDE((lg_orders.timings.actual_delivery_time - lg_orders.timings.promised_delivery_time), 60) < -15)) AND
        NOT pd_orders.is_failed_order), TRUE, FALSE) AS is_order_not_on_time

FROM `fulfillment-dwh-production.curated_data_shared.orders` AS lg_orders
LEFT JOIN UNNEST(lg_orders.deliveries) AS deliveries
      ON deliveries.is_primary
LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_countries` AS shared_countries
      ON shared_countries.global_entity_id = lg_orders.entity.id
LEFT JOIN UNNEST(lg_orders.deliveries) AS rider_deliveries
      ON rider_deliveries.is_primary
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders` AS pd_orders
      ON pd_orders.global_entity_id = lg_orders.entity.id
      AND pd_orders.code = lg_orders.platform_order_code
      AND pd_orders.created_date_utc >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH), MONTH)
LEFT JOIN `fulfillment-dwh-production.pandata_report.regional_apac_logistics_long_distance_threshold_gsheet` AS long_distance_thresholds
      ON long_distance_thresholds.lg_country_code = lg_orders.country_code
LEFT JOIN pd_basket_updates
      ON pd_basket_updates.global_entity_id = pd_orders.global_entity_id
      AND pd_basket_updates.order_id = pd_orders.id
LEFT JOIN contact_automation
      ON lg_orders.entity.id = contact_automation.global_entity_id
      AND lg_orders.platform_order_code = contact_automation.order_code

LEFT JOIN `fulfillment-dwh-production.pandata_report.regional_apac_pd_orders_agg_sb_subscriptions` AS regional_apac_pd_orders_agg_sb_subscriptions
      ON lg_orders.entity.id = regional_apac_pd_orders_agg_sb_subscriptions.global_entity_id
      AND lg_orders.platform_order_code = regional_apac_pd_orders_agg_sb_subscriptions.order_code
      AND regional_apac_pd_orders_agg_sb_subscriptions.created_date_utc >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH), MONTH)
LEFT JOIN `fulfillment-dwh-production.pandata_report.regional_apac_pd_orders_agg_cp_orders` AS pd_orders_agg_cp_orders
      ON pd_orders.uuid = pd_orders_agg_cp_orders.uuid
      AND pd_orders_agg_cp_orders.created_date_utc >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH), MONTH)
LEFT JOIN `fulfillment-dwh-production.pandata_report.regional_apac_pd_orders_agg_business_types` AS regional_apac_pd_orders_agg_business_types
      ON pd_orders.uuid = regional_apac_pd_orders_agg_business_types.order_uuid
      AND regional_apac_pd_orders_agg_business_types.ordered_at_date_local >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH), MONTH)

LEFT JOIN `fulfillment-dwh-production.pandata_report.regional_apac_mkt_customers_agg_orders_dates` AS pd_customers_agg_orders
      ON pd_customers_agg_orders.uuid = pd_orders.pd_customer_uuid
      AND TIMESTAMP(pd_customers_agg_orders.first_order_all_at_utc) = TIMESTAMP(pd_orders.ordered_at_utc)
LEFT JOIN stacked_orders_data
      ON stacked_orders_data.global_entity_id = lg_orders.entity.id
      AND stacked_orders_data.order_code = lg_orders.platform_order_code
WHERE shared_countries.management_entity = "Foodpanda APAC"
  AND lg_orders.entity.id IN ("FP_BD", "ODR_BD")
  AND lg_orders.created_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH), MONTH)
  AND pd_orders.is_gross_order
  AND pd_orders.is_own_delivery
ORDER BY 1 DESC
