SELECT
    format_date("%B",created_date_utc),
    order_code,
    CASE
    WHEN experiment_name LIKE "%Acqusition%" THEN "Acquisition"
    WHEN experiment_name LIKE "%Bad CX%" THEN "Bad CX"
    WHEN experiment_name LIKE "%Cat 1%" THEN "Cat 1"
    WHEN experiment_name LIKE "%Cat 2%" THEN "Cat 2"
    WHEN experiment_name LIKE "%Cat 3%" THEN "Cat 3"
    WHEN experiment_name LIKE "%Cat 4%" THEN "Cat 4"
    WHEN experiment_name LIKE "%PandaPro%" THEN "PandaPro"
    WHEN experiment_name LIKE "%Fallback%" THEN "Fallback"
    WHEN experiment_name LIKE "%Churning%" THEN "Churning"
  END AS customer_category,

  
  FROM `fulfillment-dwh-production.pandata_report.regional_apac_cx_autocomp_events`
  WHERE experiment_name IS NOT NULL
    AND gcc_experiment_id IS NOT NULL
    AND created_date_utc between '2023-12-01' and '2024-02-29'
    AND global_entity_id='FP_BD'
  
