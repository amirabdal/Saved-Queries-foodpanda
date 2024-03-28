WITH time_frame_counts AS (
  SELECT 
    b.order_delivered_at_local,
    a.created_date,
    a.vertical,
    a.contact_reason_l3,
    FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", TIMESTAMP_SUB(a.creation_timestamp, INTERVAL 6 HOUR), "Asia/Dhaka") AS dhaka_creation_timestamp,
    TIMESTAMP_DIFF(TIMESTAMP(b.order_delivered_at_local), TIMESTAMP(a.creation_timestamp), SECOND) AS time_diff_seconds
  FROM 
    `fulfillment-dwh-production.pandata_report.country_BD_bd_analytics` AS b
  JOIN 
    `fulfillment-dwh-production.curated_data_shared.all_contacts` AS a
  ON 
    b.order_code = a.order_id
  WHERE 
    a.global_entity_id = 'FP_BD'
    AND a.created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    AND a.stakeholder='Customer'
),
time_frame_categories AS (
  SELECT 
    vertical,
    contact_reason_l3,
    CASE
      WHEN time_diff_seconds < 300 THEN '< 5 mins'
      WHEN time_diff_seconds BETWEEN 300 AND 600 THEN '5min-10min'
      WHEN time_diff_seconds BETWEEN 600 AND 1800 THEN '10min-30min'
      WHEN time_diff_seconds BETWEEN 1800 AND 3600 THEN '30min-60min'
      WHEN time_diff_seconds BETWEEN 3600 AND 10800 THEN '1hr - 3hr'
      WHEN time_diff_seconds BETWEEN 10800 AND 21600 THEN '3hr - 6hr'
      WHEN time_diff_seconds BETWEEN 21600 AND 43200 THEN '6hr - 12hr'
      WHEN time_diff_seconds BETWEEN 43200 AND 86400 THEN '12hr - 24hr'
      ELSE '> 24hr'
    END AS time_frame
  FROM 
    time_frame_counts
)
SELECT 
    vertical,
    contact_reason_l3,
    time_frame,
    COUNT(*) AS count,
FROM 
  time_frame_categories
GROUP BY 
   vertical, contact_reason_l3, time_frame
ORDER BY 
  CASE
    WHEN time_frame = '< 5 mins' THEN 1
    WHEN time_frame = '5min-10min' THEN 2
    WHEN time_frame = '10min-30min' THEN 3
    WHEN time_frame = '30min-60min' THEN 4
    WHEN time_frame = '1hr - 3hr' THEN 5
    WHEN time_frame = '3hr - 6hr' THEN 6
    WHEN time_frame = '6hr - 12hr' THEN 7
    WHEN time_frame = '12hr - 24hr' THEN 8
    ELSE 9
  END;
