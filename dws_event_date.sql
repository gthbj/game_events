  /*针对事件发生日期进行聚合*/
DROP TABLE IF EXISTS
  dws_event_date;
CREATE TABLE IF NOT EXISTS
  dws_event_date AS
WITH
  session_start_table AS (
  SELECT
    device_type,
    location,
    player_id,
    event_date,
    COUNT(1) AS session_start_count
  FROM
    dwd_events_game
  WHERE
    event_type = 'SessionStart'
  GROUP BY
    device_type,
    location,
    player_id,
    event_date ),
  level_complete_table AS (
  SELECT
    device_type,
    location,
    player_id,
    event_date,
    COUNT(1) AS level_complete_count
  FROM
    dwd_events_game
  WHERE
    event_type = 'LevelComplete'
  GROUP BY
    device_type,
    location,
    player_id,
    event_date ),
  social_interaction_table AS (
  SELECT
    device_type,
    location,
    player_id,
    event_date,
    COUNT(1) AS social_interaction_count
  FROM
    dwd_events_game
  WHERE
    event_type = 'SocialInteraction'
  GROUP BY
    device_type,
    location,
    player_id,
    event_date ),
  in_app_purchase_table AS (
  SELECT
    device_type,
    location,
    player_id,
    event_date,
    COUNT(1) AS in_app_purchase_count,
    SUM(CAST (SUBSTR(event_details, 9) AS FLOAT) ) AS in_app_purchase_revenue
  FROM
    dwd_events_game
  WHERE
    event_type = 'InAppPurchase'
  GROUP BY
    device_type,
    location,
    player_id,
    event_date )
SELECT
  device_type,
  location,
  player_id,
  event_date,
  IFNULL(session_start_count, 0) AS session_start_count,
  IFNULL(level_complete_count, 0) AS level_complete_count,
  IFNULL(social_interaction_count, 0) AS social_interaction_count,
  IFNULL(in_app_purchase_count, 0) AS in_app_purchase_count,
  IFNULL(ROUND(in_app_purchase_revenue, 2), 0) AS in_app_purchase_revenue
FROM
  session_start_table
LEFT JOIN
  level_complete_table
USING
  ( device_type,
    location,
    player_id,
    event_date )
LEFT JOIN
  social_interaction_table
USING
  ( device_type,
    location,
    player_id,
    event_date )
LEFT JOIN
  in_app_purchase_table
USING
  ( device_type,
    location,
    player_id,
    event_date )