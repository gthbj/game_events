  /*针对每一次session进行数据的轻度聚合*/
DROP TABLE IF EXISTS
  dws_session;
CREATE TABLE IF NOT EXISTS
  dws_session AS
WITH
  session_start_table AS (
  SELECT
    device_type,
    location,
    player_id,
    event_date AS session_start_date,
    event_group,
    event_timestamp AS session_start_timestamp
  FROM
    dwd_events_game
  WHERE
    event_type = 'SessionStart' ),
  session_end_table AS (
  SELECT
    device_type,
    location,
    player_id,
    event_group,
    event_timestamp AS session_end_timestamp,
    CAST (SUBSTR(event_details, 11, INSTR(event_details, ' seconds') - 11) AS INT) AS session_duration
  FROM
    dwd_events_game
  WHERE
    event_type = 'SessionEnd' ),
  level_complete_table AS (
  SELECT
    device_type,
    location,
    player_id,
    event_group,
    COUNT(1) AS level_complete_count
  FROM
    dwd_events_game
  WHERE
    event_type = 'LevelComplete'
  GROUP BY
    device_type,
    location,
    player_id,
    event_group ),
  social_interaction_table AS (
  SELECT
    device_type,
    location,
    player_id,
    event_group,
    COUNT(1) AS social_interaction_count
  FROM
    dwd_events_game
  WHERE
    event_type = 'SocialInteraction'
  GROUP BY
    device_type,
    location,
    player_id,
    event_group ),
  in_app_purchase_table AS (
  SELECT
    device_type,
    location,
    player_id,
    event_group,
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
    event_group )
SELECT
  device_type,
  location,
  player_id,
  session_start_date,
  event_group,
  session_start_timestamp,
  session_end_timestamp,
  session_duration,
  IFNULL(level_complete_count, 0) AS level_complete_count,
  IFNULL(social_interaction_count, 0) AS social_interaction_count,
  IFNULL(in_app_purchase_count, 0) AS in_app_purchase_count,
  IFNULL(ROUND(in_app_purchase_revenue, 2), 0) AS in_app_purchase_revenue
FROM
  session_start_table
LEFT JOIN
  session_end_table
USING
  ( device_type,
    location,
    player_id,
    event_group )
LEFT JOIN
  level_complete_table
USING
  ( device_type,
    location,
    player_id,
    event_group )
LEFT JOIN
  social_interaction_table
USING
  ( device_type,
    location,
    player_id,
    event_group )
LEFT JOIN
  in_app_purchase_table
USING
  ( device_type,
    location,
    player_id,
    event_group );