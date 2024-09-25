  /*用于生成ADS层数据，供session会话时长及互动次数报表使用*/
DROP TABLE IF EXISTS
  ads_session;
CREATE TABLE IF NOT EXISTS
  ads_session AS
SELECT
  device_type,
  location,
  session_start_date,
  COUNT(DISTINCT player_id) AS session_users,
  COUNT(1) AS session_count,
  ROUND(CAST (SUM(session_duration) AS FLOAT) / 60, 1) AS session_min_duration,
  SUM(social_interaction_count) AS sum_social_interaction_count
FROM
  dws_session
GROUP BY
  device_type,
  location,
  session_start_date