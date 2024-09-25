  /*用于生成ADS层数据，供DAU报表使用*/
DROP TABLE IF EXISTS
  ads_dau;
CREATE TABLE IF NOT EXISTS
  ads_dau AS
SELECT
  device_type,
  location,
  event_date,
  COUNT(DISTINCT player_id) AS active_players
FROM
  dws_event_date
GROUP BY
  device_type,
  location,
  event_date