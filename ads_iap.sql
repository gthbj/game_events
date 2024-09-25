  /*用于生成ADS层数据，供IAP报表使用*/
DROP TABLE IF EXISTS
  ads_iap;
CREATE TABLE IF NOT EXISTS
  ads_iap AS
SELECT
  device_type,
  location,
  event_date,
  COUNT(DISTINCT player_id) AS iap_players,
  SUM(in_app_purchase_count) AS sum_in_app_purchase_count,
  ROUND(SUM(in_app_purchase_revenue), 2) AS sum_in_app_purchase_revenue
FROM
  dws_event_date
GROUP BY
  device_type,
  location,
  event_date