/*对ODS层数据进行清洗，添加unix时间戳字段及日期字段方便使用*/
DROP TABLE IF EXISTS
  dwd_events_game;
CREATE TABLE IF NOT EXISTS
  dwd_events_game AS
WITH
  base1 AS (
  SELECT
    DISTINCT EventID AS event_id,
    PlayerID AS player_id,
    EventTimestamp AS event_timestamp,
    unixepoch(EventTimestamp) AS unix_event_timestamp,
    DATE(EventTimestamp) AS event_date,
    EventType AS event_type,
    EventDetails AS event_details,
    DeviceType AS device_type,
    Location AS location
  FROM
    ods_events_game
  WHERE
    EventID IS NOT NULL
    AND PlayerID IS NOT NULL
    AND EventTimestamp IS NOT NULL
    AND EventType IS NOT NULL
    AND EventDetails IS NOT NULL
    AND DeviceType IS NOT NULL
    AND Location IS NOT NULL/*清洗数据，去掉有空值的行*/
    AND EventID LIKE 'E%'
    AND PlayerID LIKE 'P%'/*清洗数据，去掉不符合数据规则的行*/ ),
  session_event_id_count_table AS (/*有的session事件少报或者漏报，从而使得SessionStart至SessionEnd不能闭合，将其找出*/
  SELECT
    player_id,
    event_id,
    session_start_unix_event_timestamp,
    COUNT(event_id) OVER (PARTITION BY player_id, session_start_unix_event_timestamp) AS session_event_id_count
  FROM (
    SELECT
      event_id,
      player_id,
      unix_event_timestamp - CAST (SUBSTR(event_details, 11, INSTR(event_details, ' seconds') - 11) AS FLOAT) AS session_start_unix_event_timestamp
    FROM
      base1
    WHERE
      event_type = 'SessionEnd'
    UNION ALL
    SELECT
      event_id,
      player_id,
      unix_event_timestamp AS session_start_unix_event_timestamp
    FROM
      base1
    WHERE
      event_type = 'SessionStart' ) ),
  base2 AS (/*把不能闭合的SessionStart和SessionEnd清除*/
  SELECT
    event_id,
    player_id,
    event_timestamp,
    unix_event_timestamp,
    event_date,
    event_type,
    event_details,
    device_type,
    location
  FROM
    base1
  WHERE
    event_id NOT IN (
    SELECT
      event_id
    FROM
      session_event_id_count_table
    WHERE
      session_event_id_count = 1 ) ),
  base3 AS (/*把不能闭合的SessionStart和SessionEnd中间的其余事件清除*/
  SELECT
    event_id,
    player_id,
    event_timestamp,
    unix_event_timestamp,
    event_date,
    event_type,
    event_group
  FROM (
    SELECT
      event_id,
      player_id,
      event_timestamp,
      unix_event_timestamp,
      event_date,
      event_type,
      event_rank1,
      event_rank2,
      event_rank3,
      IFNULL(event_rank1 - event_rank2, event_rank3) AS event_group
    FROM (
      SELECT
        event_id,
        player_id,
        event_timestamp,
        unix_event_timestamp,
        event_date,
        event_type,
        ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY event_timestamp) AS event_rank1
      FROM
        base2 )
    LEFT JOIN (
      SELECT
        event_id,
        player_id,
        event_timestamp,
        unix_event_timestamp,
        event_date,
        event_type,
        ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY event_timestamp) AS event_rank2
      FROM
        base2
      WHERE
        event_type NOT IN ('SessionStart',
          'SessionEnd') )
    USING
      ( event_id,
        player_id,
        event_timestamp,
        unix_event_timestamp,
        event_date,
        event_type )
    LEFT JOIN (
      SELECT
        event_id,
        player_id,
        event_timestamp,
        unix_event_timestamp,
        event_date,
        event_type,
        ROW_NUMBER() OVER (PARTITION BY player_id, event_type ORDER BY event_timestamp) * 2 - 1 AS event_rank3
      FROM
        base2
      WHERE
        event_type IN ('SessionStart',
          'SessionEnd') )
    USING
      ( event_id,
        player_id,
        event_timestamp,
        unix_event_timestamp,
        event_date,
        event_type ) )
  WHERE
    event_group%2 = 1 )
SELECT
  event_id,
  player_id,
  event_timestamp,
  unix_event_timestamp,
  event_date,
  event_type,
  event_group,
  event_details,
  device_type,
  location
FROM
  base3
LEFT JOIN
  base1
USING
  ( event_id,
    player_id,
    event_timestamp,
    unix_event_timestamp,
    event_date,
    event_type )