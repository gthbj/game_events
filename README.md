# 任务1
[CSV交付，从Google Drive下载](https://drive.google.com/file/d/1Uxc784LVfjQbX87hTLfcC90_-xto80hN/view?usp=sharing)

相较于原代码做出的修改：
1. 玩家数量增加到100k个，游戏行为增加到180天。
2. 给用户随机分配第一个事件的开始时间，模拟用户新增。
3. 随着时间的增长，使用户在单位时间的平均事件量越来越少，模拟用户的流失以及控制数据量级。
4. 随机去除一些行和一些行内的某个字段的内容，模拟需要被清洗的数据。

# 任务2
通过sql进行数仓分层处理，使用Python的sqlite3库将数据写入sqlite数据库中。
[sqlite数据库文件交付，从Google Drive下载](https://drive.google.com/file/d/1EMRcCvkOqIC_aIE2EMBU65FZa-y1jxF4/view?usp=sharing)

## ODS
生成的CSV文件不做修改，直接传入数据库作为ODS层数据

## DWD
1. 去除有空值的行，以及不符合字段内容规则的行。在一行中若某个字段的值为null，则去除；另外，例如EventId都以"E"开头，PlayerID都以"P"开头，若不符合该规则，则去除。
2. 去除无法闭合的SessionStart事件和SessionEnd事件。可能会存在少报漏报的SessionStart事件或SessionEnd事件，在这种情况下，一个session的头尾无法闭合，将其去除。
3. 去除无法闭合的SessionStart事件和SessionEnd事件中间的其余事件。将一个session内的SessionStart事件和SessionEnd事件去除后，给其余的事件也去除。
4. 增加unix_timestamp字段和event_date字段方便后续处理。

## DWS
按照event_date和session两种维度，聚合出这两种主题的表，方便后续使用。

## ADS
按照四个需求聚合出了三张ADS表，可供可视化看板直接使用，无需再进行数据处理。

# 任务3
使用了metabase作为数据可视化的工具，将看板部署在本地。
[metabase官方网站](https://www.metabase.com/)

## 数据集成
![数据集成-sqlite数据源](https://raw.githubusercontent.com/gthbj/game_events/refs/heads/main/metabase_sqlite_database.jpg)

## 看板演示
看板可通过筛选器筛选系统、国家和事件日期。
DAU和IAP收入分别做了曲线图；平均会话时长（分钟）和每次会话的平均社交互动次数做在了表格中。
具体如下图所示：
![看板主页](https://raw.githubusercontent.com/gthbj/game_events/refs/heads/main/metabase_dashboard.jpg)
![筛选器-系统](https://raw.githubusercontent.com/gthbj/game_events/refs/heads/main/device_type.jpg)
![筛选器-国家](https://raw.githubusercontent.com/gthbj/game_events/refs/heads/main/location.jpg)
![筛选器-日期](https://raw.githubusercontent.com/gthbj/game_events/refs/heads/main/date.jpg)
