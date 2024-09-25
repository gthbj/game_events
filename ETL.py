import pandas as pd
import sqlite3

# 读取 CSV 文件
csv_data = pd.read_csv(r"C:\Users\Administrator\Desktop\task\game_events.csv")

# 连接 SQLite 数据库
conn = sqlite3.connect(r"C:\Users\Administrator\Desktop\task\game_events_database.db")

# 创建ODS表（如果不存在）
create_table_query = """
CREATE TABLE IF NOT EXISTS ods_events_game (
    EventID text,
    PlayerID text,
    EventTimestamp text,        
    EventType text,        
    EventDetails text,        
    DeviceType text,        
    Location text
);
"""
conn.execute(create_table_query)

# 插入数据
csv_data.to_sql('ods_events_game', conn, if_exists='append', index=False)

# 数仓分层处理
sql_files = [
    r"C:\Users\Administrator\Desktop\task\dwd_events_game.sql",
    r"C:\Users\Administrator\Desktop\task\dws_session.sql",
    r"C:\Users\Administrator\Desktop\task\dws_event_date.sql",
    r"C:\Users\Administrator\Desktop\task\ads_dau.sql",
    r"C:\Users\Administrator\Desktop\task\ads_session.sql",
    r"C:\Users\Administrator\Desktop\task\ads_iap.sql"
]

for sql_file_path in sql_files:
    with open(sql_file_path, 'r', encoding='utf-8') as file:
        sql_content = file.read()
        statements = sql_content.split(';')
        for statement in statements:
            if statement.strip():  
                conn.execute(statement)

# 关闭连接
conn.close()