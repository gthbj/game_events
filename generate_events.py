import simpy
import pandas as pd
import random
from datetime import datetime, timedelta

# Constants
NUM_PLAYERS = 100000  # Total number of players to simulate
SIMULATION_TIME = 180  # Total simulation time in days
EVENT_TYPES = ["LevelComplete", "InAppPurchase", "SocialInteraction"]
COUNTRIES = ["USA", "Singapore", "Brazil", "Japan", "Germany", "India"]
DEVICE_TYPES = ["Android", "iOS"]

# Initialize random seed
random.seed(42)

# Helper functions
def generate_event_details(event_type):
    """Generate event details based on event type"""
    if event_type == "LevelComplete":
        return f"Level: {random.randint(1, 10)}"
    elif event_type == "InAppPurchase":
        return f"Amount: {random.uniform(0.99, 50.99):.2f}"
    elif event_type == "SocialInteraction":
        return f"Joined Guild: G{random.randint(100, 999)}"
    else:
        return "-"

# Player session process
def player_session(env, player_id, device, country, events, initial_min_delay, initial_max_delay):
    min_delay = initial_min_delay
    max_delay = initial_max_delay
    while True:
        # Session start
        start_time = env.now
        events.append({
            "EventID": f"E{10000 + len(events)}",
            "PlayerID": player_id,
            "EventTimestamp": start_time,
            "EventType": "SessionStart",
            "EventDetails": "-",
            "DeviceType": device,
            "Location": country
        })

        # Generate events within a session
        session_duration = random.randint(5 * 60, 2 * 60 * 60)  # Session duration between 5 minutes and 2 hours
        while env.now - start_time < session_duration:
            event_type = random.choice(EVENT_TYPES)
            event_time = env.now
            event_details = generate_event_details(event_type)

            # Add event to the list
            events.append({
                "EventID": f"E{10000 + len(events)}",
                "PlayerID": player_id,
                "EventTimestamp": event_time,
                "EventType": event_type,
                "EventDetails": event_details,
                "DeviceType": device,
                "Location": country
            })

            # Wait for next event
            yield env.timeout(random.randint(10, 600))  # Wait between 10 seconds to 10 minutes

        # Session end
        end_time = env.now
        events.append({
            "EventID": f"E{10000 + len(events)}",
            "PlayerID": player_id,
            "EventTimestamp": end_time,
            "EventType": "SessionEnd",
            "EventDetails": f"Duration: {(end_time - start_time)} seconds",
            "DeviceType": device,
            "Location": country
        })

        growth_factor = random.uniform(1.0, 2.0)# 分配一个随机数，使不同用户的两次会话间隔时间的增长速度不同
        
        # 使得两次会话随机间隔时间上下限越来越大，符合用户逐渐流失的逻辑
        min_delay = min(int(min_delay * growth_factor), 24 * 60 * 60 * 720)
        max_delay = min(int(max_delay * growth_factor), 24 * 60 * 60 * 720)
        yield env.timeout(random.randint(min_delay, max_delay))

# Simulation setup
def setup_simulation():
    env = simpy.Environment()
    events = []

    # Create players with random properties and initial timeouts
    initial_min_delay = 1 * 60 * 60
    initial_max_delay = 24 * 60 * 60

    # 随机分配一个事件开始日期，可理解为用户新增日期
    start_dates = [datetime(2023, 1, 1) + timedelta(days=random.randint(0, 180), hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59)) for _ in range(NUM_PLAYERS)]

    for i in range(NUM_PLAYERS):
        player_id = f"P{10000 + i}"
        device = random.choice(DEVICE_TYPES)
        country = random.choice(COUNTRIES)
        env.process(player_session(env, player_id, device, country, events, initial_min_delay, initial_max_delay))

    # Run the simulation
    env.run(until=SIMULATION_TIME * 24 * 60 * 60)

    df_events = pd.DataFrame(events)
    df_events['EventTimestamp'] = df_events.apply(lambda row: start_dates[int(row['PlayerID'].split('P')[1]) - 10000] + timedelta(seconds=row['EventTimestamp']), axis=1)

    # 随机选择万分之一比例的数据并整行删除，模拟埋点少报的情况
    num_rows = len(df_events)
    rows_to_delete = random.sample(range(num_rows), num_rows // 10000)
    df_events = df_events.drop(rows_to_delete)

    # 随机选择五千分之一比例的数据并随机把这些行中随机一列的数据改为空值，模拟数据缺失的情况
    selected_rows = random.sample(range(len(df_events)), len(df_events) // 5000)
    for row_idx in selected_rows:
        random_col_idx = random.randint(0, len(df_events.columns)-1)
        df_events.iat[row_idx, random_col_idx] = None

    return df_events

df_processed_events = setup_simulation()

df_processed_events.to_csv(r"C:\Users\Administrator\Desktop\task\game_events.csv", index=False)