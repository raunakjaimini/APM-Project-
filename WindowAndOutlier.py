import sqlite3
import psutil
import time
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from collections import deque
import threading

# Initialize constants
LOG_INTERVAL = 1  # Interval in seconds
DB_NAME = "system_monitor.db"
BATCH_SIZE = 10  # Number of records to batch-insert into the database
OUTLIER_THRESHOLD = 85  # Threshold for CPU and memory usage to detect outliers
DATA_WINDOW = 60  # Number of seconds to analyze for predictions

# Initialize historical data lists
cpu_usage_history = deque(maxlen=DATA_WINDOW)
memory_usage_history = deque(maxlen=DATA_WINDOW)
system_log = []  # Temporary storage for batched log entries

def initialize_database():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            cpu_usage_percent REAL,
            memory_usage_percent REAL,
            cpu_frequency_mhz REAL,
            available_memory_mb REAL
        )
    """)
    conn.commit()
    conn.close()

def collect_metrics():
    cpu_percent = psutil.cpu_percent()
    memory = psutil.virtual_memory()

    # Append data for historical tracking
    cpu_usage_history.append(cpu_percent)
    memory_usage_history.append(memory.percent)

    return {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "cpu_usage_percent": cpu_percent,
        "memory_usage_percent": memory.percent,
        "cpu_frequency_mhz": psutil.cpu_freq().current,
        "available_memory_mb": memory.available / (1024 * 1024)
    }

def detect_outliers():
    """
    Detects if the current CPU or memory usage exceeds the defined threshold.
    Returns a boolean indicating if an alert is triggered.
    """
    latest_cpu = cpu_usage_history[-1]
    latest_memory = memory_usage_history[-1]
    alert = latest_cpu > OUTLIER_THRESHOLD or latest_memory > OUTLIER_THRESHOLD

    if alert:
        print(f"ALERT: High usage detected! CPU: {latest_cpu:.2f}%, Memory: {latest_memory:.2f}%")
    
    return alert

def predict_usage():
    """
    Predicts future CPU and memory usage growth based on the last DATA_WINDOW seconds.
    """
    if len(cpu_usage_history) < 10:  # Ensure sufficient data
        return {"error": "Insufficient data for prediction"}
    
    time_index = np.arange(len(cpu_usage_history))
    cpu_series = pd.Series(cpu_usage_history)
    memory_series = pd.Series(memory_usage_history)

    # Linear regression for CPU usage
    cpu_slope, cpu_intercept = np.polyfit(time_index, cpu_series, 1)
    memory_slope, memory_intercept = np.polyfit(time_index, memory_series, 1)

    predictions = {
        "predicted_cpu_growth_rate_percent": cpu_slope,
        "predicted_memory_growth_rate_percent": memory_slope,
        "cpu_usage_prediction_next_interval": cpu_slope * (len(cpu_series) + 1) + cpu_intercept,
        "memory_usage_prediction_next_interval": memory_slope * (len(memory_series) + 1) + memory_intercept
    }
    return predictions

def log_metrics():
    global system_log
    while True:
        metrics = collect_metrics()
        system_log.append(metrics)
        detect_outliers()  # Check for any outliers
        
        # Print predictions and trends
        predictions = predict_usage()
        if "error" not in predictions:
            print(f"Predicted CPU Usage: {predictions['cpu_usage_prediction_next_interval']:.2f}%")
            print(f"Predicted Memory Usage: {predictions['memory_usage_prediction_next_interval']:.2f}%")

        # Insert into the database when batch size is reached
        if len(system_log) >= BATCH_SIZE:
            insert_data_to_db(system_log)
            system_log = []

        time.sleep(LOG_INTERVAL)

def insert_data_to_db(data_batch):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.executemany("""
        INSERT INTO metrics (timestamp, cpu_usage_percent, memory_usage_percent,
                             cpu_frequency_mhz, available_memory_mb)
        VALUES (:timestamp, :cpu_usage_percent, :memory_usage_percent,
                :cpu_frequency_mhz, :available_memory_mb)
    """, data_batch)
    conn.commit()
    conn.close()

def fetch_data_for_analysis():
    conn = sqlite3.connect(DB_NAME)
    query = "SELECT * FROM metrics ORDER BY id DESC LIMIT 100"
    data = pd.read_sql_query(query, conn)
    conn.close()
    return data

def plot_real_time_graphs():
    fig, axes = plt.subplots(2, 2, figsize=(12, 8))
    plt.subplots_adjust(hspace=0.4)

    cpu_usage = deque(maxlen=100)
    memory_usage = deque(maxlen=100)
    cpu_frequency = deque(maxlen=100)
    available_memory = deque(maxlen=100)

    def update_graphs(frame):
        data = fetch_data_for_analysis()
        cpu_usage.extend(data["cpu_usage_percent"])
        memory_usage.extend(data["memory_usage_percent"])
        cpu_frequency.extend(data["cpu_frequency_mhz"])
        available_memory.extend(data["available_memory_mb"])

        axes[0, 0].clear()
        axes[0, 0].plot(cpu_usage, label="CPU Usage (%)")
        axes[0, 0].legend()

        axes[0, 1].clear()
        axes[0, 1].plot(memory_usage, label="Memory Usage (%)")
        axes[0, 1].legend()

        axes[1, 0].clear()
        axes[1, 0].plot(cpu_frequency, label="CPU Frequency (MHz)")
        axes[1, 0].legend()

        axes[1, 1].clear()
        axes[1, 1].plot(available_memory, label="Available Memory (MB)")
        axes[1, 1].legend()

    ani = FuncAnimation(fig, update_graphs, interval=1000)
    plt.show()

def main():
    initialize_database()
    log_thread = threading.Thread(target=log_metrics)
    plot_thread = threading.Thread(target=plot_real_time_graphs)
    log_thread.start()
    plot_thread.start()
    log_thread.join()
    plot_thread.join()

if __name__ == "__main__":
    main()
