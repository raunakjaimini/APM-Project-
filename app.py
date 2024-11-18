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

# Initialize historical data lists
cpu_usage_history = []
memory_usage_history = []
system_log = []  # Temporary storage for batched log entries

def initialize_database():
    """
    Sets up the SQLite database with the required table structure.
    """
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            cpu_usage_percent REAL,
            memory_usage_percent REAL,
            cpu_frequency_mhz REAL,
            cpu_temperature_celsius REAL,
            available_memory_mb REAL
        )
    """)
    conn.commit()
    conn.close()

def collect_metrics():
    """
    Collects real-time CPU and memory metrics.
    Returns a dictionary with CPU percentage, memory usage, CPU frequency,
     and available memory.
    """
    cpu_percent = psutil.cpu_percent()  # Get CPU usage as a percentage
    memory = psutil.virtual_memory()  # Get memory usage details

    # Append data for historical tracking
    cpu_usage_history.append(cpu_percent)
    memory_usage_history.append(memory.percent)

    return {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "cpu_usage_percent": cpu_percent,
        "memory_usage_percent": memory.percent,
        "cpu_frequency_mhz": psutil.cpu_freq().current,
        "available_memory_mb": memory.available / (1024 * 1024)  # Convert bytes to MB
    }
    
def predict_usage(self):
    """
    Predicts future CPU and memory usage growth based on past data.
    Calculates the slope (growth rate) manually without using prebuilt functions.
    """
    # Check if we have enough data points for a reliable prediction
    if len(self.cpu_usage_history) < 10:
        return {"error": "Insufficient data for prediction"}
    
    # Convert data history to series for easy computation
    cpu_series = pd.Series(self.cpu_usage_history)
    memory_series = pd.Series(self.memory_usage_history)
    
    # Create a time index to represent observation intervals
    time_index = np.arange(len(cpu_series))
    
    # Calculate mean values for time and usage data
    time_mean = np.mean(time_index)
    cpu_mean = np.mean(cpu_series)
    memory_mean = np.mean(memory_series)
    
    # Calculate the slope (growth rate) and intercept for CPU usage
    cpu_numerator = np.sum((time_index - time_mean) * (cpu_series - cpu_mean))
    cpu_denominator = np.sum((time_index - time_mean) ** 2)
    cpu_slope = cpu_numerator / cpu_denominator
    cpu_intercept = cpu_mean - (cpu_slope * time_mean)
    
    # Calculate the slope (growth rate) and intercept for memory usage
    memory_numerator = np.sum((time_index - time_mean) * (memory_series - memory_mean))
    memory_denominator = np.sum((time_index - time_mean) ** 2)
    memory_slope = memory_numerator / memory_denominator
    memory_intercept = memory_mean - (memory_slope * time_mean)
    
    # Predictions based on the calculated slopes (growth rates)
    predictions = {
        "predicted_cpu_growth_rate_percent": cpu_slope,
        "predicted_memory_growth_rate_percent": memory_slope,
        "cpu_usage_prediction_next_interval": cpu_slope * (len(cpu_series) + 1) + cpu_intercept,
        "memory_usage_prediction_next_interval": memory_slope * (len(memory_series) + 1) + memory_intercept
    }
    return predictions



def calculate_trends(self):
    """
    Calculate statistical trends for historical CPU and memory usage.
    Uses `pd.Series` to calculate max, average, and standard deviation,
    which simplifies aggregation operations on historical data.
    """
    cpu_series = pd.Series(self.cpu_usage_history)
    memory_series = pd.Series(self.memory_usage_history)
    
    trends = {
        "cpu_peak_usage_percent": cpu_series.max(),
        "memory_peak_usage_percent": memory_series.max(),
        "cpu_avg_usage_percent": cpu_series.mean(),
        "memory_avg_usage_percent": memory_series.mean(),
        "cpu_usage_std_dev": cpu_series.std(),
        "memory_usage_std_dev": memory_series.std(),
    }
    
    return trends

def insert_data_to_db(data_batch):
    """
    Inserts a batch of data into the SQLite database.
    :param data_batch: List of dictionaries containing metric data.
    """
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.executemany("""
        INSERT INTO metrics (timestamp, cpu_usage_percent, memory_usage_percent,
                             cpu_frequency_mhz, cpu_temperature_celsius, available_memory_mb)
        VALUES (:timestamp, :cpu_usage_percent, :memory_usage_percent,
                :cpu_frequency_mhz, :cpu_temperature_celsius, :available_memory_mb)
    """, data_batch)
    conn.commit()
    conn.close()

def log_metrics():
    """
    Logs metrics at regular intervals, buffers them, and inserts into the database in batches.
    """
    global system_log
    while True:
        metrics = collect_metrics()
        system_log.append(metrics)
        trends = calculate_trends()
        predictions = predict_usage()

        # Insert into the database when batch size is reached
        if len(system_log) >= BATCH_SIZE:
            insert_data_to_db(system_log)
            system_log = []  # Clear the buffer

        time.sleep(LOG_INTERVAL)

def fetch_data_for_analysis():
    """
    Fetches data from the SQLite database for real-time analysis.
    Returns a pandas DataFrame containing the data.
    """
    conn = sqlite3.connect(DB_NAME)
    query = "SELECT * FROM metrics ORDER BY id DESC LIMIT 100"  # Fetch the latest 100 records
    data = pd.read_sql_query(query, conn)
    conn.close()
    return data

def plot_real_time_graphs():
    """
    Plots four real-time graphs: CPU usage, Memory usage, CPU frequency, and Available memory.
    Updates the graphs with new data instead of creating new graphs.
    """
    fig, axes = plt.subplots(2, 2, figsize=(12, 8))
    plt.subplots_adjust(hspace=0.4)

    # Initialize deque for storing live data
    cpu_usage = deque(maxlen=100)
    memory_usage = deque(maxlen=100)
    cpu_frequency = deque(maxlen=100)
    available_memory = deque(maxlen=100)

    def update_graphs(frame):
        # Fetch the latest data
        data = fetch_data_for_analysis()

        # Update deque objects
        cpu_usage.clear()
        cpu_usage.extend(data["cpu_usage_percent"])
        memory_usage.clear()
        memory_usage.extend(data["memory_usage_percent"])
        cpu_frequency.clear()
        cpu_frequency.extend(data["cpu_frequency_mhz"])
        available_memory.clear()
        available_memory.extend(data["available_memory_mb"])

        # Update each subplot
        axes[0, 0].clear()
        axes[0, 0].plot(cpu_usage, label="CPU Usage (%)")
        axes[0, 0].set_title("CPU Usage (%)")
        axes[0, 0].legend()

        axes[0, 1].clear()
        axes[0, 1].plot(memory_usage, label="Memory Usage (%)")
        axes[0, 1].set_title("Memory Usage (%)")
        axes[0, 1].legend()

        axes[1, 0].clear()
        axes[1, 0].plot(cpu_frequency, label="CPU Frequency (MHz)")
        axes[1, 0].set_title("CPU Frequency (MHz)")
        axes[1, 0].legend()

        axes[1, 1].clear()
        axes[1, 1].plot(available_memory, label="Available Memory (MB)")
        axes[1, 1].set_title("Available Memory (MB)")
        axes[1, 1].legend()

    ani = FuncAnimation(fig, update_graphs, interval=1000)  # Update every second
    plt.show()

# Main function to coordinate data logging and real-time plotting
def main():
    initialize_database()

    # Start data logging and real-time plotting in parallel using threading
    log_thread = threading.Thread(target=log_metrics)
    plot_thread = threading.Thread(target=plot_real_time_graphs)

    log_thread.start()
    plot_thread.start()

    # Join threads to main to keep them running
    log_thread.join()
    plot_thread.join()

# Run the main function
if __name__ == "__main__":
    main()
