# import psutil
# import streamlit as st
# import pandas as pd
# import time

# # Initialize data storage with a limited buffer size for real-time display
# buffer_size = 50
# metrics_data = {
#     "Timestamp": [],
#     "CPU Usage (%)": [],
#     "RAM Usage (%)": [],
#     "Disk Usage (GB)": [],
#     "Network Sent (MB)": [],
#     "Network Received (MB)": []
# }

# # Monitor system resources
# def monitor_resources():
#     cpu_usage = psutil.cpu_percent(interval=0.5)
#     ram_usage = psutil.virtual_memory().percent
#     disk_usage = psutil.disk_usage('/').used / (1024 ** 3)  # in GB

#     # Network I/O
#     net_io = psutil.net_io_counters()
#     net_sent = net_io.bytes_sent / (1024 ** 2)  # in MB
#     net_recv = net_io.bytes_recv / (1024 ** 2)  # in MB

#     # Find the process with the highest CPU usage
#     processes = [(proc.info['name'], proc.info['cpu_percent']) 
#                  for proc in psutil.process_iter(['name', 'cpu_percent'])]
#     top_cpu_proc = max(processes, key=lambda x: x[1], default=("N/A", 0))

#     # Append data with a limited buffer
#     metrics_data["Timestamp"].append(time.time())
#     metrics_data["CPU Usage (%)"].append(cpu_usage)
#     metrics_data["RAM Usage (%)"].append(ram_usage)
#     metrics_data["Disk Usage (GB)"].append(disk_usage)
#     metrics_data["Network Sent (MB)"].append(net_sent)
#     metrics_data["Network Received (MB)"].append(net_recv)

#     # Keep only the last 'buffer_size' records to maintain real-time effect without memory overflow
#     for key in metrics_data:
#         if len(metrics_data[key]) > buffer_size:
#             metrics_data[key].pop(0)

#     return top_cpu_proc, cpu_usage

# # Streamlit UI
# def streamlit_dashboard():
#     st.title("Real-Time Advanced System Monitor")
#     interval = st.sidebar.slider("Update Interval (seconds)", 0.5, 5.0, 1.0)
#     cpu_threshold = st.sidebar.slider("CPU Usage Alert Threshold (%)", 0, 100, 75)

#     st.sidebar.subheader("System Information")
#     cpu_cores = psutil.cpu_count(logical=False)
#     total_ram = psutil.virtual_memory().total / (1024 ** 3)
#     total_disk = psutil.disk_usage('/').total / (1024 ** 3)
#     st.sidebar.write(f"Total CPU Cores: {cpu_cores}")
#     st.sidebar.write(f"Total RAM: {total_ram:.2f} GB")
#     st.sidebar.write(f"Total Disk Space: {total_disk:.2f} GB")
    
#     # Initialize line charts for updating
#     st.subheader("CPU Usage Over Time")
#     cpu_chart = st.line_chart(pd.DataFrame({ "CPU Usage (%)": [] }))
#     st.caption("CPU Usage (%)")

#     st.subheader("RAM Usage Over Time")
#     ram_chart = st.line_chart(pd.DataFrame({ "RAM Usage (%)": [] }))
#     st.caption("RAM Usage (%)")

#     st.subheader("Disk Usage Over Time")
#     disk_chart = st.line_chart(pd.DataFrame({ "Disk Usage (GB)": [] }))
#     st.caption("Disk Usage (GB)")

#     st.subheader("Network Data Sent and Received Over Time")
#     network_chart = st.line_chart(pd.DataFrame({ "Network Sent (MB)": [], "Network Received (MB)": [] }))
#     st.caption("Network Sent (MB) and Network Received (MB)")

#     # Real-time monitoring and updating graphs
#     while True:
#         top_cpu_proc, current_cpu_usage = monitor_resources()
#         data_df = pd.DataFrame(metrics_data)

#         # Update existing charts with the new data
#         cpu_chart.add_rows(data_df[["CPU Usage (%)"]])
#         ram_chart.add_rows(data_df[["RAM Usage (%)"]])
#         disk_chart.add_rows(data_df[["Disk Usage (GB)"]])
#         network_chart.add_rows(data_df[["Network Sent (MB)", "Network Received (MB)"]])

#         # Warning if CPU usage crosses the set threshold
#         if current_cpu_usage > cpu_threshold:
#             st.warning(f"High CPU usage detected! Process '{top_cpu_proc[0]}' is using {top_cpu_proc[1]}% CPU.")

#         # Dynamic updates
#         time.sleep(interval)
        

# if __name__ == "__main__":
#     streamlit_dashboard()

import sqlite3
import psutil
import time
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from collections import deque
import threading

class SystemMonitorSDK:
    def __init__(self, log_interval=1, db_name="system_monitor.db", batch_size=10):
        """
        Initialize the SystemMonitorSDK with a logging interval, database name, and batch size.
        :param log_interval: Interval in seconds for logging system metrics.
        :param db_name: Name of the SQLite database file.
        :param batch_size: Number of records to accumulate before bulk insert into the database.
        """
        self.log_interval = log_interval
        self.cpu_usage_history = []  # Stores historical CPU usage data
        self.memory_usage_history = []  # Stores historical memory usage data
        self.system_log = []  # Temporary buffer for log entries
        self.db_name = db_name
        self.batch_size = batch_size

        # Initialize SQLite database
        self.initialize_database()

    def initialize_database(self):
        """
        Sets up the SQLite database with the required table structure.
        """
        conn = sqlite3.connect(self.db_name)
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

    def collect_metrics(self):
        """
        Collects real-time CPU and memory metrics.
        Returns a dictionary with CPU percentage, memory usage, CPU frequency,
        CPU temperature (if available), and available memory.
        """
        cpu_percent = psutil.cpu_percent()  # Get CPU usage as a percentage
        memory = psutil.virtual_memory()  # Get memory usage details

        # Append data for historical tracking
        self.cpu_usage_history.append(cpu_percent)
        self.memory_usage_history.append(memory.percent)

        return {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "cpu_usage_percent": cpu_percent,
            "memory_usage_percent": memory.percent,
            "cpu_frequency_mhz": psutil.cpu_freq().current,
            "cpu_temperature_celsius": self.get_cpu_temperature(),
            "available_memory_mb": memory.available / (1024 * 1024)  # Convert bytes to MB
        }

    def get_cpu_temperature(self):
        """
        Fetches CPU temperature if available.
        Returns a list of temperatures or None if not supported.
        """
        try:
            temps = psutil.sensors_temperatures()
            if 'coretemp' in temps:
                return [temp.current for temp in temps['coretemp']]
        except AttributeError:
            return None
        return None

    def insert_data_to_db(self, data_batch):
        """
        Inserts a batch of data into the SQLite database.
        :param data_batch: List of dictionaries containing metric data.
        """
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.executemany("""
            INSERT INTO metrics (timestamp, cpu_usage_percent, memory_usage_percent,
                                 cpu_frequency_mhz, cpu_temperature_celsius, available_memory_mb)
            VALUES (:timestamp, :cpu_usage_percent, :memory_usage_percent,
                    :cpu_frequency_mhz, :cpu_temperature_celsius, :available_memory_mb)
        """, data_batch)
        conn.commit()
        conn.close()

    def log_metrics(self):
        """
        Logs metrics at regular intervals, buffers them, and inserts into the database in batches.
        """
        while True:
            # Collect current metrics
            metrics = self.collect_metrics()
            self.system_log.append(metrics)

            # Insert into the database when batch size is reached
            if len(self.system_log) >= self.batch_size:
                self.insert_data_to_db(self.system_log)
                self.system_log = []  # Clear the buffer

            time.sleep(self.log_interval)

    def fetch_data_for_analysis(self):
        """
        Fetches data from the SQLite database for real-time analysis.
        Returns a pandas DataFrame containing the data.
        """
        conn = sqlite3.connect(self.db_name)
        query = "SELECT * FROM metrics ORDER BY id DESC LIMIT 100"  # Fetch the latest 100 records
        data = pd.read_sql_query(query, conn)
        conn.close()
        return data

    def plot_real_time_graphs(self):
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
            data = self.fetch_data_for_analysis()

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

# Create the monitor instance
monitor = SystemMonitorSDK(log_interval=1)

# Run data logging and real-time graph plotting in parallel using threading
log_thread = threading.Thread(target=monitor.log_metrics)
plot_thread = threading.Thread(target=monitor.plot_real_time_graphs)

log_thread.start()
plot_thread.start()

