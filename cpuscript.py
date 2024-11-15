import psutil
import time
from datetime import datetime
import json
import sqlite3
import threading

class SystemMonitor:
    def __init__(self, interval=2, db_file='system_metrics.db', alert_thresholds=None):
        self.interval = interval
        self.db_file = db_file  # Always use the same database file
        self.monitoring = False
        self.thread = None
        self.data_log = []

        # Default thresholds (can be modified)
        self.alert_thresholds = alert_thresholds if alert_thresholds else {
            "cpu_usage": 85,  # CPU usage above 85% triggers alert
            "memory_usage": 55,  # Memory usage above 55% triggers alert
            "disk_read_time": 100,  # Disk read time above 100ms triggers alert
            "disk_write_time": 100  # Disk write time above 100ms triggers alert
        }

        # Ensure the table is created only once if it doesn’t exist
        self._create_table()

    def _create_table(self):
        """Creates the system metrics table if it doesn't exist."""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS system_metrics (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                timestamp TEXT,
                                cpu_usage REAL,
                                memory_usage REAL,
                                num_processes INTEGER,
                                cpu_usage_per_core TEXT,
                                disk_read_time REAL,
                                disk_write_time REAL
                              )''')
        conn.commit()
        conn.close()

    def _monitor(self):
        # Create a new connection for this thread
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()

        while self.monitoring:
            try:
                # Collect essential system metrics
                cpu_usage = psutil.cpu_percent(interval=self.interval)
                cpu_usage_per_core = psutil.cpu_percent(interval=None, percpu=True)
                memory_info = psutil.virtual_memory()
                memory_usage = memory_info.percent  # This is a percentage value
                disk_io = psutil.disk_io_counters()
                disk_read_time = disk_io.read_time
                disk_write_time = disk_io.write_time
                num_processes = len(psutil.pids())
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                # Insert data into SQLite
                self._insert_data(cursor, conn, timestamp, cpu_usage, memory_usage, num_processes, 
                                  json.dumps(cpu_usage_per_core), disk_read_time, disk_write_time)

                # Display monitoring status
                print(f"[{timestamp}] CPU Usage: {cpu_usage}% | Memory Usage: {memory_usage}% | "
                      f"Processes: {num_processes} | CPU Usage Per Core: {cpu_usage_per_core} | "
                      f"Disk Read Time: {disk_read_time} ms | Disk Write Time: {disk_write_time} ms")

                # Check for alerts
                self._check_alerts(cpu_usage, memory_usage, disk_read_time, disk_write_time)

                # Sleep for 1 minute (60 seconds) before the next check
                time.sleep(60)

            except Exception as e:
                print(f"Error occurred: {e}")
                time.sleep(5)

        conn.close()

    def _insert_data(self, cursor, conn, timestamp, cpu_usage, memory_usage, num_processes, 
                     cpu_usage_per_core, disk_read_time, disk_write_time):
        """Inserts data into the SQLite database."""
        cursor.execute('''INSERT INTO system_metrics 
                          (timestamp, cpu_usage, memory_usage, num_processes, cpu_usage_per_core, 
                           disk_read_time, disk_write_time)
                          VALUES (?, ?, ?, ?, ?, ?, ?)''', 
                       (timestamp, cpu_usage, memory_usage, num_processes, cpu_usage_per_core, 
                        disk_read_time, disk_write_time))
        conn.commit()

    def _check_alerts(self, cpu_usage, memory_usage, disk_read_time, disk_write_time):
        """Checks if any metric exceeds the defined threshold and shows an alert."""
        print(f"Checking Alerts... CPU Usage: {cpu_usage}%, Memory Usage: {memory_usage}%")
        
        if cpu_usage > self.alert_thresholds["cpu_usage"]:
            print(f"ALERT: CPU usage is above {self.alert_thresholds['cpu_usage']}% - Current: {cpu_usage}%")
        if memory_usage > self.alert_thresholds["memory_usage"]:
            print(f"ALERT: Memory usage is above {self.alert_thresholds['memory_usage']}% - Current: {memory_usage}%")
        if disk_read_time > self.alert_thresholds["disk_read_time"]:
            print(f"ALERT: Disk read time is above {self.alert_thresholds['disk_read_time']}ms - Current: {disk_read_time}ms")
        if disk_write_time > self.alert_thresholds["disk_write_time"]:
            print(f"ALERT: Disk write time is above {self.alert_thresholds['disk_write_time']}ms - Current: {disk_write_time}ms")

    def start_monitoring(self, duration_minutes=10):
        if not self.monitoring:
            self.monitoring = True
            self.thread = threading.Thread(target=self._monitor)
            self.thread.start()
            print("Started monitoring...")

            # Stop monitoring after the specified duration
            time.sleep(duration_minutes * 60)
            self.stop_monitoring()

    def stop_monitoring(self):
        if self.monitoring:
            self.monitoring = False
            self.thread.join()
            print("Stopped monitoring.")

    def view_data(self):
        """Retrieve and display stored data from the database."""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        
        # Query all data from the system_metrics table
        cursor.execute("SELECT * FROM system_metrics")
        rows = cursor.fetchall()
        
        # Print each row
        for row in rows:
            print(row)
        
        conn.close()

# Usage example:
if __name__ == "__main__":
    monitor = SystemMonitor(interval=3, alert_thresholds={
        "cpu_usage": 85,  # Alert if CPU usage exceeds 85%
        "memory_usage": 55,  # Alert if memory usage exceeds 55%
        "disk_read_time": 100,  # Alert if disk read time exceeds 100ms
        "disk_write_time": 100  # Alert if disk write time exceeds 100ms
    })
    try:
        monitor.start_monitoring(duration_minutes=10)  # Monitor for 10 minutes
    except KeyboardInterrupt:
        pass
    finally:
        monitor.view_data()  # Display stored data after monitoring stops
