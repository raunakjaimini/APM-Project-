import psutil
import time
from datetime import datetime
import json
import sqlite3
import threading

class SystemMonitor:
    def __init__(self, interval=2, db_file='wal_.db', alert_thresholds=None, alert_log_file='alerts.json'):
        self.interval = interval
        self.db_file = db_file
        self.alert_log_file = alert_log_file
        self.monitoring = False
        self.thread = None
        self.data_log = []         # Data collection (currently unused)
        self.lock = threading.Lock()  # Lock to ensure thread-safe DB access

        # Default thresholds (can be modified)
        self.alert_thresholds = alert_thresholds if alert_thresholds else {
            "cpu_usage": 50,
            "memory_usage": 85,
            "disk_read_time": 1000,
            "disk_write_time": 1000
        }

        # Open a single database connection
        self.conn = sqlite3.connect(self.db_file, check_same_thread=False)
        self.cursor = self.conn.cursor()
        
        # Enable WAL mode
        self.cursor.execute('PRAGMA journal_mode=WAL;')
        
        # Ensure the table is created
        self._create_table()

    def _create_table(self):
        """Creates the system wal_metrics table if it doesn't exist."""
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS wal_metrics (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                timestamp TEXT,
                                cpu_usage REAL,
                                memory_usage REAL,
                                num_processes INTEGER,
                                cpu_usage_per_core TEXT,
                                disk_read_time REAL,
                                disk_write_time REAL
                              )''')
        self.conn.commit()

    def _monitor(self):
        """Collects all the system metrics and stores them in the database."""
        while self.monitoring:
            try:
                # Collect system metrics
                cpu_usage = psutil.cpu_percent(interval=self.interval)
                cpu_usage_per_core = psutil.cpu_percent(interval=None, percpu=True)
                memory_info = psutil.virtual_memory()
                memory_usage = memory_info.percent
                disk_io = psutil.disk_io_counters()
                disk_read_time = disk_io.read_time
                disk_write_time = disk_io.write_time
                num_processes = len(psutil.pids())  # Number of active processes
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                # Create a new SQLite connection for this thread to avoid thread-safety issues
                with sqlite3.connect(self.db_file, check_same_thread=False) as conn:
                    cursor = conn.cursor()
                    # Insert data into SQLite
                    self._insert_data(cursor, timestamp, cpu_usage, memory_usage, num_processes, 
                                      json.dumps(cpu_usage_per_core), disk_read_time, disk_write_time)

                # Display monitoring status
                print(f"[{timestamp}] CPU Usage: {cpu_usage}% | RAM Usage: {memory_usage}% | "
                      f"Processes: {num_processes} | CPU Usage Per Core: {cpu_usage_per_core} | "
                      f"Disk Read Time: {disk_read_time} ms | Disk Write Time: {disk_write_time} ms\n")

                # Check for alerts
                self._check_alerts(cpu_usage, memory_usage, disk_read_time, disk_write_time)

                # Sleep for the specified interval before the next check
                time.sleep(30)

            except Exception as e:
                print(f"Error occurred: {e}")
                time.sleep(5)

    def _insert_data(self, cursor, timestamp, cpu_usage, memory_usage, num_processes, 
                     cpu_usage_per_core, disk_read_time, disk_write_time):
        """Inserts data into the SQLite database."""
        cursor.execute('''INSERT INTO wal_metrics 
                          (timestamp, cpu_usage, memory_usage, num_processes, cpu_usage_per_core, 
                           disk_read_time, disk_write_time) VALUES (?, ?, ?, ?, ?, ?, ?)''', 
                       (timestamp, cpu_usage, memory_usage, num_processes, cpu_usage_per_core, 
                        disk_read_time, disk_write_time))
        cursor.connection.commit()

    def _check_alerts(self, cpu_usage, memory_usage, disk_read_time, disk_write_time):
        """Checks if any metric exceeds the defined threshold and logs an alert."""
        alerts = []
        if cpu_usage > self.alert_thresholds["cpu_usage"]:
            alert = {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "type": "CPU",
                "value": cpu_usage,
                "threshold": self.alert_thresholds["cpu_usage"],
                "message": f"CPU usage is above {self.alert_thresholds['cpu_usage']}% - Current: {cpu_usage}%"
            }
            alerts.append(alert)
            print(alert["message"])

        if memory_usage > self.alert_thresholds["memory_usage"]:
            alert = {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "type": "Memory",
                "value": memory_usage,
                "threshold": self.alert_thresholds["memory_usage"],
                "message": f"Memory usage is above {self.alert_thresholds['memory_usage']}% - Current: {memory_usage}%"
            }
            alerts.append(alert)
            print(alert["message"])

        if disk_read_time > self.alert_thresholds["disk_read_time"]:
            alert = {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "type": "Disk Read",
                "value": disk_read_time,
                "threshold": self.alert_thresholds["disk_read_time"],
                "message": f"Disk read time is above {self.alert_thresholds['disk_read_time']}ms - Current: {disk_read_time}ms"
            }
            alerts.append(alert)
            print(alert["message"])

        if disk_write_time > self.alert_thresholds["disk_write_time"]:
            alert = {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "type": "Disk Write",
                "value": disk_write_time,
                "threshold": self.alert_thresholds["disk_write_time"],
                "message": f"Disk write time is above {self.alert_thresholds['disk_write_time']}ms - Current: {disk_write_time}ms"
            }
            alerts.append(alert)
            print(alert["message"])

        # Store alerts in a JSON file
        if alerts:
            self._store_alerts(alerts)

    def _store_alerts(self, alerts):
        """Store alerts in a JSON file."""
        with self.lock:
            try:
                with open(self.alert_log_file, 'r') as f:
                    all_alerts = json.load(f)
            except (FileNotFoundError, json.JSONDecodeError):
                all_alerts = []

            all_alerts.extend(alerts)

            with open(self.alert_log_file, 'w') as f:
                json.dump(all_alerts, f, indent=4)

    def start_monitoring(self, duration_minutes=10):
        if not self.monitoring:
            self.monitoring = True
            self.thread = threading.Thread(target=self._monitor)
            self.thread.start()
            print("Started monitoring...")

            # Stop monitoring after the specified duration
            time.sleep(duration_minutes * 30)
            self.stop_monitoring()

    def stop_monitoring(self):
        if self.monitoring:
            self.monitoring = False
            self.thread.join()
            print("Stopped monitoring.")

    def view_data(self):
        """Retrieve and display stored data from the database."""
        self.cursor.execute("SELECT * FROM wal_metrics")
        rows = self.cursor.fetchall()
        for row in rows:
            print(f"{row}")

    def close(self):
        """Close the database connection."""
        self.conn.close()

if __name__ == "__main__":
    monitor = SystemMonitor(interval=2, alert_thresholds={
        "cpu_usage": 55,
        "memory_usage": 85,
        "disk_read_time": 1000,
        "disk_write_time": 1000
    })

    try:
        monitor.start_monitoring(duration_minutes=3)
    except KeyboardInterrupt:
        print("Monitoring interrupted by user.")
        monitor.stop_monitoring()
    finally:
        monitor.view_data()
        monitor.close()
        print("Program exiting...")
