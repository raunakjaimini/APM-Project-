# this code in thred unsafe
# db jiss thred me bana h usi se operation kar skte h sqlite me 
# soln = we need to chwkc for same thred 


import psutil
import time
from datetime import datetime
import json
import sqlite3
import threading  # Multithreading

class SystemMonitor:
    def __init__(self, interval=2, db_file='wal_.db', alert_thresholds=None, alert_log_file='alerts.json'):
        self.interval = interval
        self.db_file = db_file
        self.alert_log_file = alert_log_file
        self.monitoring = False    # Monitoring loop
        self.thread = None
        self.data_log = []         # Data collection (currently unused)



        # Default thresholds (can be modified)
        self.alert_thresholds = alert_thresholds if alert_thresholds else {
            "cpu_usage": 50,  # CPU usage above 50% triggers alert
            "memory_usage": 85,  # Memory usage above 85% triggers alert
            "disk_read_time": 1000,  # Disk read time above 1000ms triggers alert
            "disk_write_time": 1000  # Disk write time above 1000ms triggers alert
        }


        # Open a single database connection
        # pehle harr baar new bana rhae the, which is waste
        self.conn = sqlite3.connect(self.db_file)
        self.cursor = self.conn.cursor()

        # Enable WAL mode, addition of this line 
        self.cursor.execute('PRAGMA journal_mode=WAL;')

        # Ensure the table is created only once if it doesn’t exist
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
        """Collects all the system wal_metrics and stores them in the database."""
        while self.monitoring:
            try:
                cpu_usage = psutil.cpu_percent(interval=self.interval)
                cpu_usage_per_core = psutil.cpu_percent(interval=None, percpu=True)
                memory_info = psutil.virtual_memory()
                memory_usage = memory_info.percent  # Memory usage in percentage
                disk_io = psutil.disk_io_counters()
                disk_read_time = disk_io.read_time
                disk_write_time = disk_io.write_time
                num_processes = len(psutil.pids())  # Number of active processes
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                # Insert data into SQLite
                self._insert_data(timestamp, cpu_usage, memory_usage, num_processes, 
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

    def _insert_data(self, timestamp, cpu_usage, memory_usage, num_processes, 
                     cpu_usage_per_core, disk_read_time, disk_write_time):
        """Inserts data into the SQLite database."""
        self.cursor.execute('''INSERT INTO wal_metrics 
                              (timestamp, cpu_usage, memory_usage, num_processes, cpu_usage_per_core, 
                               disk_read_time, disk_write_time) VALUES (?, ?, ?, ?, ?, ?, ?)''', 
                           (timestamp, cpu_usage, memory_usage, num_processes, cpu_usage_per_core, 
                            disk_read_time, disk_write_time))
        self.conn.commit()

    x=1

    def _check_alerts(self, cpu_usage, memory_usage, disk_read_time, disk_write_time):
        """Checks if any metric exceeds the defined threshold and logs an alert."""
        x+=1
        print(f"{x}, Checking Alerts... CPU Usage: {cpu_usage}%, Memory Usage: {memory_usage}%")
        
        alerts = []
        # array isliye banayi h isme jisse sab complete data json me store ho sake
        # this will act as log file 
        
        if cpu_usage > self.alert_thresholds["cpu_usage"]:
            alert = {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "type": "CPU",
                "value": cpu_usage,
                "threshold": self.alert_thresholds["cpu_usage"],
                "message": f"CPU usage is above {self.alert_thresholds['cpu_usage']}% - Current: {cpu_usage}%"
            }
            alerts.append(alert)   #add krte ja rahe h ek ek cheez in the array & print the msg
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
        try:
            # Read existing alerts from the file
            # opened file ko ek f variable me assign kardiay 
            # ab ye f ke through hi file me changes karenge
            with open(self.alert_log_file, 'r') as f:
                all_alerts = json.load(f)
                # load se saara file ka data nikala, and variable me daal diya format change krke
        except (FileNotFoundError, json.JSONDecodeError):
            # If file doesn't exist or is empty, start with an empty list
            all_alerts = []
            # koi error aati h ya gadbad hoti h toh list reset

        # Append new alerts
        all_alerts.extend(alerts)

        # Save all alerts back to the file
        with open(self.alert_log_file, 'w') as f:
            json.dump(all_alerts, f, indent=4)  #syntax aisa hi hota h iska 3 params wala
        # list ko extend kardiya and wapas se json me convert krke dal diya 



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
        monitor.start_monitoring(duration_minutes=3)  # Monitor for 3 minutes
    except KeyboardInterrupt:
        print("Monitoring interrupted by user.")
        monitor.stop_monitoring()  # Ensure the monitoring stops gracefully
    finally:
        monitor.view_data()  # Display stored data after monitoring stops
        monitor.close()  # Close the database connection before exiting
        print("Program exiting...")