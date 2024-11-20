#working code file 
 


# import psutil
# import sqlite3
# import time
# import json
# from datetime import datetime

# DATABASE_FILE = 'met.db'
# BATCH_SIZE = 3
# MAX_ENTRIES = 9

# def setup_database():
#     """Sets up the database to store metrics."""
#     print("[INFO] Setting up the database...")
#     conn = sqlite3.connect(DATABASE_FILE)
#     cursor = conn.cursor()

#     cursor.execute('''
#         CREATE TABLE IF NOT EXISTS metrics (
#             timestamp TEXT,
#             metrics_data TEXT
#         )
#     ''')
#     conn.commit()
#     conn.close()
#     print("[INFO] Database setup complete.")

# def collect_metrics():
#     """Collects system metrics."""
#     print("[INFO] Collecting system metrics...")
#     metrics = {
#         'cpu_percent': psutil.cpu_percent(),
#         'memory_percent': psutil.virtual_memory().percent,
#         'disk_percent': psutil.disk_usage('/').percent,
#         'network_bytes_sent': psutil.net_io_counters().bytes_sent,
#         'network_bytes_recv': psutil.net_io_counters().bytes_recv
#     }
#     print(f"[INFO] Collected metrics: {metrics}")
#     return metrics

# def display_database_contents():
#     """Displays the current contents of the database."""
#     conn = sqlite3.connect(DATABASE_FILE)
#     cursor = conn.cursor()
#     cursor.execute('SELECT * FROM metrics ORDER BY timestamp ASC')
#     rows = cursor.fetchall()
#     print("[INFO] Current database contents:")
#     for row in rows:
#         print(row)
#     conn.close()

# def store_batch(batch):
#     """Stores a batch of metrics in the database."""
#     print(f"[INFO] Storing a batch of {len(batch)} metrics in the database...")
#     conn = sqlite3.connect(DATABASE_FILE)
#     cursor = conn.cursor()

#     cursor.executemany(
#         'INSERT INTO metrics (timestamp, metrics_data) VALUES (?, ?)',
#         batch
#     )
#     conn.commit()
#     print("[INFO] Batch stored successfully.")
#     display_database_contents()

#     # Ensure the database has no more than MAX_ENTRIES
#     cursor.execute('SELECT COUNT(*) FROM metrics')
#     total_entries = cursor.fetchone()[0]
#     print(f"[INFO] Total entries in the database: {total_entries}")

#     if total_entries > MAX_ENTRIES:
#         # Delete the 3 oldest entries
#         print(f"[INFO] Removing the 3 oldest entries to maintain the database size...")
#         cursor.execute('DELETE FROM metrics WHERE rowid IN (SELECT rowid FROM metrics ORDER BY timestamp ASC LIMIT 3)')
#         conn.commit()
#         print(f"[INFO] 3 oldest entries removed.")
#         display_database_contents()

#     conn.close()

# def monitor_system():
#     """Monitors the system, collects data, and stores it in batches."""
#     setup_database()
#     batch = []
    
#     print("[INFO] Starting system monitoring...")
#     while True:
#         try:
#             # Collect metrics
#             current_metrics = collect_metrics()
#             timestamp = datetime.now().isoformat()

#             # Prepare batch entry
#             batch.append((timestamp, json.dumps(current_metrics)))

#             print(f"[INFO] Current batch size: {len(batch)}")

#             # If batch size reaches the limit, store it and reset batch
#             if len(batch) >= BATCH_SIZE:
#                 print("[INFO] Batch size limit reached. Storing batch...")
#                 store_batch(batch)
#                 batch = []  # Reset batch after storing
#                 print("[INFO] Batch reset after storing.")

#             # Sleep for 1 minute before collecting metrics again
#             time.sleep(10)  # Collect metrics every 60 seconds

#         except Exception as e:
#             print(f"[ERROR] Error during monitoring: {e}")

# if __name__ == "_main_":
#     print("[INFO] Starting data collection...")
#     monitor_system()
import psutil
import sqlite3
import json
from datetime import datetime

DATABASE_FILE = 'met.db'
BATCH_SIZE = 3
MAX_ENTRIES = 9

def setup_database():
    """Sets up the database to store metrics."""
    print("[INFO] Setting up the database...")
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS metrics (
            timestamp TEXT,
            metrics_data TEXT
        )
    ''')
    conn.commit()
    conn.close()
    print("[INFO] Database setup complete.")

def collect_metrics():
    """Collects system metrics."""
    metrics = {
        'cpu_percent': psutil.cpu_percent(),
        'memory_percent': psutil.virtual_memory().percent,
        'disk_percent': psutil.disk_usage('/').percent,
        'network_bytes_sent': psutil.net_io_counters().bytes_sent,
        'network_bytes_recv': psutil.net_io_counters().bytes_recv
    }
    print(f"[INFO] Collected metrics: {metrics}")
    return metrics

def store_metrics(batch):
    """Stores a batch of metrics in the database."""
    print(f"[INFO] Storing a batch of {len(batch)} metrics in the database...")
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()

    cursor.executemany(
        'INSERT INTO metrics (timestamp, metrics_data) VALUES (?, ?)',
        batch
    )
    conn.commit()
    print("[INFO] Metrics batch stored successfully.")

    # Ensure the database has no more than MAX_ENTRIES
    cursor.execute('SELECT COUNT(*) FROM metrics')
    total_entries = cursor.fetchone()[0]
    print(f"[INFO] Total entries in the database: {total_entries}")

    if total_entries > MAX_ENTRIES:
        print(f"[INFO] Removing the oldest entries to maintain a maximum of {MAX_ENTRIES} records.")
        cursor.execute('DELETE FROM metrics WHERE rowid IN (SELECT rowid FROM metrics ORDER BY timestamp ASC LIMIT ?)', (BATCH_SIZE,))
        conn.commit()
        print("[INFO] Oldest entries removed successfully.")

    conn.close()

def main():
    """Collect and store metrics."""
    print("[INFO] Starting data collection...")
    setup_database()
    timestamp = datetime.now().isoformat()
    print(f"[INFO] Current timestamp: {timestamp}")
    metrics = collect_metrics()
    store_metrics([(timestamp, json.dumps(metrics))])
    print("[INFO] Data collection cycle complete.")

if __name__ == "__main__":
    main()
