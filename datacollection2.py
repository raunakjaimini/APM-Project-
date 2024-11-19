import psutil
import sqlite3
import time
import json
import zlib
from datetime import datetime

DATABASE_FILE = 'met.db'
BATCH_SIZE = 3
MAX_ENTRIES = 9

def setup_database():
    """Sets up the database to store metrics."""
    print("[INFO] Setting up the database...")
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()

    # Create table for active metrics
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS metrics (
            timestamp TEXT,
            metrics_data TEXT
        )
    ''')

    # Create table for archived metrics
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS archived_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            compressed_data BLOB
        )
    ''')

    conn.commit()
    conn.close()
    print("[INFO] Database setup complete.")

def collect_metrics():
    """Collects system metrics."""
    print("[INFO] Collecting system metrics...")
    metrics = {
        'cpu_percent': psutil.cpu_percent(),
        'memory_percent': psutil.virtual_memory().percent,
        'disk_percent': psutil.disk_usage('/').percent,
        'network_bytes_sent': psutil.net_io_counters().bytes_sent,
        'network_bytes_recv': psutil.net_io_counters().bytes_recv
    }
    print(f"[INFO] Collected metrics: {metrics}")
    return metrics

def display_database_contents():
    """Displays the current contents of the database."""
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM metrics ORDER BY timestamp ASC')
    rows = cursor.fetchall()
    print("[INFO] Current database contents:")
    for row in rows:
        print(row)
    conn.close()

def store_batch(batch):
    """Stores a batch of metrics in the database."""
    print(f"[INFO] Storing a batch of {len(batch)} metrics in the database...")
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()

    cursor.executemany(
        'INSERT INTO metrics (timestamp, metrics_data) VALUES (?, ?)',
        batch
    )
    conn.commit()
    print("[INFO] Batch stored successfully.")
    display_database_contents()

    # Ensure the database has no more than MAX_ENTRIES
    cursor.execute('SELECT COUNT(*) FROM metrics')
    total_entries = cursor.fetchone()[0]
    print(f"[INFO] Total entries in the database: {total_entries}")

    if total_entries > MAX_ENTRIES:
        # Archive and delete the 3 oldest entries
        print(f"[INFO] Archiving the 3 oldest entries to maintain the database size...")
        cursor.execute('SELECT rowid, timestamp, metrics_data FROM metrics ORDER BY timestamp ASC LIMIT 3')
        oldest_entries = cursor.fetchall()

        for entry in oldest_entries:
            rowid, timestamp, metrics_data = entry
            compressed_data = zlib.compress(metrics_data.encode('utf-8'))

            # Store in archived_metrics
            cursor.execute(
                'INSERT INTO archived_metrics (timestamp, compressed_data) VALUES (?, ?)',
                (timestamp, compressed_data)
            )
        
        # Remove archived entries from the main table
        cursor.execute('DELETE FROM metrics WHERE rowid IN (SELECT rowid FROM metrics ORDER BY timestamp ASC LIMIT 3)')
        conn.commit()
        print(f"[INFO] 3 oldest entries archived and removed from active metrics.")
        display_database_contents()

    conn.close()

def monitor_system():
    """Monitors the system, collects data, and stores it in batches."""
    setup_database()
    batch = []
    
    print("[INFO] Starting system monitoring...")
    while True:
        try:
            # Collect metrics
            current_metrics = collect_metrics()
            timestamp = datetime.now().isoformat()

            # Prepare batch entry
            batch.append((timestamp, json.dumps(current_metrics)))

            print(f"[INFO] Current batch size: {len(batch)}")

            # If batch size reaches the limit, store it and reset batch
            if len(batch) >= BATCH_SIZE:
                print("[INFO] Batch size limit reached. Storing batch...")
                store_batch(batch)
                batch = []  # Reset batch after storing
                print("[INFO] Batch reset after storing.")

            # Sleep for 1 minute before collecting metrics again
            time.sleep(10)  # Collect metrics every 60 seconds

        except Exception as e:
            print(f"[ERROR] Error during monitoring: {e}")

if __name__ == "__main__":
    print("[INFO] Starting data collection...")
    monitor_system()
