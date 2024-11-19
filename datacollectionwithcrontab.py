import psutil
import sqlite3
import json
import zlib
from datetime import datetime

DATABASE_FILE = 'met.db'
BATCH_SIZE = 3
MAX_ENTRIES = 9

def setup_database():
    """Sets up the database to store metrics."""
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

def collect_metrics():
    """Collects system metrics."""
    metrics = {
        'cpu_percent': psutil.cpu_percent(),
        'memory_percent': psutil.virtual_memory().percent,
        'disk_percent': psutil.disk_usage('/').percent,
        'network_bytes_sent': psutil.net_io_counters().bytes_sent,
        'network_bytes_recv': psutil.net_io_counters().bytes_recv
    }
    return metrics

def store_batch(batch):
    """Stores a batch of metrics in the database."""
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()

    cursor.executemany(
        'INSERT INTO metrics (timestamp, metrics_data) VALUES (?, ?)',
        batch
    )
    conn.commit()

    # Ensure the database has no more than MAX_ENTRIES
    cursor.execute('SELECT COUNT(*) FROM metrics')
    total_entries = cursor.fetchone()[0]

    if total_entries > MAX_ENTRIES:
        # Archive and delete the 3 oldest entries
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

    conn.close()

def main():
    """Collects metrics and stores them in the database."""
    setup_database()
    batch = []

    # Collect metrics
    current_metrics = collect_metrics()
    timestamp = datetime.now().isoformat()

    # Prepare batch entry
    batch.append((timestamp, json.dumps(current_metrics)))

    # Store batch
    store_batch(batch)

if __name__ == "__main__":
    main()
