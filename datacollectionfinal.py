import psutil
import psycopg2  # PostgreSQL library for Python
import time
import json
from datetime import datetime
import zlib  # For data compression

# Configuration for the PostgreSQL database
DATABASE_CONFIG = {
    'dbname': 'postgres',
    'user': 'postgres.cwkrmwnrdglcuffvlhss',
    'password': 'R@unak87709',
    'host': 'aws-0-ap-south-1.pooler.supabase.com',
    'port': '6543'
}

# user=postgres.cwkrmwnrdglcuffvlhss password=[YOUR-PASSWORD] host=aws-0-ap-south-1.pooler.supabase.com port=6543 dbname=postgres

# Batch size for storing metrics at once
BATCH_SIZE = 3

# Maximum number of entries allowed in the `metrics` table
MAX_ENTRIES = 9

def setup_database():
    """
    Sets up the database by creating required tables:
    1. `metrics` for live system metrics
    2. `metrics_archive` for compressed, archived metrics
    """
    print("[INFO] Setting up the database schema...")
    conn = psycopg2.connect(**DATABASE_CONFIG)
    cursor = conn.cursor()

    # Create `metrics` table to store live system metrics
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS metrics (
            id SERIAL PRIMARY KEY,
            timestamp TEXT,
            cpu_percent REAL,
            memory_percent REAL,
            disk_percent REAL,
            network_bytes_sent BIGINT,
            network_bytes_recv BIGINT
        )
    ''')
    print("[INFO] Table `metrics` is ready.")

    # Create `metrics_archive` table to store archived metrics in compressed format
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS metrics_archive (
            id SERIAL PRIMARY KEY,
            archived_timestamp TEXT,
            compressed_data BYTEA
        )
    ''')
    print("[INFO] Table `metrics_archive` is ready.")

    conn.commit()
    conn.close()
    print("[INFO] Database setup complete.")

def collect_metrics():
    """
    Collects current system metrics such as CPU, memory, disk, and network usage.
    :return: Dictionary of collected metrics.
    """
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
    """
    Displays the contents of the `metrics` table.
    """
    print("[INFO] Fetching and displaying the current contents of the `metrics` table...")
    conn = psycopg2.connect(**DATABASE_CONFIG)
    cursor = conn.cursor()

    # Fetch all records in the `metrics` table
    cursor.execute('SELECT * FROM metrics ORDER BY timestamp ASC')
    rows = cursor.fetchall()

    print("[INFO] Current records in the `metrics` table:")
    for row in rows:
        print(row)
    
    conn.close()

def store_batch(batch):
    """
    Stores a batch of system metrics into the `metrics` table.
    :param batch: List of tuples containing metrics data.
    """
    print(f"[INFO] Storing a batch of {len(batch)} metrics into the database...")
    conn = psycopg2.connect(**DATABASE_CONFIG)
    cursor = conn.cursor()

    # Insert metrics into the `metrics` table
    cursor.executemany(
        'INSERT INTO metrics (timestamp, cpu_percent, memory_percent, disk_percent, network_bytes_sent, network_bytes_recv) VALUES (%s, %s, %s, %s, %s, %s)',
        batch
    )
    conn.commit()
    print("[INFO] Batch stored successfully.")
    display_database_contents()

    # Check the total number of entries and archive excess if needed
    cursor.execute('SELECT COUNT(*) FROM metrics')
    total_entries = cursor.fetchone()[0]
    print(f"[INFO] Total entries in the `metrics` table: {total_entries}")

    if total_entries > MAX_ENTRIES:
        print(f"[INFO] Archiving and deleting {total_entries - MAX_ENTRIES} excess entries...")
        move_and_delete_oldest_entries(cursor, total_entries - MAX_ENTRIES)

    conn.commit()
    conn.close()

def move_and_delete_oldest_entries(cursor, excess_entries):
    """
    Moves the oldest entries from the `metrics` table to the `metrics_archive` table and deletes them.
    :param cursor: Cursor object for executing SQL commands.
    :param excess_entries: Number of oldest entries to be moved and deleted.
    """
    print(f"[INFO] Fetching {excess_entries} oldest entries for archiving...")

    # Fetch the oldest entries from the `metrics` table
    cursor.execute(f'SELECT * FROM metrics ORDER BY timestamp ASC LIMIT {excess_entries}')
    oldest_entries = cursor.fetchall()

    print(f"[INFO] Archiving {len(oldest_entries)} entries...")

    # Archive each entry in compressed form
    for entry in oldest_entries:
        id, timestamp, cpu, memory, disk, bytes_sent, bytes_recv = entry

        # Prepare data for compression
        data = {
            'timestamp': timestamp,
            'cpu_percent': cpu,
            'memory_percent': memory,
            'disk_percent': disk,
            'network_bytes_sent': bytes_sent,
            'network_bytes_recv': bytes_recv
        }

        # Compress the data using zlib
        compressed_data = zlib.compress(json.dumps(data).encode('utf-8'))

        # Insert compressed data into the `metrics_archive` table
        cursor.execute(
            'INSERT INTO metrics_archive (archived_timestamp, compressed_data) VALUES (%s, %s)',
            (datetime.now().isoformat(), compressed_data)
        )

    # Delete the oldest entries from the `metrics` table
    cursor.execute(f'DELETE FROM metrics WHERE id IN (SELECT id FROM metrics ORDER BY timestamp ASC LIMIT {excess_entries})')
    print(f"[INFO] Archived and deleted {len(oldest_entries)} entries from the `metrics` table.")

def monitor_system():
    """
    Continuously monitors the system, collects metrics, and stores them in batches.
    Manages database size by archiving old records.
    """
    setup_database()
    batch = []

    print("[INFO] Starting system monitoring...")
    while True:
        try:
            # Collect metrics
            current_metrics = collect_metrics()
            timestamp = datetime.now().isoformat()

            # Add the collected metrics to the batch
            batch.append((timestamp, current_metrics['cpu_percent'], current_metrics['memory_percent'],
                          current_metrics['disk_percent'], current_metrics['network_bytes_sent'], current_metrics['network_bytes_recv']))

            print(f"[INFO] Current batch size: {len(batch)}")

            # If batch size limit is reached, store the batch and reset it
            if len(batch) >= BATCH_SIZE:
                print("[INFO] Batch size limit reached. Storing the batch...")
                store_batch(batch)
                batch = []  # Reset the batch
                print("[INFO] Batch reset after storing.")

            # Wait for 10 seconds before collecting metrics again
            time.sleep(10)

        except Exception as e:
            print(f"[ERROR] Error during monitoring: {e}")

if __name__ == "__main__":
    print("[INFO] Starting the metrics collection script...")
    monitor_system()
