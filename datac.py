import psutil
import psycopg2  # Use psycopg2 for PostgreSQL
import time
from datetime import datetime

DATABASE_CONFIG = {
    'dbname': 'new_metrics_db',  # Change to your new database name
    'user': 'postgres.kpcfwxpuwvwdqytccsac',  # Your PostgreSQL user
    'password': 'R@unak87709',  # Your PostgreSQL password
    'host': 'aws-0-ap-south-1.pooler.supabase.com',  # Database host
    'port': '6543'  # Default PostgreSQL port
}

BATCH_SIZE = 3
MAX_ENTRIES = 9

def setup_database():
    """Sets up the database to store metrics."""
    print("[INFO] Setting up the database...")
    conn = psycopg2.connect(**DATABASE_CONFIG)
    cursor = conn.cursor()

    # Create table with separate columns for metrics
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS metrics (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL,
            cpu_percent REAL NOT NULL,
            memory_percent REAL NOT NULL,
            disk_percent REAL NOT NULL,
            network_bytes_sent BIGINT NOT NULL,
            network_bytes_recv BIGINT NOT NULL
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
    conn = psycopg2.connect(**DATABASE_CONFIG)
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
    conn = psycopg2.connect(**DATABASE_CONFIG)
    cursor = conn.cursor()

    cursor.executemany(
        '''
        INSERT INTO metrics (
            timestamp, cpu_percent, memory_percent, disk_percent, network_bytes_sent, network_bytes_recv
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ''',
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
        # Remove the oldest entries
        print(f"[INFO] Removing the oldest entries to maintain the database size...")
        cursor.execute(
            'DELETE FROM metrics WHERE id IN (SELECT id FROM metrics ORDER BY timestamp ASC LIMIT %s)',
            (total_entries - MAX_ENTRIES,)
        )
        conn.commit()
        print("[INFO] Oldest entries removed.")
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
            timestamp = datetime.now()

            # Prepare batch entry
            batch.append((
                timestamp,
                current_metrics['cpu_percent'],
                current_metrics['memory_percent'],
                current_metrics['disk_percent'],
                current_metrics['network_bytes_sent'],
                current_metrics['network_bytes_recv']
            ))

            print(f"[INFO] Current batch size: {len(batch)}")

            # If batch size reaches the limit, store it and reset batch
            if len(batch) >= BATCH_SIZE:
                print("[INFO] Batch size limit reached. Storing batch...")
                store_batch(batch)
                batch = []  # Reset batch after storing
                print("[INFO] Batch reset after storing.")

            # Sleep for 10 seconds before collecting metrics again
            time.sleep(10)  # Collect metrics every 10 seconds

        except Exception as e:
            print(f"[ERROR] Error during monitoring: {e}")

if __name__ == "__main__":
    print("[INFO] Starting data collection...")
    monitor_system()
