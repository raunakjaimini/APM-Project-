# working code for datacollectionwithsupabase

import psutil
import psycopg2  # PostgreSQL library for Python
import time
import json
from datetime import datetime
import zlib  # For data compression
import requests
from psycopg2 import sql, connect

# Configuration for the PostgreSQL database
DATABASE_CONFIG = {
    'dbname': 'postgres',
    'user': 'postgres.cwkrmwnrdglcuffvlhss',
    'password': 'R@unak87709',
    'host': 'aws-0-ap-south-1.pooler.supabase.com',
    'port': '6543'
}



# Batch size for storing metrics at once
BATCH_SIZE = 3

# Maximum number of entries allowed in the `metrics` table
MAX_ENTRIES = 9

# Global DEFCON levels
DEFAULT_CPU_LEVELS = [0, 70, 80, 85, 90, 95, 100]
DEFAULT_RAM_LEVELS = [0, 70, 80, 85, 90, 95, 100]
DEFAULT_DISK_LEVELS = [100, 50, 30, 25, 15, 5, 0]


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
    # Create `thresholds` table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS thresholds (
            id SERIAL PRIMARY KEY,
            metric_type TEXT UNIQUE,
            levels TEXT
        )
    ''')
    print("[INFO] Table `thresholds` is ready.")


# Create `endpoints` table with request count and average response time
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS endpoints (
            id SERIAL PRIMARY KEY,
            name_endpoint TEXT NOT NULL,  
            request_count INTEGER NOT NULL,  
            avg_response_time FLOAT NOT NULL,  
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
        )
    ''')
    print("[INFO] Table `endpoints` is ready.")
    
    # create system_alerts table 
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS system_alerts (
            id SERIAL PRIMARY KEY,
            metric VARCHAR(10),        -- Metric type (e.g., "CPU", "RAM", "Disk")
            defcon_level INTEGER,      -- DEFCON level
            alert_color VARCHAR(10),   -- Alert color
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')    
    print("[INFO] Table `alert table` is ready.")


    conn.commit()
    conn.close()
    print("[INFO] Database setup complete.")

def get_thresholds_from_db():
    """
    Fetches alert thresholds from the database. If not present, returns None.
    """
    conn = psycopg2.connect(**DATABASE_CONFIG)
    cursor = conn.cursor()
    cursor.execute('SELECT metric_type, levels FROM thresholds')
    rows = cursor.fetchall()
    conn.close()

    thresholds = {}
    for metric_type, levels in rows:
        thresholds[metric_type] = json.loads(levels)

    return thresholds if thresholds else None

def update_thresholds_in_db(cpu_levels, ram_levels, disk_levels):
    """
    Updates or inserts new alert thresholds into the database.
    """
    conn = psycopg2.connect(**DATABASE_CONFIG)
    cursor = conn.cursor()

    # Upsert logic for thresholds
    for metric, levels in zip(
        ["cpu", "ram", "disk"],
        [cpu_levels, ram_levels, disk_levels]
    ):
        cursor.execute('''
            INSERT INTO thresholds (metric_type, levels)
            VALUES (%s, %s)
            ON CONFLICT (metric_type)
            DO UPDATE SET levels = EXCLUDED.levels
        ''', (metric, json.dumps(levels)))

    conn.commit()
    conn.close()
    print("[INFO] Thresholds updated in the database.")


def collect_metrics():
    """
    Collects current system metrics such as CPU, memory, disk, and network usage.
    :return: Dictionary of collected metrics.
    """
    print("[INFO] Collecting system metrics...")
    metrics = {
        'cpu_percent': psutil.cpu_percent(),
        'memory_percent': psutil.virtual_memory().percent,
        'disk_percent': 100 - psutil.disk_usage('/').percent,
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

def get_alert_level(value, thresholds, descending=False):
    """
    Determines the DEFCON level for a given value based on thresholds.
    """
    if descending:
        thresholds = thresholds[::-1]
    for i, threshold in enumerate(thresholds):
        if value <= threshold:
            return i + 1
    return len(thresholds)

def alert_system(cpu, ram, disk):
    """
    Checks the system metrics against DEFCON thresholds and triggers alerts with colors.
    :param cpu: CPU usage percentage.
    :param ram: RAM usage percentage.
    :param disk: Disk usage percentage.
    """
    cpu_level = get_alert_level(cpu, DEFAULT_CPU_LEVELS)
    ram_level = get_alert_level(ram, DEFAULT_RAM_LEVELS)
    disk_level = 8-get_alert_level(disk, DEFAULT_DISK_LEVELS, descending=True)

    cpu_color = get_alert_color(cpu_level)
    ram_color = get_alert_color(ram_level)
    disk_color = get_alert_color(disk_level)

    print(f"CPU Alert: DEFCON {cpu_level} ({cpu_color})")
    print(f"RAM Alert: DEFCON {ram_level} ({ram_color})")
    print(f"Disk Alert: DEFCON {disk_level} ({disk_color})")
    
    # Store alerts in the database
    store_alert_in_db("CPU", cpu_level, cpu_color)
    store_alert_in_db("RAM", ram_level, ram_color)
    store_alert_in_db("Disk", disk_level, disk_color)

def get_sorted_input(prompt, order="asc"):
    """
    Helper function to get a sorted list of unique values from the user.
    """
    while True:
        try:
            print(prompt)
            values = list(map(int, input("Enter 5 unique numbers separated by spaces: ").split()))
            if len(set(values)) != 5:
                print("Please enter exactly 5 unique numbers.")
                continue
            values = [0] + sorted(values) + [100]
            if order == "desc":
                values = [100] + sorted(values[1:-1], reverse=True) + [0]
            return values
        except ValueError:
            print("Invalid input. Please enter numeric values only.")



def get_alert_color(defcon_level):
    """
    Maps a DEFCON level to a specific color.
    :param defcon_level: DEFCON level as an integer (1 to 6).
    :return: Corresponding alert color.
    """
    colors = {
        1: "Blue",
        2: "Green",
        3: "Yellow",
        4: "Orange",
        5: "Red",
        6: "White"  # Optional: Handle cases above the highest DEFCON level
    }
    return colors.get(defcon_level, "Unknown")




def get_db_connection():
    """
    Establishes and returns a database connection using DATABASE_CONFIG.
    """
    try:
        return connect(
            dbname=DATABASE_CONFIG['dbname'],
            user=DATABASE_CONFIG['user'],
            password=DATABASE_CONFIG['password'],
            host=DATABASE_CONFIG['host'],
            port=DATABASE_CONFIG['port']
        )
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        raise

def store_alert_in_db(metric, defcon_level, alert_color):
    """
    Stores an alert message in the database.
    :param metric: The metric type (CPU, RAM, or Disk).
    :param defcon_level: The DEFCON level.
    :param alert_color: The alert color.
    """
    try:
        # Establish the database connection
        conn = get_db_connection()
        cursor = conn.cursor()

        # SQL query to insert an alert
        insert_query = sql.SQL(
            "INSERT INTO system_alerts (metric, defcon_level, alert_color) VALUES (%s, %s, %s)"
        )

        # Execute the query with provided data
        cursor.execute(insert_query, (metric, defcon_level, alert_color))
        conn.commit()

        # Close the cursor and connection
        cursor.close()
        conn.close()

        print(f"Alert stored in database: {metric} DEFCON {defcon_level} ({alert_color})")
    except Exception as e:
        print(f"Error storing alert: {e}")



# def main():
#     run = input('Want to update the Alert Level Configuration [y/n]')
#     if run.lower() == 'y':
#         global DEFAULT_CPU_LEVELS, DEFAULT_RAM_LEVELS, DEFAULT_DISK_LEVELS
#         DEFAULT_CPU_LEVELS = get_sorted_input("Enter CPU usage thresholds:")
#         DEFAULT_RAM_LEVELS = get_sorted_input("Enter RAM usage thresholds:")
#         DEFAULT_DISK_LEVELS = get_sorted_input("Enter Disk usage thresholds (descending):", order="desc")




def insert_endpoint_data(cursor, endpoint_name, request_count, avg_response_time, timestamp):
    """
    Insert the request count, average response time, and timestamp for an endpoint into the database.
    If the endpoint already exists, it will not update the existing record.
    """
    try:
        # Insert the data into the `endpoints` table
        cursor.execute('''
            INSERT INTO endpoints (name_endpoint, request_count, avg_response_time, last_updated)
            VALUES (%s, %s, %s, %s);
        ''', (endpoint_name, request_count, avg_response_time, timestamp))

        print(f"[INFO] Successfully inserted data for endpoint '{endpoint_name}' with average response time {avg_response_time}.")
    except Exception as e:
        print(f"[ERROR] Failed to insert data for endpoint '{endpoint_name}': {e}")






# def monitor_request_counts(log_file="request_counts_log.json"):
#     """
#     Fetch and log request counts and average response times for each endpoint.
#     The counts and response times are reset by the application after each fetch.
#     """
#     url = "http://localhost:8081/monitor/request_counts"
#     # Connect to the database
#     conn = psycopg2.connect(**DATABASE_CONFIG)
#     cursor = conn.cursor()
#     try:
#         response = requests.get(url)
#         if response.status_code == 200:
#             data = response.json()
#             request_counts = data.get("request_counts", {})
#             total_response_times = data.get("total_response_times", {})

#             # Calculate average response times
#             avg_response_times = {
#                 endpoint: (total_response_times.get(endpoint, 0.0) / count if count > 0 else 0.0)
#                 for endpoint, count in request_counts.items()
#             }

#             # Prepare log data
#             log_data = {
#                 "timestamp": datetime.now().isoformat(),  # Current timestamp
#                 "endpoints": {
#                     endpoint: {
#                         "request_count": request_counts.get(endpoint, 0),
#                         "average_response_time": avg_response_times.get(endpoint, 0.0)
#                     }
#                     for endpoint in request_counts
#                 }
#             }
#             # Commit the transaction
#             conn.commit()

#             # Append log to the JSON file
#             with open(log_file, "a") as f:
#                 f.write(json.dumps(log_data) + "\n")
            
#             # Print for monitoring purposes
#             print(f"[{log_data['timestamp']}] Logged Data: {log_data['endpoints']}")
#         else:
#             print(f"Failed to retrieve request counts. Status Code: {response.status_code}")
#     except Exception as e:
#         print(f"Error connecting to the application: {e}")
        
#     finally:
#         # Close the database connection
#         cursor.close()
#         conn.close()
def monitor_request_counts(log_file="request_counts_log.json"):
    """
    Fetch and log request counts and average response times for each endpoint.
    The counts and response times are reset by the application after each fetch.
    """
    url = "http://localhost:8081/monitor/request_counts"
    # Connect to the database
    conn = psycopg2.connect(**DATABASE_CONFIG)
    cursor = conn.cursor()
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            request_counts = data.get("request_counts", {})
            total_response_times = data.get("total_response_times", {})

            # Calculate average response times
            avg_response_times = {
                endpoint: (total_response_times.get(endpoint, 0.0) / count if count > 0 else 0.0)
                for endpoint, count in request_counts.items()
            }

            # Prepare log data
            log_data = {
                "timestamp": datetime.now().isoformat(),  # Current timestamp
                "endpoints": {
                    endpoint: {
                        "request_count": request_counts.get(endpoint, 0),
                        "average_response_time": avg_response_times.get(endpoint, 0.0)
                    }
                    for endpoint in request_counts
                }
            }

            

            # Append log to the JSON file
            # with open(log_file, "a") as f:
            #     f.write(json.dumps(log_data) + "\n")
            
            # Print for monitoring purposes
            print(f"[{log_data['timestamp']}] Logged Data: {log_data['endpoints']}")

            # Insert data into the database
            for endpoint, data in log_data["endpoints"].items():
                insert_endpoint_data(cursor, endpoint, data["request_count"], data["average_response_time"], log_data["timestamp"])
            
            # Commit the transaction
            conn.commit()
        else:
            print(f"Failed to retrieve request counts. Status Code: {response.status_code}")
    except Exception as e:
        print(f"Error connecting to the application: {e}")
        
    finally:
        # Close the database connection
        cursor.close()
        conn.close()


# # Function to monitor request counts
# def monitor_request_counts(log_file="request_counts_log.json"):
#     """
#     Fetch and log request counts for each minute.
#     The counts are reset by the application after each fetch.
#     """
#     url = "http://localhost:8081/monitor/request_counts"
    
#     # Connect to the database
#     conn = psycopg2.connect(**DATABASE_CONFIG)
#     cursor = conn.cursor()

#     try:
#         response = requests.get(url)
#         if response.status_code == 200:
#             data = response.json()
#             log_data = {
#                 "timestamp": datetime.now().isoformat(),  # Current timestamp
#                 "endpoints": data.get("request_counts", {})  # Log request counts
#             }
            
#             # # Write log data to a JSON file
#             # with open(log_file, "a") as f:
#             #     f.write(json.dumps(log_data) + "\n")
            
#             # print(f"[{log_data['timestamp']}] Logged Request Counts: {log_data['endpoints']}")

#             # Insert data into the database
#             for endpoint, count in log_data["endpoints"].items():
#                 insert_endpoint_data(cursor, endpoint, count, log_data["timestamp"])
            
#             # Commit the transaction
#             conn.commit()
        
#         else:
#             print(f"Failed to retrieve request counts. Status Code: {response.status_code}")
    
#     except Exception as e:
#         print(f"[ERROR] Error connecting to the application: {e}")
    
#     finally:
#         # Close the database connection
#         cursor.close()
#         conn.close()
        

def main():
    """
    Main function to configure thresholds and start monitoring.
    """
    setup_database()
    run = input('Want to update the Alert Level Configuration [y/n]? ').lower()
    thresholds_from_db = get_thresholds_from_db()

    global DEFAULT_CPU_LEVELS, DEFAULT_RAM_LEVELS, DEFAULT_DISK_LEVELS

    if run == 'y':
        DEFAULT_CPU_LEVELS = get_sorted_input("Enter CPU usage thresholds:")
        DEFAULT_RAM_LEVELS = get_sorted_input("Enter RAM usage thresholds:")
        DEFAULT_DISK_LEVELS = get_sorted_input("Enter Disk usage thresholds (descending):", order="desc")

        # Update thresholds in the database
        update_thresholds_in_db(DEFAULT_CPU_LEVELS, DEFAULT_RAM_LEVELS, DEFAULT_DISK_LEVELS)
    elif thresholds_from_db:
        # Load thresholds from the database
        DEFAULT_CPU_LEVELS = thresholds_from_db.get("cpu", DEFAULT_CPU_LEVELS)
        DEFAULT_RAM_LEVELS = thresholds_from_db.get("ram", DEFAULT_RAM_LEVELS)
        DEFAULT_DISK_LEVELS = thresholds_from_db.get("disk", DEFAULT_DISK_LEVELS)
    else:
        print("[INFO] Using default static thresholds.")
# Function to insert or update endpoint data


def monitor_system():
    """
    Continuously monitors the system, collects metrics, and stores them in batches.
    Manages database size by archiving old records.
    """
    
    batch = []

    print("[INFO] Starting system monitoring...")
    while True:
        try:
            # Collect metrics
            current_metrics = collect_metrics()
            timestamp = datetime.now().isoformat()
            
            alert_system(
                cpu=current_metrics['cpu_percent'],
                ram=current_metrics['memory_percent'],
                disk=current_metrics['disk_percent']
            )
            # Add the collected metrics to the batch
            batch.append((timestamp, current_metrics['cpu_percent'], current_metrics['memory_percent'],
                          current_metrics['disk_percent'], current_metrics['network_bytes_sent'], current_metrics['network_bytes_recv']))
            
            print(f"[INFO] Metrics collected and added to the batch. Current batch : {batch}")
            print(f"[INFO] Current batch size: {len(batch)}") 
            # Log file location
            log_file_name = "request_counts_log.json"
            monitor_request_counts(log_file_name)

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
    main()
    monitor_system()
