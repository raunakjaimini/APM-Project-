import psutil 
import time 
import os
import statistics
import sqlite3
import json
from datetime import datetime 
import threading
import logging 
from collections import deque
import requests

def setup_database():
    '''
    Creates the database tables for storing metrics and insights.
    timestamp, type, value, batch ID store the data
    '''
    print("Setting up database...")
    
    conn = sqlite3.connect('metrics.db')
    cursor = conn.cursor()
    
    cursor.execute('''
            CREATE TABLE IF NOT EXISTS metrics (
                timestamp TEXT,
                metric_type TEXT,
                value REAL,
                batch_id TEXT
            )
        ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS insights (
            timestamp TEXT,
            insight_type TEXT,
            description TEXT
        )
    ''')
    
    conn.commit()
    conn.close()
    
    print("Database setup complete.")


# Collect system metrics
def collect_metrics():
    metrics = {
        'cpu_percent': psutil.cpu_percent(),
        'memory_percent': psutil.virtual_memory().percent,
        'disk_percent': psutil.disk_usage('/').percent,
        'network_bytes_sent': psutil.net_io_counters().bytes_sent,
        'network_bytes_recv': psutil.net_io_counters().bytes_recv
    }
    print(f"Collected metrics: {metrics}")
    return metrics  

def calculate_statistics(data_points):
    if not data_points:
        return None

    # Convert deque to list for statistical calculations
    data_list = list(data_points)

    stats = {
        'mean': statistics.mean(data_list),
        'median': statistics.median(data_list),
        'std_dev': statistics.stdev(data_list) if len(data_list) > 1 else 0,
        'min': min(data_list),
        'max': max(data_list)
    }
    print(f"Calculated statistics: {stats}")
    return stats

# WAL (write-ahead-log) implementation
def write_to_wal(data):
    print(f"Writing to WAL: {data}")
    with open('wal.log', 'a') as f:
        f.write(json.dumps(data) + '\n')


# Batch processing from WAL to database
def process_wal_batch():
    print("Processing WAL batch...")
    batched_data = []
    if not os.path.exists('wal.log'):
        return

    conn = sqlite3.connect('metrics.db')
    cursor = conn.cursor()

    with open('wal.log', 'r') as f:
        for line in f:
            try:
                data = json.loads(line.strip())
                batched_data.append((
                    data['timestamp'],
                    data['metric_type'],
                    data['value'],
                    data['batch_id']
                ))
            except json.JSONDecodeError:
                print(f"Error processing WAL line: {line}")
                continue

    if batched_data:
        cursor.executemany(
            'INSERT INTO metrics VALUES (?, ?, ?, ?)',
            batched_data
        )
        conn.commit()

    conn.close()
    # Clear WAL after successful processing
    open('wal.log', 'w').close()
    print(f"Processed {len(batched_data)} records from WAL")


# Predict future values using simple moving average
def predict_future_value(data_points, periods=5):
    if len(data_points) < periods:
        return None

    # Convert to list and get the last 'periods' elements
    recent_data = list(data_points)[-periods:]
    moving_avg = statistics.mean(recent_data)
    prediction = moving_avg + (moving_avg * 0.1)  # Simple trend adjustment
    print(f"Predicted value: {prediction}")
    return prediction


# Generate insights
def generate_insights(metrics_history):
    insights = []

    for metric_type, values in metrics_history.items():
        # Convert deque to list for processing
        values_list = list(values)
        stats = calculate_statistics(values_list)
        if not stats:
            continue

        # Trend analysis
        if len(values_list) >= 3:
            recent_trend = values_list[-3:]
            if all(x < y for x, y in zip(recent_trend, recent_trend[1:])):
                insights.append(f"Steady increase in {metric_type}")
            elif all(x > y for x, y in zip(recent_trend, recent_trend[1:])):
                insights.append(f"Steady decrease in {metric_type}")

        # Threshold violations
        if metric_type == 'cpu_percent':
            if stats['mean'] > 80:
                insights.append(f"High CPU utilization: {stats['mean']:.2f}%")
            elif stats['mean'] < 20:
                insights.append(f"Low CPU utilization: {stats['mean']:.2f}%")

        # Variability analysis
        if stats['std_dev'] > stats['mean'] * 0.5:
            insights.append(f"High variability in {metric_type}")

    print(f"Generated {len(insights)} insights")
    return insights

# Main monitoring loop
def monitor_system(duration_minutes=15, interval_seconds=60):
    print(f"Starting system monitoring for {duration_minutes} minutes...")
    setup_database()

    metrics_history = {
        'cpu_percent': deque(maxlen=100),
        'memory_percent': deque(maxlen=100),
        'disk_percent': deque(maxlen=100),
        'network_bytes_sent': deque(maxlen=100),
        'network_bytes_recv': deque(maxlen=100)
    }

    end_time = time.time() + (duration_minutes * 60)
    batch_id = datetime.now().strftime('%Y%m%d%H%M%S')

    while time.time() < end_time:
        try:
            # Collect metrics
            current_metrics = collect_metrics()
            timestamp = datetime.now().isoformat()

            # Update history
            for metric_type, value in current_metrics.items():
                metrics_history[metric_type].append(value)

                # Write to WAL
                wal_entry = {
                    'timestamp': timestamp,
                    'metric_type': metric_type,
                    'value': value,
                    'batch_id': batch_id
                }
                write_to_wal(wal_entry)

            # Generate insights every minute
            insights = generate_insights(metrics_history)

            # Display current status
            print("\n=== Current System Status ===")
            for metric_type, value in current_metrics.items():
                print(f"{metric_type}: {value}")

            # Display predictions
            print("\n=== Predictions ===")
            for metric_type, values in metrics_history.items():
                prediction = predict_future_value(values)
                if prediction:
                    print(f"Predicted {metric_type}: {prediction:.2f}")

            # Display insights (show last 5 insights)
            print("\n=== Recent Insights ===")
            recent_insights = insights[-5:] if len(insights) > 5 else insights
            for insight in recent_insights:
                print(f"- {insight}")

            # Process WAL batch every 5 minutes
            if int(time.time()) % 300 < interval_seconds:
                process_wal_batch()

            time.sleep(interval_seconds)

        except Exception as e:
            print(f"Error in monitoring loop: {e}")
            continue
        
if __name__ == "__main__":
    print("Starting performance monitoring application...")
    monitor_system(duration_minutes=15, interval_seconds=60)