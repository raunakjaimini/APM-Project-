# predicted values will be stored in the table


import sqlite3
import statistics
import time
import json
from datetime import datetime

DATABASE_FILE = 'met.db'

def create_predictions_table():
    """Creates the predictions table if it doesn't exist."""
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            metric_type TEXT,
            predicted_value REAL,
            timestamp TEXT,
            data_used TEXT
        )
    ''')
    conn.commit()
    conn.close()
    print("[INFO] Predictions table created or already exists.")

def predict_future_value(data_points):
    """Predicts future values using a simple moving average based on all available data."""
    print(f"[INFO] Calculating prediction for data points: {data_points}")
    if len(data_points) < 1:
        print("[WARNING] No data points available to calculate prediction.")
        return None

    moving_avg = statistics.mean(data_points)  # Use all available data for average
    print(f"[INFO] Calculated moving average: {moving_avg:.2f}")
    prediction = moving_avg + (moving_avg * 0.1)  # Simple trend adjustment
    print(f"[INFO] Final prediction with trend adjustment: {prediction:.2f}")
    return prediction

def fetch_metrics():
    """Fetches all metrics from the database."""
    print("[INFO] Fetching metrics from the database...")
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()

    # Fetching 'timestamp' and 'metrics_data' columns
    cursor.execute('SELECT timestamp, metrics_data FROM metrics ORDER BY timestamp ASC')
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        print("[WARNING] No metrics found in the database.")
    else:
        print(f"[INFO] Fetched {len(rows)} records from the database.")

    # Organize metrics by type (assuming metrics_data contains JSON-encoded data)
    metrics_by_type = {}
    for timestamp, metrics_json in rows:
        metrics = json.loads(metrics_json)
        for metric_type, value in metrics.items():
            if metric_type not in metrics_by_type:
                metrics_by_type[metric_type] = []
            metrics_by_type[metric_type].append(value)

    print("[INFO] Organized metrics by type:")
    for metric_type, values in metrics_by_type.items():
        print(f"  - {metric_type}: {values}")

    return metrics_by_type

def store_prediction(metric_type, predicted_value, data_used):
    """Stores the prediction in the predictions table."""
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO predictions (metric_type, predicted_value, timestamp, data_used)
        VALUES (?, ?, ?, ?)
    ''', (metric_type, predicted_value, datetime.now().isoformat(), json.dumps(data_used)))
    conn.commit()
    conn.close()
    print(f"[INFO] Stored prediction for {metric_type}: {predicted_value:.2f}")

def generate_predictions():
    """Generates predictions for all metric types based on all available data."""
    print(f"\n=== Generating Predictions at {datetime.now().isoformat()} ===")
    metrics_by_type = fetch_metrics()

    for metric_type, values in metrics_by_type.items():
        print(f"[INFO] Generating prediction for metric: {metric_type}")
        prediction = predict_future_value(values)  # Use all data in the database
        if prediction is not None:
            print(f"[INFO] Predicted {metric_type}: {prediction:.2f}")
            store_prediction(metric_type, prediction, values)
        else:
            print(f"[WARNING] Unable to generate prediction for {metric_type} due to insufficient data.")

def main():
    """Runs the prediction process every 5 minutes after an initial delay of 5 minutes."""
    print("[INFO] Starting the prediction process...")
    create_predictions_table()  # Ensure the predictions table exists

    # Wait for 5 minutes before starting the first prediction cycle
    print("[INFO] Waiting for 5 minutes before starting the first prediction cycle...")
    time.sleep(300)  # Delay before starting first cycle (5 minutes)

    while True:
        try:
            generate_predictions()
            print("[INFO] Sleeping for 5 minutes before the next prediction cycle...")
            time.sleep(300)  # Run every 5 minutes
        except Exception as e:
            print(f"[ERROR] Error during prediction: {e}")

if __name__ == "__main__":
    print("[INFO] Starting the prediction script...")
    main()
