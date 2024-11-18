# working code file 

import sqlite3
import statistics
import time
import json
from datetime import datetime

DATABASE_FILE = 'met.db'

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

    # Fetching 'timestamp' and 'metrics_data' columns, since the columns should match Code 1's structure
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

def generate_predictions():
    """Generates predictions for all metric types based on all available data."""
    print(f"\n=== Generating Predictions at {datetime.now().isoformat()} ===")
    metrics_by_type = fetch_metrics()

    for metric_type, values in metrics_by_type.items():
        print(f"[INFO] Generating prediction for metric: {metric_type}")
        prediction = predict_future_value(values)  # Use all data in the database
        if prediction:
            print(f"[INFO] Predicted {metric_type}: {prediction:.2f}")
        else:
            print(f"[WARNING] Unable to generate prediction for {metric_type} due to insufficient data.")

def main():
    """Runs the prediction process every 5 minutes after an initial delay of 5 minutes."""
    print("[INFO] Starting the prediction process...")

    # Wait for 5 minutes before starting the first prediction cycle
    print("[INFO] Waiting for 5 minutes before starting the first prediction cycle...")
    time.sleep(60)  # Delay before starting first cycle (5 minutes)

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
