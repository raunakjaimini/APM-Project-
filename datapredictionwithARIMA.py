import psycopg2
import time
from datetime import datetime

# PostgreSQL configuration
DATABASE_CONFIG = {
    'dbname': 'postgres',
    'user': 'postgres.cwkrmwnrdglcuffvlhss',
    'password': 'R@unak87709',
    'host': 'aws-0-ap-south-1.pooler.supabase.com',
    'port': '6543'
}

def setup_database():
    """
    Ensures that the 'predicted_metrics' table exists in the PostgreSQL database.
    """
    print("[INFO] Setting up the database...")
    conn = psycopg2.connect(**DATABASE_CONFIG)
    cursor = conn.cursor()

    # Create the predicted_metrics table with separate columns for each metric
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS predicted_metric (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            cpu_percent NUMERIC,
            memory_percent NUMERIC,
            disk_percent NUMERIC,
            network_bytes_sent NUMERIC,
            network_bytes_recv NUMERIC
        )
    """)
    conn.commit()
    conn.close()
    print("[INFO] Database setup completed.")

def difference(data, lag=1):
    """
    Calculates the differenced series to make the data stationary.
    :param data: List of values.
    :param lag: Number of lags for differencing.
    :return: Differenced series.
    """
    return [data[i] - data[i - lag] for i in range(lag, len(data))]

def predict_future_value_arima(data_points, p=2, q=2):
    """
    Predicts future values using ARIMA logic.
    Differencing (difference): Removes trends to make the data stationary.
    AutoRegressive Component: Uses a weighted sum of the past p observations.
    Moving Average Component: Uses a weighted sum of past q errors.
    Combination: Combines AR and MA for final predictions.
    """
    print(f"[INFO] Calculating ARIMA-based prediction for data points: {data_points}")
    if len(data_points) <= max(p, q):
        print("[WARNING] Not enough data points for ARIMA prediction.")
        return None

    # Step 1: Make the series stationary by differencing
    differenced = difference(data_points)
    
    # Step 2: Calculate AR component (linear combination of past p values)
    ar_part = 0
    for i in range(1, p + 1):
        ar_part += differenced[-i]  # Use last p differenced values

    # Step 3: Calculate MA component (linear combination of past q errors)
    ma_part = 0
    residuals = [0] * len(differenced)  # Initialize residuals
    for i in range(1, q + 1):
        if len(residuals) >= i:
            ma_part += residuals[-i]  # Use last q residuals

    # Combine AR and MA parts
    predicted_difference = ar_part + ma_part

    # Undo differencing to get final prediction
    prediction = data_points[-1] + predicted_difference

    print(f"[INFO] ARIMA-based prediction: {prediction:.2f}")
    return prediction

def fetch_metrics():
    """
    Fetches all metrics from the PostgreSQL database.
    :return: Dictionary of metrics organized by type.
    """
    print("[INFO] Fetching metrics from the database...")
    conn = psycopg2.connect(**DATABASE_CONFIG)
    cursor = conn.cursor()

    # Fetching metrics from the metrics table
    cursor.execute("""
        SELECT timestamp, cpu_percent, memory_percent, disk_percent, network_bytes_sent, network_bytes_recv 
        FROM metrics ORDER BY timestamp ASC
    """)
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        print("[WARNING] No metrics found in the database.")
    else:
        print(f"[INFO] Fetched {len(rows)} records from the database.")

    # Organize metrics by type
    metrics_by_type = {
        'cpu_percent': [],
        'memory_percent': [],
        'disk_percent': [],
        'network_bytes_sent': [],
        'network_bytes_recv': []
    }
    for row in rows:
        _, cpu, memory, disk, bytes_sent, bytes_recv = row
        metrics_by_type['cpu_percent'].append(cpu)
        metrics_by_type['memory_percent'].append(memory)
        metrics_by_type['disk_percent'].append(disk)
        metrics_by_type['network_bytes_sent'].append(bytes_sent)
        metrics_by_type['network_bytes_recv'].append(bytes_recv)

    print("[INFO] Organized metrics by type:")
    for metric_type, values in metrics_by_type.items():
        print(f"  - {metric_type}: {values}")

    return metrics_by_type

def store_prediction(predictions):
    """
    Stores the predicted values in the 'predicted_metric' table.
    :param predictions: Dictionary of predicted values by metric type.
    """
    print(f"[INFO] Storing predictions: {predictions}")
    conn = psycopg2.connect(**DATABASE_CONFIG)
    cursor = conn.cursor()

    # Insert predictions into the appropriate columns
    cursor.execute("""
        INSERT INTO predicted_metric (timestamp, cpu_percent, memory_percent, disk_percent, network_bytes_sent, network_bytes_recv)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        datetime.now(),
        predictions.get('cpu_percent'),
        predictions.get('memory_percent'),
        predictions.get('disk_percent'),
        predictions.get('network_bytes_sent'),
        predictions.get('network_bytes_recv')
    ))

    conn.commit()
    conn.close()
    print("[INFO] Predictions stored successfully.")

def generate_predictions():
    """
    Generates predictions for all metric types based on all available data and stores them in the database.
    """
    print(f"\n=== Generating Predictions at {datetime.now().isoformat()} ===")
    metrics_by_type = fetch_metrics()
    predictions = {}

    for metric_type, values in metrics_by_type.items():
        print(f"[INFO] Generating prediction for metric: {metric_type}")
        prediction = predict_future_value_arima(values)  # Use ARIMA-based prediction
        if prediction:
            print(f"[INFO] Predicted {metric_type}: {prediction:.2f}")
            predictions[metric_type] = prediction
        else:
            print(f"[WARNING] Unable to generate prediction for {metric_type} due to insufficient data.")

    if predictions:
        store_prediction(predictions)

def main():
    """
    Runs the prediction process every 5 minutes after an initial delay of 5 minutes.
    """
    print("[INFO] Starting the prediction process...")

    # Set up the database table for storing predictions
    setup_database()

    # Wait for 5 minutes before starting the first prediction cycle
    print("[INFO] Waiting for 5 minutes before starting the first prediction cycle...")
    time.sleep(30)  # Delay before starting first cycle (5 minutes)

    while True:
        try:
            generate_predictions()
            print("[INFO] Sleeping for 5 minutes before the next prediction cycle...")
            time.sleep(120)  # Run every 5 minutes
        except Exception as e:
            print(f"[ERROR] Error during prediction: {e}")

if __name__ == "__main__":
    print("[INFO] Starting the prediction script...")
    main()
    
