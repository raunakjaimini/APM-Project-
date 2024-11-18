import time
import requests
import pandas as pd
import matplotlib.pyplot as plt

# Configuration
api_endpoint = "http://localhost:8080/articles"  # Replace with your API endpoint
interval = 1.0  # Request interval in seconds
response_time_threshold = 1.0  # Threshold in seconds for response time alerts

# Initialize data storage for response times
response_times = []
timestamps = []
request_count = 0  # Initialize request counter

# Function to measure API response time
def track_api_response_time():
    try:
        start_time = time.time()
        response = requests.get(api_endpoint)
        response.raise_for_status()  # Ensure request was successful
        response_time = time.time() - start_time
    except requests.RequestException as e:
        response_time = None  # Mark as None if request fails
        print(f"Error reaching API: {e}")
    return response_time

# Monitor and log response times
def monitor_api():
    global request_count  # Use the global counter variable
    print("Starting API response time monitoring...")
    while True:
        response_time = track_api_response_time()
        current_time = time.time()
        request_count += 1  # Increment request counter

        if response_time is not None:
            response_times.append(response_time)
            timestamps.append(current_time)
            print(f"[{time.ctime(current_time)}] Response Time: {response_time:.4f} seconds")

            # Check for high response time
            if response_time > response_time_threshold:
                print(f"ALERT: High response time detected - {response_time:.2f} seconds")
        else:
            print("No response received from API.")

        print(f"Total requests sent so far: {request_count}")  # Print request count
        # Wait before sending the next request
        time.sleep(interval)

# Run monitoring function
try:
    monitor_api()
except KeyboardInterrupt:
    print("\nMonitoring stopped by user.")
    print(f"Total requests sent during monitoring: {request_count}")  # Final count
    
