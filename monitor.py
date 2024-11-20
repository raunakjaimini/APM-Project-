import requests

def monitor_request_count():
    """
    Periodically monitor the request count of the application.
    """
    url = "http://localhost:8080/monitor/request_count"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            print(f"[{data['timestamp']}] Request Count: {data['request_count']}")
        else:
            print(f"Failed to retrieve request count. Status Code: {response.status_code}")
    except Exception as e:
        print(f"Error connecting to the application: {e}")

if __name__ == "__main__":
    import time
    # Monitor the request count every 5 seconds
    while True:
        monitor_request_count()
        time.sleep(5)
