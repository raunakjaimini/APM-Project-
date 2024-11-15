import psutil
import time
import numpy as np
import pandas as pd
from scipy.stats import linregress
import json

class SystemMonitorSDK:
    def __init__(self, log_interval=1):
        """
        Initialize the SystemMonitorSDK with a logging interval.
        :param log_interval: Interval in seconds for logging system metrics.
        """
        self.log_interval = log_interval
        self.cpu_usage_history = []  # Stores historical CPU usage data
        self.memory_usage_history = []  # Stores historical memory usage data
        self.system_log = []  # Stores each log entry as a dictionary

    def collect_metrics(self):
        """
        Collects real-time CPU and memory metrics.
        Returns a dictionary with CPU percentage, memory usage, CPU frequency,
        CPU temperature (if available), and available memory.
        """
        cpu_percent = psutil.cpu_percent()  # Get CPU usage as a percentage
        memory = psutil.virtual_memory()  # Get memory usage details
        
        # Append data for historical tracking
        self.cpu_usage_history.append(cpu_percent)
        self.memory_usage_history.append(memory.percent)
        
        return {
            "cpu_usage_percent": cpu_percent,
            "memory_usage_percent": memory.percent,
            "cpu_frequency_mhz": psutil.cpu_freq().current,
            "cpu_temperature_celsius": self.get_cpu_temperature(),
            "available_memory_mb": memory.available / (1024 * 1024)  # Convert bytes to MB
        }
    
    def get_cpu_temperature(self):
        """
        Fetches CPU temperature if available.
        Returns a list of temperatures or None if not supported.
        """
        try:
            temps = psutil.sensors_temperatures()
            if 'coretemp' in temps:
                return [temp.current for temp in temps['coretemp']]
        except AttributeError:
            return None
        return None
    
    def calculate_trends(self):
        """
        Calculate statistical trends for historical CPU and memory usage.
        Uses `pd.Series` to calculate max, average, and standard deviation,
        which simplifies aggregation operations on historical data.
        """
        cpu_series = pd.Series(self.cpu_usage_history)
        memory_series = pd.Series(self.memory_usage_history)
        
        trends = {
            "cpu_peak_usage_percent": cpu_series.max(),
            "memory_peak_usage_percent": memory_series.max(),
            "cpu_avg_usage_percent": cpu_series.mean(),
            "memory_avg_usage_percent": memory_series.mean(),
            "cpu_usage_std_dev": cpu_series.std(),
            "memory_usage_std_dev": memory_series.std(),
        }
        
        return trends
    
    def predict_usage(self):
        """
        Predicts future CPU and memory usage growth based on past data.
        Calculates the slope (growth rate) manually without using prebuilt functions.
        """
        # Check if we have enough data points for a reliable prediction
        if len(self.cpu_usage_history) < 10:
            return {"error": "Insufficient data for prediction"}
        
        # Convert data history to series for easy computation
        cpu_series = pd.Series(self.cpu_usage_history)
        memory_series = pd.Series(self.memory_usage_history)
        
        # Create a time index to represent observation intervals
        time_index = np.arange(len(cpu_series))
        
        # Calculate mean values for time and usage data
        time_mean = np.mean(time_index)
        cpu_mean = np.mean(cpu_series)
        memory_mean = np.mean(memory_series)
        
        # Calculate the slope (growth rate) and intercept for CPU usage
        cpu_numerator = np.sum((time_index - time_mean) * (cpu_series - cpu_mean))
        cpu_denominator = np.sum((time_index - time_mean) ** 2)
        cpu_slope = cpu_numerator / cpu_denominator
        cpu_intercept = cpu_mean - (cpu_slope * time_mean)
        
        # Calculate the slope (growth rate) and intercept for memory usage
        memory_numerator = np.sum((time_index - time_mean) * (memory_series - memory_mean))
        memory_denominator = np.sum((time_index - time_mean) ** 2)
        memory_slope = memory_numerator / memory_denominator
        memory_intercept = memory_mean - (memory_slope * time_mean)
        
        # Predictions based on the calculated slopes (growth rates)
        predictions = {
            "predicted_cpu_growth_rate_percent": cpu_slope,
            "predicted_memory_growth_rate_percent": memory_slope,
            "cpu_usage_prediction_next_interval": cpu_slope * (len(cpu_series) + 1) + cpu_intercept,
            "memory_usage_prediction_next_interval": memory_slope * (len(memory_series) + 1) + memory_intercept
        }
        
        return predictions

    
    def log_metrics(self):
        """
        Logs metrics at regular intervals along with calculated trends and predictions.
        Saves data to a JSON file after each entry.
        """
        while True:
            # Collect current metrics
            metrics = self.collect_metrics()
            
            # Calculate historical trends
            trends = self.calculate_trends()
            
            # Predict future usage trends
            predictions = self.predict_usage()
            
            # Combine metrics, trends, and predictions into a single log entry
            log_entry = {
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "current_metrics": metrics,
                "historical_trends": trends,
                "predictions": predictions
            }
            
            # Add entry to the system log
            self.system_log.append(log_entry)
            
            # Write the log to a JSON file for persistent storage
            with open("system_monitor_log.json", "w") as json_file:
                json.dump(self.system_log, json_file, indent=4)
            
            print("Logged data:", log_entry)
            time.sleep(self.log_interval)  # Wait for the specified interval

# Example usage
monitor = SystemMonitorSDK(log_interval=1)
monitor.log_metrics()
