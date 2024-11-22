import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Database connection configuration
DATABASE_CONFIG = {
    'dbname': 'postgres',
    'user': 'postgres.cwkrmwnrdglcuffvlhss',
    'password': 'R@unak87709',
    'host': 'aws-0-ap-south-1.pooler.supabase.com',
    'port': '6543'
}

def get_data_from_db(query):
    """
    Fetches data from the database using the provided SQL query.

    Args:
    - query (str): The SQL query to be executed on the database.

    Returns:
    - DataFrame: A pandas DataFrame containing the fetched data.
    """
    try:
        conn = psycopg2.connect(**DATABASE_CONFIG)
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error connecting to the database: {e}")
        return pd.DataFrame()

def plot_time_series(df, metric_column, title):
    """
    Plots an interactive time series graph using Plotly.

    Args:
    - df (DataFrame): DataFrame containing the data.
    - metric_column (str): The column representing the metric to plot.
    - title (str): Title of the plot.
    """
    fig = px.line(
        df,
        x='timestamp',
        y=metric_column,
        title=title,
        labels={'timestamp': 'Time', metric_column: metric_column.replace('_', ' ').capitalize()},
        template='plotly_white'
    )
    fig.update_traces(line=dict(width=2))
    st.plotly_chart(fig, use_container_width=True)

def plot_peak_utilization(df, metric_column, title):
    """
    Plots the peak utilization graph using Plotly.

    Args:
    - df (DataFrame): DataFrame containing the data.
    - metric_column (str): The column representing the metric.
    - title (str): Title of the plot.
    """
    max_value = df[metric_column].max()
    fig = go.Figure()

    # Line plot
    fig.add_trace(
        go.Scatter(
            x=df['timestamp'],
            y=df[metric_column],
            mode='lines',
            name=metric_column,
            line=dict(color='blue', width=2)
        )
    )

    # Add a horizontal line for the peak
    fig.add_trace(
        go.Scatter(
            x=df['timestamp'],
            y=[max_value] * len(df),
            mode='lines',
            name=f"Peak ({max_value:.2f}%)",
            line=dict(color='red', dash='dash'),
            showlegend=True
        )
    )

    # Annotate the peak value
    peak_time = df[df[metric_column] == max_value]['timestamp'].iloc[0]
    fig.add_annotation(
        x=peak_time,
        y=max_value,
        text=f"Peak: {max_value:.2f}%",
        showarrow=True,
        arrowhead=2,
        ax=0,
        ay=-40,
        font=dict(color="red", size=12)
    )

    # Update layout
    fig.update_layout(
        title=title,
        xaxis_title='Time',
        yaxis_title=f'{metric_column.replace("_", " ").capitalize()} (%)',
        template='plotly_white'
    )
    st.plotly_chart(fig, use_container_width=True)

def main():
    """
    Main function to set up the Streamlit app, fetch data, and display metrics and graphs.
    """
    st.title("System Metrics Dashboard")
    st.markdown("Monitor CPU, RAM, and Disk usage metrics in real-time.")

    # Fetch data from the database
    query = "SELECT * FROM metrics ORDER BY timestamp DESC LIMIT 100"
    metrics_df = get_data_from_db(query)

    if metrics_df.empty:
        st.warning("No data available.")
        return

    # Convert timestamp to datetime
    metrics_df['timestamp'] = pd.to_datetime(metrics_df['timestamp'])

    # Display the data
    st.header("Live Metrics Data")
    st.dataframe(metrics_df)

    # Plot metrics
    st.header("Metrics Visualization")
    plot_time_series(metrics_df, 'cpu_percent', 'CPU Utilization Over Time')
    plot_time_series(metrics_df, 'memory_percent', 'RAM Utilization Over Time')
    plot_time_series(metrics_df, 'disk_percent', 'Disk Utilization Over Time')

    # Plot peak utilization
    st.header("Peak Utilization")
    plot_peak_utilization(metrics_df, 'cpu_percent', 'CPU Peak Utilization')
    plot_peak_utilization(metrics_df, 'memory_percent', 'RAM Peak Utilization')
    plot_peak_utilization(metrics_df, 'disk_percent', 'Disk Peak Utilization')

if __name__ == "__main__":
    main()
