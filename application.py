from fastapi import FastAPI, Request
from datetime import datetime
import uvicorn
import time

# Initialize the FastAPI app
app = FastAPI()

# Global variables to track request counts and total response times
request_counts = {"articles": 0, "blogs": 0, "news": 0}
response_times = {"articles": 0.0, "blogs": 0.0, "news": 0.0}

@app.get("/articles", tags=["Articles"])
async def get_articles():
    """
    Handle GET requests to /articles.
    """
    return {"message": "Articles endpoint accessed.", "timestamp": datetime.now()}

@app.get("/blogs", tags=["Blogs"])
async def get_blogs():
    """
    Handle GET requests to /blogs.
    """
    return {"message": "Blogs endpoint accessed.", "timestamp": datetime.now()}

@app.get("/news", tags=["News"])
async def get_news():
    """
    Handle GET requests to /news.
    """
    return {"message": "News endpoint accessed.", "timestamp": datetime.now()}

# Middleware to calculate response times and track requests
@app.middleware("http")
async def track_metrics(request: Request, call_next):
    start_time = time.perf_counter()
    response = await call_next(request)
    end_time = time.perf_counter()

    # Extract the endpoint from the request
    endpoint = request.url.path.strip("/")
    
    # Update metrics if the endpoint is being tracked
    if endpoint in request_counts:
        request_counts[endpoint] += 1
        response_times[endpoint] += (end_time - start_time)
    
    return response

@app.get("/monitor/request_counts", tags=["Monitoring"])
async def get_and_reset_request_counts():
    """
    Return the total number of requests received and total response times for each endpoint.
    Reset both request counts and response times after fetching.
    """
    global request_counts, response_times
    current_counts = request_counts.copy()  # Snapshot of counts
    current_response_times = response_times.copy()  # Snapshot of response times

    # Reset the counters
    request_counts = {key: 0 for key in request_counts}
    response_times = {key: 0.0 for key in response_times}
    
    return {
        "request_counts": current_counts,
        "total_response_times": current_response_times,
        "timestamp": datetime.now(),
    }

if __name__ == "__main__":
    # Run the FastAPI app on localhost:8081
    uvicorn.run(app, host="0.0.0.0", port=8081)