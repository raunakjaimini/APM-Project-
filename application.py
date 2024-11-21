from fastapi import FastAPI
from datetime import datetime
import uvicorn

# Initialize the FastAPI app
app = FastAPI()

# Request count trackers
request_counts = {"articles": 0, "blogs": 0, "news": 0}

@app.get("/articles", tags=["Articles"])
async def get_articles():
    """
    Handle GET requests to /articles.
    Increment the request count for this endpoint.
    """
    request_counts["articles"] += 1
    return {"message": "Articles endpoint accessed.", "timestamp": datetime.now()}

@app.get("/blogs", tags=["Blogs"])
async def get_blogs():
    """
    Handle GET requests to /blogs.
    Increment the request count for this endpoint.
    """
    request_counts["blogs"] += 1
    return {"message": "Blogs endpoint accessed.", "timestamp": datetime.now()}

@app.get("/news", tags=["News"])
async def get_news():
    """
    Handle GET requests to /news.
    Increment the request count for this endpoint.
    """
    request_counts["news"] += 1
    return {"message": "News endpoint accessed.", "timestamp": datetime.now()}

@app.get("/monitor/request_counts", tags=["Monitoring"])
async def get_and_reset_request_counts():
    """
    Return the total number of requests received for all endpoints and reset the counters.
    """
    global request_counts
    current_counts = request_counts.copy()  # Take a snapshot of the current counts
    request_counts = {"articles": 0, "blogs": 0, "news": 0}  # Reset counts
    return {"request_counts": current_counts, "timestamp": datetime.now()}

if __name__ == "__main__":
    # Run the FastAPI app on localhost:8080
    uvicorn.run(app, host="0.0.0.0", port=8081)