from fastapi import FastAPI
from datetime import datetime
import uvicorn

# Initialize the FastAPI app
app = FastAPI()

# Request count tracker
request_count = 0

@app.get("/articles", tags=["Articles"])
async def get_articles():
    """
    Handle GET requests to /articles.
    Increment the request count each time this endpoint is accessed.
    """
    global request_count
    request_count += 1
    return {"message": "Articles endpoint accessed.", "timestamp": datetime.now()}

@app.get("/monitor/request_count", tags=["Monitoring"])
async def get_request_count():
    """
    Return the total number of requests received by the /articles endpoint.
    """
    return {"request_count": request_count, "timestamp": datetime.now()}

if __name__ == "__main__":
    # Run the FastAPI app on localhost:8080
    uvicorn.run(app, host="0.0.0.0", port=8080)
