# app/main.py
from fastapi import FastAPI
from app.api.v1.endpoints import router as api_router
from app.core.spark_session import SparkSessionFactory
from app.utils.sqs_listener import sqs_listener
import uvicorn
import threading

app = FastAPI()

app.include_router(api_router, prefix="/api/v1")

@app.on_event("startup")
async def startup_event():
    # Start SQS Listener in a separate thread
    sqs_thread = threading.Thread(target=sqs_listener.listen_to_sqs, args=("your-sqs-queue-url",))
    sqs_thread.start()

@app.on_event("shutdown")
async def shutdown_event():
    # Stop Spark session
    SparkSessionFactory.stop_spark_session()

if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=8000)
