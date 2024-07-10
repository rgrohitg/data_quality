from fastapi import FastAPI
from app.api.v1.endpoints import router as api_router
from app.services.converter import load_conversion_config
from app.services.shutdown import stop_spark_session

app = FastAPI()

app.include_router(api_router, prefix="/api/v1")


@app.on_event("shutdown")
async def shutdown_event():
    # Stop Spark session
    stop_spark_session()

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)