import threading
from pyspark.sql import SparkSession
from queue import Queue

class SparkSessionPool:
    _instance = None
    _lock = threading.Lock()

    def __init__(self, pool_size: int = 5):
        if SparkSessionPool._instance is not None:
            raise Exception("This class is a singleton!")
        self.pool_size = pool_size
        self._pool = Queue(maxsize=pool_size)
        for _ in range(pool_size):
            self._pool.put(self._create_spark_session())
        SparkSessionPool._instance = self

    @staticmethod
    def get_instance(pool_size: int = 5):
        if SparkSessionPool._instance is None:
            with SparkSessionPool._lock:
                if SparkSessionPool._instance is None:
                    SparkSessionPool(pool_size)
        return SparkSessionPool._instance

    def _create_spark_session(self) -> SparkSession:
        return (SparkSession.builder
                .appName("SodaQualityChecks")
                .config("spark.executor.memory", "4g")
                .config("spark.driver.memory", "2g")
                .config("spark.local.dir", "./data")
                .config("spark.ui.port", "4040")
                .getOrCreate())

    def get_spark_session(self) -> SparkSession:
        return self._pool.get()

    def return_spark_session(self, spark_session: SparkSession):
        self._pool.put(spark_session)

# FastAPI endpoint example using SparkSessionPool
from fastapi import APIRouter, HTTPException, Depends
from app.models.models import ValidationResult, DataValidationRequest

router = APIRouter()

def get_spark_session():
    pool = SparkSessionPool.get_instance()
    spark_session = pool.get_spark_session()
    try:
        yield spark_session
    finally:
        pool.return_spark_session(spark_session)

@router.post("/validate", response_model=ValidationResult)
def validate_data(request: DataValidationRequest, spark: SparkSession = Depends(get_spark_session)) -> ValidationResult:
    try:
        if request.file_type == 'csv':
            validation_result = validate_csv(request.data_file_path, request.spec_file_path, spark)
        elif request.file_type == 'excel':
            validation_result = validate_excel(request.data_file_path, request.spec_file_path, spark)
        else:
            raise HTTPException(status_code=400, detail="Unsupported file type")

        return validation_result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Usage example
from fastapi import FastAPI
from app.api.v1.endpoints import router as api_router

app = FastAPI()
app.include_router(api_router, prefix="/api/v1")

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
