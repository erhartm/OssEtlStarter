# Dockerfile for Bronze to Silver ETL
FROM apache/spark-py:latest

WORKDIR /app

# Install Delta Lake
RUN pip install delta-spark

COPY etl_bronze_to_silver.py .

CMD ["spark-submit", "etl_bronze_to_silver.py"]
