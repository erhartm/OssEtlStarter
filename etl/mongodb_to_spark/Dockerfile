# Dockerfile for MongoDB to Spark ETL
FROM apache/spark-py:latest

WORKDIR /app

# Install Delta Lake and MongoDB dependencies
RUN pip install delta-spark pymongo

COPY etl_mongodb_to_spark.py .

CMD ["spark-submit", "etl_mongodb_to_spark.py"]
