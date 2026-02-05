# Dockerfile for SQL Server to Spark ETL
FROM apache/spark-py:latest

WORKDIR /app

# Install Delta Lake and SQL Server JDBC dependencies
RUN pip install delta-spark pymssql

COPY etl_sqlserver_to_spark.py .

CMD ["spark-submit", "etl_sqlserver_to_spark.py"]
