## Delta Lake Schema Evolution
All ETL template scripts now support schema evolution using Delta Lake OSS. When you add new columns to your schema in the ETL script, the table schema in the metastore will automatically update to match, thanks to the `mergeSchema` option in write and merge operations.

- **How it works:**
   - When writing or merging data, the script uses `.option("mergeSchema", "true")` to allow new columns to be added to the Delta table.
   - The metastore schema will evolve to match your DataFrame’s schema for supported changes (e.g., adding columns).
- **Customization required:**
   - Update your ETL script’s schema definition as your source evolves.
   - For complex schema changes (e.g., dropping columns, changing types), additional Spark SQL or table management may be needed.

See comments in each ETL script for details and instructions.
## Schema Enforcement Boilerplate
All ETL template scripts now include a schema enforcement example using PySpark's `StructType`. This allows you to define the expected schema for your source data and ensure that only data matching your structure is loaded.

- **How it works:**
   - Each script provides a placeholder schema definition (see `example_schema`).
   - Update the field names and types to match your actual source table or collection.
   - Apply the schema when reading or transforming data.

See comments in each ETL script for details and instructions.
# ETL Pipeline Boilerplate: MongoDB/SQL Server to Apache Spark (Databricks)

## Overview
This project provides a pure PySpark, open-source ETL template for extracting data from MongoDB or SQL Server and loading it into Databricks Apache Spark using Delta Lake format. Scheduling is handled via Kubernetes CronJobs. All table management is done using open-source Spark SQL, not Databricks-proprietary APIs.

## Project Structure
- `etl/sqlserver_to_spark/etl_sqlserver_to_spark.py` & `Dockerfile`: Extract from SQL Server to Delta Lake
- `etl/mongodb_to_spark/etl_mongodb_to_spark.py` & `Dockerfile`: Extract from MongoDB to Delta Lake
- `etl/bronze_to_silver/etl_bronze_to_silver.py` & `Dockerfile`: Join/Map bronze tables to create a mapping table (silver layer)
- `etl/gold_layer/bronze_to_gold.py`: Create a gold-layer view by joining bronze tables directly (no mapping table)
- `etl/gold_layer/silver_to_gold.py`: Create a gold-layer view by joining the mapping (silver) table with both bronze tables
- `client/read_gold_layer.py`: Python client to read and display data from a gold-layer view
- `client/.env`: Environment variables for the client (Spark/Databricks connection, view name)
- `k8s_cronjobs.yaml`: Example Kubernetes CronJob definitions
- `requirements.txt`: Python dependencies

## Environment Variables for Catalog/Schema Compatibility
To support both Unity Catalog and OSS Delta Lake, ETL scripts accept optional environment variables:
- `DELTA_CATALOG`, `DELTA_SCHEMA`, `DELTA_TABLE` (SQL Server/MongoDB ETL)
- `MAPPING_CATALOG`, `MAPPING_SCHEMA`, `MAPPING_TABLE` (Bronze to Silver ETL)
If `DELTA_CATALOG` and `DELTA_SCHEMA` are set, tables are registered as `catalog.schema.table` (Unity Catalog). If only `DELTA_SCHEMA` is set, tables are registered as `schema.table` (OSS Hive metastore or Databricks legacy). If neither is set, tables are registered as `table`.
This ensures maximum compatibility across Databricks Unity Catalog and open-source Spark/Delta Lake.

## Orchestration and Job Triggering
Each ETL job (bronze, silver, gold) runs independently and does not automatically trigger downstream jobs. You must orchestrate execution using Kubernetes CronJobs, Databricks Jobs, Airflow, Prefect, or another workflow manager if you want automatic dependencies or chaining.

## Prerequisites
- Python 3.8+
- PySpark
- delta-spark (open-source Delta Lake)
- MongoDB and/or SQL Server instance
- Access to Databricks or Spark cluster (with Delta Lake support)
- Kubernetes cluster
- Docker (for containerizing jobs)

## Setup
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Download the [MongoDB Spark Connector](https://www.mongodb.com/docs/spark-connector/current/installation/), [SQL Server JDBC Driver](https://docs.microsoft.com/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server), and ensure [delta-spark](https://delta.io/) is installed.
3. Build Docker images for each ETL script (see below).
4. Deploy to Kubernetes using `k8s_cronjobs.yaml`.

## Running Locally
Set environment variables as needed, then run:
```bash
python etl/etl_mongodb_to_spark.py
python etl/etl_sqlserver_to_spark.py
```
The scripts will write Delta Lake tables to your specified storage and register them using Spark SQL (OSS-compatible, works on Databricks and open-source Spark).

## Docker Example
Create a Dockerfile for each ETL job:
```dockerfile
FROM apache/spark-py
COPY etl_mongodb_to_spark.py /app/
WORKDIR /app
RUN pip install -r /app/requirements.txt
CMD ["spark-submit", "etl_mongodb_to_spark.py"]
```

## Kubernetes CronJob
Edit `k8s_cronjobs.yaml` with your image names and environment variables (including DELTA_PATH and DELTA_TABLE), then apply:
```bash
kubectl apply -f k8s_cronjobs.yaml
```


## Delta Lake Merge (Upsert) Boilerplate
The ETL template scripts for SQL Server, MongoDB, and bronze-to-silver now include a Delta Lake merge (upsert) example. This allows you to efficiently handle new and updated records when running ETL jobs periodically.

- **How it works:**
   - If the Delta table already exists, the script performs a merge (upsert) using a placeholder key column (e.g., `id`, `_id`, or `sqlserver_id`).
   - If the table does not exist, it writes the full data and registers the table.
- **Customization required:**
   - Replace the placeholder key in the merge condition (e.g., `target.id = source.id`) with your actual primary key column name(s).
   - Adjust the merge logic as needed for your schema.

See comments in each ETL script for details.

---
For Databricks, simply run these scripts as jobs or notebooks. All table management is done using open-source Spark SQL, so you can migrate to open-source Spark/Delta Lake in the future.
All connectors and table management are open source (no Databricks-proprietary APIs).

## Gold Layer Views
- Gold-layer scripts create Spark SQL views (not physical tables) for analytics, either by joining bronze tables directly or using the mapping table (silver layer).

## How to Run
- Use the Dockerfiles in each ETL job folder to build images for Kubernetes or local use.
- Set environment variables for table/view names and storage paths as needed.
- For gold-layer scripts, run with Spark and query the resulting view using Spark SQL.

## Example: Running a Gold Layer Script
```bash
spark-submit etl/gold_layer/bronze_to_gold.py
# Then in a Spark SQL session:
# SELECT * FROM gold_bronze_joined_view
```

## Client Application
- The client app in `client/read_gold_layer.py` connects to Spark or Databricks, reads from the specified gold-layer view, and displays the data.
- Configure the connection and view name in `client/.env` (see commented examples for Databricks Connect and Spark master).
- Example usage:
```bash
cd client
python read_gold_layer.py
```

---
MIT License
