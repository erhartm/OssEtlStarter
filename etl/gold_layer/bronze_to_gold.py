"""
Bronze to Gold: Create a gold-layer view by joining bronze tables directly (no mapping table).
This script creates a Spark SQL view, not a physical table.
"""
from pyspark.sql import SparkSession
import os

SQLSERVER_BRONZE_TABLE = os.getenv('SQLSERVER_BRONZE_TABLE', 'etl_sqlserver_table')
MONGODB_BRONZE_TABLE = os.getenv('MONGODB_BRONZE_TABLE', 'etl_mongo_table')
GOLD_VIEW = os.getenv('GOLD_VIEW', 'gold_bronze_joined_view')

spark = SparkSession.builder.getOrCreate()

sqlserver_df = spark.table(SQLSERVER_BRONZE_TABLE)
mongodb_df = spark.table(MONGODB_BRONZE_TABLE)

# Example: join on 'id' (customize as needed)
gold_df = sqlserver_df.join(mongodb_df, sqlserver_df.id == mongodb_df.id, 'inner')


# Note: This script creates a Spark SQL view, not a Delta table.
# Delta Lake merge (upsert) is not applicable for views.
gold_df.createOrReplaceTempView(GOLD_VIEW)

print(f"Gold view '{GOLD_VIEW}' created. Query it with: SELECT * FROM {GOLD_VIEW}")
