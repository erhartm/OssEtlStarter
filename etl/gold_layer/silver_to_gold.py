"""
Silver to Gold: Create a gold-layer view by joining the mapping table (silver) with the bronze tables.
This script creates a Spark SQL view, not a physical table.
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType # Add more types as needed
import os

SQLSERVER_BRONZE_TABLE = os.getenv('SQLSERVER_BRONZE_TABLE', 'etl_sqlserver_table')
MONGODB_BRONZE_TABLE = os.getenv('MONGODB_BRONZE_TABLE', 'etl_mongo_table')
MAPPING_TABLE = os.getenv('MAPPING_TABLE', 'silver_mapping_table')
GOLD_VIEW = os.getenv('GOLD_VIEW', 'gold_silver_joined_view')

spark = SparkSession.builder.getOrCreate()


# --- Schema Enforcement Boilerplate ---
# Define your schema here. Replace field names and types as needed.
example_schema = StructType([
    StructField("sqlserver_id", IntegerType(), True),
    StructField("mongodb_id", StringType(), True)
    # Add more fields as needed
])

sqlserver_df = spark.table(SQLSERVER_BRONZE_TABLE)
mongodb_df = spark.table(MONGODB_BRONZE_TABLE)
mapping_df = spark.table(MAPPING_TABLE)
# Optionally, enforce schema after join:
# gold_df = spark.createDataFrame(gold_df.rdd, example_schema)


# Join mapping table to both sources (customize join keys as needed)
gold_df = mapping_df \
    .join(sqlserver_df, mapping_df.sqlserver_id == sqlserver_df.id, 'inner') \
    .join(mongodb_df, mapping_df.mongodb_id == mongodb_df.id, 'inner')

# Optionally, enforce schema on gold_df:
# gold_df = spark.createDataFrame(gold_df.rdd, example_schema)

# Note: If your gold view schema changes, update 'example_schema' above to match.


# Note: This script creates a Spark SQL view, not a Delta table.
# Delta Lake merge (upsert) is not applicable for views.
gold_df.createOrReplaceTempView(GOLD_VIEW)

print(f"Gold view '{GOLD_VIEW}' created. Query it with: SELECT * FROM {GOLD_VIEW}")
