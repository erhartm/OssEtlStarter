"""
Bronze to Gold: Create a gold-layer view by joining bronze tables directly (no mapping table).
This script creates a Spark SQL view, not a physical table.
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType # Add more types as needed
import os

SQLSERVER_BRONZE_TABLE = os.getenv('SQLSERVER_BRONZE_TABLE', 'etl_sqlserver_table')
MONGODB_BRONZE_TABLE = os.getenv('MONGODB_BRONZE_TABLE', 'etl_mongo_table')
GOLD_VIEW = os.getenv('GOLD_VIEW', 'gold_bronze_joined_view')

spark = SparkSession.builder.getOrCreate()


# --- Schema Enforcement Boilerplate ---
# Define your schema here. Replace field names and types as needed.
example_schema = StructType([
	StructField("id", IntegerType(), True),
	StructField("name", StringType(), True),
	StructField("value", DoubleType(), True)
	# Add more fields as needed
])

sqlserver_df = spark.table(SQLSERVER_BRONZE_TABLE)
mongodb_df = spark.table(MONGODB_BRONZE_TABLE)
# Optionally, enforce schema after join:
# gold_df = spark.createDataFrame(gold_df.rdd, example_schema)


# Example: join on 'id' (customize as needed)
gold_df = sqlserver_df.join(mongodb_df, sqlserver_df.id == mongodb_df.id, 'inner')

# Optionally, enforce schema on gold_df:
# gold_df = spark.createDataFrame(gold_df.rdd, example_schema)

# Note: If your gold view schema changes, update 'example_schema' above to match.


# Note: This script creates a Spark SQL view, not a Delta table.
# Delta Lake merge (upsert) is not applicable for views.
gold_df.createOrReplaceTempView(GOLD_VIEW)

print(f"Gold view '{GOLD_VIEW}' created. Query it with: SELECT * FROM {GOLD_VIEW}")
