# Bronze to Silver ETL script
# ...existing code will be copied here...
"""
ETL pipeline: Transform bronze Delta table to silver/gold Delta table in Databricks
Pure PySpark, OSS-compatible, persistent table registration
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType # Add more types as needed
import os


# Input bronze table names and output mapping table name/location
SQLSERVER_BRONZE_TABLE = os.getenv('SQLSERVER_BRONZE_TABLE', 'etl_sqlserver_table')
MONGODB_BRONZE_TABLE = os.getenv('MONGODB_BRONZE_TABLE', 'etl_mongo_table')

# --- Unity Catalog Compatibility ---
# Optionally specify catalog and schema for Unity Catalog, fallback to OSS-compatible table name
MAPPING_CATALOG = os.getenv('MAPPING_CATALOG')  # e.g., 'main' or 'my_catalog'
MAPPING_SCHEMA = os.getenv('MAPPING_SCHEMA')    # e.g., 'default' or 'my_schema'
MAPPING_TABLE_NAME = os.getenv('MAPPING_TABLE', 'silver_mapping_table')
if MAPPING_CATALOG and MAPPING_SCHEMA:
	MAPPING_TABLE = f"{MAPPING_CATALOG}.{MAPPING_SCHEMA}.{MAPPING_TABLE_NAME}"
elif MAPPING_SCHEMA:
	MAPPING_TABLE = f"{MAPPING_SCHEMA}.{MAPPING_TABLE_NAME}"
else:
	MAPPING_TABLE = MAPPING_TABLE_NAME
MAPPING_PATH = os.getenv('MAPPING_PATH', '/mnt/etl_output/silver_mapping_data_delta')

# Spark session with Delta Lake and Hive metastore support
spark = SparkSession.builder \
	.appName("BronzeToSilverETL") \
	.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
	.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
	.config("spark.sql.catalogImplementation", "hive") \
	.enableHiveSupport() \
	.getOrCreate()


# Read from both bronze tables (registered in metastore)

# --- Schema Enforcement Boilerplate ---
# Define your schema here. Replace field names and types as needed.
example_schema = StructType([
	StructField("sqlserver_id", IntegerType(), True),
	StructField("mongodb_id", StringType(), True)
	# Add more fields as needed
])

# Apply schema when reading from bronze tables (if needed)
sqlserver_df = spark.table(SQLSERVER_BRONZE_TABLE)
mongodb_df = spark.table(MONGODB_BRONZE_TABLE)
# Optionally, enforce schema after join:
# mapping_df = spark.createDataFrame(mapping_df.rdd, example_schema)



# Example: one-to-one mapping join (customize join keys as needed)
# Assume both tables have a common key 'id' for mapping
mapping_df = sqlserver_df.join(mongodb_df, sqlserver_df.id == mongodb_df.id, 'inner')

# Select only the mapping IDs (customize column names as needed)
mapping_df = mapping_df.select(sqlserver_df.id.alias("sqlserver_id"), mongodb_df.id.alias("mongodb_id"))

# Optionally, enforce schema on mapping_df:
# mapping_df = spark.createDataFrame(mapping_df.rdd, example_schema)

# Note: If your mapping table schema changes, update 'example_schema' above to match.


# --- Delta Lake Merge (Upsert) Example ---
# Replace 'sqlserver_id' with your actual primary key column name
from delta.tables import DeltaTable

# Register Delta table if not exists (works for Unity Catalog and OSS)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {MAPPING_TABLE}
    USING DELTA
    LOCATION '{MAPPING_PATH}'
""")

if DeltaTable.isDeltaTable(spark, MAPPING_PATH):
	delta_table = DeltaTable.forPath(spark, MAPPING_PATH)
	# Perform merge (upsert) into Delta table
	# Replace 'sqlserver_id' with your actual key column
	(
		delta_table.alias("target")
		.merge(
			mapping_df.alias("source"),
			"target.sqlserver_id = source.sqlserver_id"  # TODO: Replace with your key column
		)
		.whenMatchedUpdateAll()
		.whenNotMatchedInsertAll()
		.option("mergeSchema", "true")
		.execute()
	)
else:
	# First time: write full data
	mapping_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(MAPPING_PATH)

	# Register Delta table in metastore
	spark.sql(f"""
		CREATE TABLE IF NOT EXISTS {MAPPING_TABLE}
		USING DELTA
		LOCATION '{MAPPING_PATH}'
	""")

print("Delta Lake merge (upsert) completed. Replace 'sqlserver_id' with your actual key column.")

spark.stop()

spark.stop()
