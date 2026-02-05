# Bronze to Silver ETL script
# ...existing code will be copied here...
"""
ETL pipeline: Transform bronze Delta table to silver/gold Delta table in Databricks
Pure PySpark, OSS-compatible, persistent table registration
"""
from pyspark.sql import SparkSession
import os


# Input bronze table names and output mapping table name/location
SQLSERVER_BRONZE_TABLE = os.getenv('SQLSERVER_BRONZE_TABLE', 'etl_sqlserver_table')
MONGODB_BRONZE_TABLE = os.getenv('MONGODB_BRONZE_TABLE', 'etl_mongo_table')
MAPPING_TABLE = os.getenv('MAPPING_TABLE', 'silver_mapping_table')
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
sqlserver_df = spark.table(SQLSERVER_BRONZE_TABLE)
mongodb_df = spark.table(MONGODB_BRONZE_TABLE)


# Example: one-to-one mapping join (customize join keys as needed)
# Assume both tables have a common key 'id' for mapping
mapping_df = sqlserver_df.join(mongodb_df, sqlserver_df.id == mongodb_df.id, 'inner')

# Select only the mapping IDs (customize column names as needed)
mapping_df = mapping_df.select(sqlserver_df.id.alias("sqlserver_id"), mongodb_df.id.alias("mongodb_id"))

# Write mapping table to Delta Lake (silver layer)
mapping_df.write.format("delta").mode("overwrite").save(MAPPING_PATH)

# Register mapping table in metastore (persistent)
spark.sql(f"""
	CREATE TABLE IF NOT EXISTS {MAPPING_TABLE}
	USING DELTA
	LOCATION '{MAPPING_PATH}'
""")

spark.stop()
