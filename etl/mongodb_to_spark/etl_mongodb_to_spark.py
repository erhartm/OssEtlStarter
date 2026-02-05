"""
ETL pipeline: Extract from MongoDB, load to Databricks Apache Spark (Delta Lake)
Pure PySpark, OSS connectors only, Databricks-compatible
"""
from pyspark.sql import SparkSession
import os

# MongoDB connection parameters (set via env vars or config)
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
MONGO_DB = os.getenv('MONGO_DB', 'testdb')
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION', 'testcoll')
# Optional: MongoDB aggregation pipeline for filtering (as JSON string)
MONGO_PIPELINE = os.getenv('MONGO_PIPELINE', None)

# Output Delta path (Databricks-accessible, e.g., DBFS, S3, or ADLS)
DELTA_PATH = os.getenv('DELTA_PATH', '/mnt/etl_output/mongo_data_delta')
DELTA_TABLE = os.getenv('DELTA_TABLE', 'etl_mongo_table')

# Spark session with MongoDB, Delta Lake, and persistent Hive metastore support
# To make table registration persistent and visible in Databricks UI, ensure your cluster is configured with a Hive metastore.
spark = SparkSession.builder \
    .appName("MongoDBToSparkETL") \
    .config("spark.mongodb.read.connection.uri", MONGO_URI) \
    .config("spark.mongodb.read.database", MONGO_DB) \
    .config("spark.mongodb.read.collection", MONGO_COLLECTION) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

# Extract from MongoDB with optional pipeline filtering
read_options = {}
if MONGO_PIPELINE:
    read_options["pipeline"] = MONGO_PIPELINE
mongo_df = spark.read.format("mongodb").options(**read_options).load()

# Example transformation (identity)
transformed_df = mongo_df # .select(...)

# Write to Delta Lake format
transformed_df.write.format("delta").mode("overwrite").save(DELTA_PATH)

# Register Delta table using Spark SQL (persistent in Hive metastore)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DELTA_TABLE}
    USING DELTA
    LOCATION '{DELTA_PATH}'
""")

spark.stop()