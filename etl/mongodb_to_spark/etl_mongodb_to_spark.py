"""
ETL pipeline: Extract from MongoDB, load to Databricks Apache Spark (Delta Lake)
Pure PySpark, OSS connectors only, Databricks-compatible
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType # Add more types as needed
import os

# MongoDB connection parameters (set via env vars or config)
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
MONGO_DB = os.getenv('MONGO_DB', 'testdb')
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION', 'testcoll')
# Optional: MongoDB aggregation pipeline for filtering (as JSON string)
MONGO_PIPELINE = os.getenv('MONGO_PIPELINE', None)

# Output Delta path (Databricks-accessible, e.g., DBFS, S3, or ADLS)
DELTA_PATH = os.getenv('DELTA_PATH', '/mnt/etl_output/mongo_data_delta')

# --- Unity Catalog Compatibility ---
# Optionally specify catalog and schema for Unity Catalog, fallback to OSS-compatible table name
DELTA_CATALOG = os.getenv('DELTA_CATALOG')  # e.g., 'main' or 'my_catalog'
DELTA_SCHEMA = os.getenv('DELTA_SCHEMA')    # e.g., 'default' or 'my_schema'
DELTA_TABLE_NAME = os.getenv('DELTA_TABLE', 'etl_mongo_table')
if DELTA_CATALOG and DELTA_SCHEMA:
    DELTA_TABLE = f"{DELTA_CATALOG}.{DELTA_SCHEMA}.{DELTA_TABLE_NAME}"
elif DELTA_SCHEMA:
    DELTA_TABLE = f"{DELTA_SCHEMA}.{DELTA_TABLE_NAME}"
else:
    DELTA_TABLE = DELTA_TABLE_NAME

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

# --- Schema Enforcement Boilerplate ---
# Define your schema here. Replace field names and types as needed.
example_schema = StructType([
    StructField("_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("value", DoubleType(), True)
    # Add more fields as needed
])

# Apply schema when reading from MongoDB
mongo_df = spark.read.format("mongodb").options(**read_options).schema(example_schema).load()


# Example transformation (identity)
transformed_df = mongo_df # .select(...)

# Note: If your source collection schema changes, update 'example_schema' above to match.

# Write to Delta Lake format

# --- Delta Lake Merge (Upsert) Example ---
# Replace '_id' with your actual primary key column name
from delta.tables import DeltaTable

# Register Delta table if not exists (works for Unity Catalog and OSS)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DELTA_TABLE}
    USING DELTA
    LOCATION '{DELTA_PATH}'
""")

if DeltaTable.isDeltaTable(spark, DELTA_PATH):
    delta_table = DeltaTable.forPath(spark, DELTA_PATH)
    # Perform merge (upsert) into Delta table
    # Replace '_id' with your actual key column
    (
        delta_table.alias("target")
        .merge(
            transformed_df.alias("source"),
            "target._id = source._id"  # TODO: Replace '_id' with your key column
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    # First time: write full data
    transformed_df.write.format("delta").mode("overwrite").save(DELTA_PATH)

    # Register Delta table in metastore
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DELTA_TABLE}
        USING DELTA
        LOCATION '{DELTA_PATH}'
    """)

print("Delta Lake merge (upsert) completed. Replace '_id' with your actual key column.")

spark.stop()