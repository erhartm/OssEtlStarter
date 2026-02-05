"""
ETL pipeline: Extract from SQL Server, load to Databricks Apache Spark (Delta Lake)
Pure PySpark, OSS connectors only, Databricks-compatible
"""
from pyspark.sql import SparkSession
import os

# SQL Server connection parameters (set via env vars or config)
SQLSERVER_HOST = os.getenv('SQLSERVER_HOST', 'localhost')
SQLSERVER_PORT = os.getenv('SQLSERVER_PORT', '1433')
SQLSERVER_DB = os.getenv('SQLSERVER_DB', 'testdb')
SQLSERVER_USER = os.getenv('SQLSERVER_USER', 'sa')
SQLSERVER_PASSWORD = os.getenv('SQLSERVER_PASSWORD', 'yourStrong(!)Password')
# You can provide a table name (e.g., 'dbo.testtable') or a SQL subquery (e.g., '(SELECT col1, col2 FROM dbo.testtable WHERE ...) AS subq')
SQLSERVER_QUERY = os.getenv('SQLSERVER_QUERY', 'dbo.testtable')

# ---
# Partitioning for large tables:
# To use partitioned reads, you should determine the minimum and maximum values of your partition column (e.g., id) in advance.
# Example SQL to get bounds:
#   SELECT MIN(id) AS min_id, MAX(id) AS max_id FROM your_table;
# Use these as SQLSERVER_LOWER_BOUND and SQLSERVER_UPPER_BOUND.
# ---

# Output Delta path (Databricks-accessible, e.g., DBFS, S3, or ADLS)
DELTA_PATH = os.getenv('DELTA_PATH', '/mnt/etl_output/sqlserver_data_delta')
DELTA_TABLE = os.getenv('DELTA_TABLE', 'etl_sqlserver_table')

JDBC_URL = f"jdbc:sqlserver://{SQLSERVER_HOST}:{SQLSERVER_PORT};databaseName={SQLSERVER_DB}"

# Spark session with Delta Lake and persistent Hive metastore support
# To make table registration persistent and visible in Databricks UI, ensure your cluster is configured with a Hive metastore.
spark = SparkSession.builder \
	.appName("SQLServerToSparkETL") \
	.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
	.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
	.config("spark.sql.catalogImplementation", "hive") \
	.enableHiveSupport() \
	.getOrCreate()

# Partitioning options for large tables (optional, set via env vars)
PARTITION_COLUMN = os.getenv('SQLSERVER_PARTITION_COLUMN')  # e.g., 'id'
LOWER_BOUND = os.getenv('SQLSERVER_LOWER_BOUND')  # e.g., '1'
UPPER_BOUND = os.getenv('SQLSERVER_UPPER_BOUND')  # e.g., '1000000'
NUM_PARTITIONS = os.getenv('SQLSERVER_NUM_PARTITIONS')  # e.g., '8'

read_options = {
	"url": JDBC_URL,
	"dbtable": SQLSERVER_QUERY,
	"user": SQLSERVER_USER,
	"password": SQLSERVER_PASSWORD,
	"driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}
if PARTITION_COLUMN and LOWER_BOUND and UPPER_BOUND and NUM_PARTITIONS:
	read_options["partitionColumn"] = PARTITION_COLUMN
	read_options["lowerBound"] = LOWER_BOUND
	read_options["upperBound"] = UPPER_BOUND
	read_options["numPartitions"] = NUM_PARTITIONS

sqlserver_df = spark.read.format("jdbc").options(**read_options).load()

# Example transformation (identity)
transformed_df = sqlserver_df # .select(...)

# Write to Delta Lake format
transformed_df.write.format("delta").mode("overwrite").save(DELTA_PATH)

# Register Delta table using Spark SQL (persistent in Hive metastore)
spark.sql(f"""
	CREATE TABLE IF NOT EXISTS {DELTA_TABLE}
	USING DELTA
	LOCATION '{DELTA_PATH}'
""")
