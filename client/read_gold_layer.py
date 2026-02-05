"""
Python client to read and display data from a gold-layer Spark SQL view.
"""
from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

SPARK_MASTER = os.getenv('SPARK_MASTER', 'local[*]')
GOLD_VIEW = os.getenv('GOLD_VIEW', 'gold_bronze_joined_view')

# Databricks Connect support (for remote execution)
DATABRICKS_HOST = os.getenv('DATABRICKS_HOST')
DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN')
DATABRICKS_CLUSTER_ID = os.getenv('DATABRICKS_CLUSTER_ID')

if DATABRICKS_HOST and DATABRICKS_TOKEN and DATABRICKS_CLUSTER_ID:
    # Databricks Connect configuration
    spark = SparkSession.builder \
        .appName("GoldLayerClient") \
        .master(f"databricks://{DATABRICKS_CLUSTER_ID}") \
        .config("spark.databricks.service.address", DATABRICKS_HOST) \
        .config("spark.databricks.service.token", DATABRICKS_TOKEN) \
        .getOrCreate()
else:
    # Default Spark (OSS/local/cluster)
    spark = SparkSession.builder \
        .master(SPARK_MASTER) \
        .appName("GoldLayerClient") \
        .getOrCreate()

# Read from the gold-layer view
try:
    df = spark.sql(f"SELECT * FROM {GOLD_VIEW}")
    print(f"Showing data from view: {GOLD_VIEW}")
    df.show(truncate=False)
except Exception as e:
    print(f"Error reading from view '{GOLD_VIEW}': {e}")
finally:
    spark.stop()
