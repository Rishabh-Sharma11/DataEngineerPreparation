import logging
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Iceberg-Read-Example")

# Initialize SparkSession with Iceberg properties
spark = (
    SparkSession.builder.appName("Iceberg-Read-Example")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://iceberg-warehouse-gds/warehouse/")
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.defaultCatalog", "glue_catalog")
    .getOrCreate()
)

# ----------------------------------------------------------------------------------------
# 1. Read Iceberg Table
# ----------------------------------------------------------------------------------------
logger.info("Reading the latest data from the Iceberg table...")
try:
    # Read the latest snapshot of the table
    table_path = "glue_catalog.ecommerce.orders"
    df = spark.read.format("iceberg").load(table_path)
    logger.info("Showing data from the latest snapshot of the table:")
    df.show()
except Exception as e:
    logger.error(f"Error while reading the Iceberg table: {e}")

# ----------------------------------------------------------------------------------------
# 2. Query Snapshots Metadata Table
# ----------------------------------------------------------------------------------------
logger.info("Querying the snapshots metadata table...")
try:
    # Query the snapshots metadata table
    snapshots_df = spark.sql(f"SELECT * FROM glue_catalog.ecommerce.orders.snapshots")
    logger.info("Snapshot details:")
    snapshots_df.show(truncate=False)
except Exception as e:
    logger.error(f"Error while querying snapshots metadata table: {e}")

# ----------------------------------------------------------------------------------------
# 3. Perform Time Travel Query Based on Snapshot ID
# ----------------------------------------------------------------------------------------
try:
    logger.info("Performing time travel query based on a snapshot ID...")
    snapshots = snapshots_df.collect()
    if snapshots:
        # Pick the first snapshot ID as an example
        snapshot_id = snapshots[0]["snapshot_id"]
        logger.info(f"Using snapshot ID: {snapshot_id}")

        # Perform the time travel query
        time_travel_df = spark.read.format("iceberg").option("snapshot-id", snapshot_id).load(table_path)
        logger.info(f"Data from snapshot ID: {snapshot_id}")
        time_travel_df.show()
    else:
        logger.info("No snapshots available for time travel query.")
except Exception as e:
    logger.error(f"Error while performing time travel query based on snapshot ID: {e}")

# ----------------------------------------------------------------------------------------
# 4. Perform Time Travel Query Based on Timestamp
# ----------------------------------------------------------------------------------------
try:
    logger.info("Performing time travel query based on a timestamp...")
    if snapshots:
        # Extract the first snapshot's committed_at timestamp (already a datetime object)
        raw_timestamp = snapshots[0]["committed_at"]
        logger.info(f"Raw timestamp from metadata: {raw_timestamp}")

        # Convert the timestamp to a Unix timestamp in milliseconds
        unix_timestamp_ms = int(raw_timestamp.timestamp() * 1000)  # Convert to milliseconds
        logger.info(f"Using Unix timestamp in milliseconds: {unix_timestamp_ms}")

        # Perform the time travel query
        time_travel_df = spark.read.format("iceberg").option("as-of-timestamp", unix_timestamp_ms).load(table_path)
        logger.info(f"Data from Unix timestamp (ms): {unix_timestamp_ms}")
        time_travel_df.show()
    else:
        logger.info("No snapshots available for time travel query.")
except Exception as e:
    logger.error(f"Error while performing time travel query based on timestamp: {e}")

# Stop Spark session
spark.stop()
logger.info("Iceberg read operations completed successfully.")