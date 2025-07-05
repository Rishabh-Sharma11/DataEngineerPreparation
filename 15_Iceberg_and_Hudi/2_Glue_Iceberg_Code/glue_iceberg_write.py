import logging
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Glue-Iceberg-Write-Example")

# Initialize SparkSession with Iceberg properties
spark = (
    SparkSession.builder.appName("Glue-Iceberg-Write-Example")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://iceberg-warehouse-gds/warehouse/")
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.defaultCatalog", "glue_catalog")
    .getOrCreate()
)

# ----------------------------------------------------------------------------------------
# 1. Define Dataset and Schema
# ----------------------------------------------------------------------------------------
orders_data = [
    (101, "2025-01-01", "Electronics", 599.99, "Shipped", "USA"),
    (102, "2025-01-02", "Books", 29.99, "Delivered", "Canada"),
    (103, "2025-01-03", "Fashion", 79.99, "Pending", "UK"),
]
orders_schema = ["order_id", "order_date", "category", "total_price", "order_status", "country"]

orders_df = spark.createDataFrame(orders_data, schema=orders_schema)

# Ensure the database exists

database_name = "glue_catalog.ecommerce"
logger.info(f"Ensuring database '{database_name}' exists...")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
logger.info(f"Database '{database_name}' is ready.")

# ----------------------------------------------------------------------------------------
# 2. Create or Replace Table with Partitioning and Table Properties
# ----------------------------------------------------------------------------------------
logger.info("Creating or Replacing Iceberg Table with Table Properties and Partitioning...")

orders_df.writeTo("glue_catalog.ecommerce.orders") \
    .partitionedBy("country", "category") \
    .tableProperty("write.format.default", "parquet") \
    .tableProperty("write.target-file-size-bytes", "536870912") \
    .createOrReplace()

# ----------------------------------------------------------------------------------------
# 3. Append New Data (Without Schema Evolution)
# ----------------------------------------------------------------------------------------
append_without_evolution_data = [
    (104, "2025-01-04", "Fashion", 99.99, "Delivered", "USA"),
    (105, "2025-01-05", "Home", 399.99, "Shipped", "India"),
]
append_without_evolution_df = spark.createDataFrame(append_without_evolution_data, schema=orders_schema)

logger.info("Appending New Data without Schema Evolution...")
append_without_evolution_df.writeTo("glue_catalog.ecommerce.orders") \
    .append()

# ----------------------------------------------------------------------------------------
# 4. Dynamic Insert Overwrite (Partition Overwrite)
# ----------------------------------------------------------------------------------------
overwrite_orders_data = [
    (106, "2025-01-06", "Books", 19.99, "Returned", "Canada"),
    (107, "2025-01-07", "Electronics", 999.99, "Delivered", "USA"),
]
overwrite_orders_df = spark.createDataFrame(overwrite_orders_data, schema=orders_schema)

logger.info("Performing Dynamic Insert Overwrite...")
overwrite_orders_df.writeTo("glue_catalog.ecommerce.orders") \
    .overwritePartitions()

# # ----------------------------------------------------------------------------------------
# # 5. Upsert (MERGE Operation)
# # ----------------------------------------------------------------------------------------
merge_orders_data = [
    (105, "2025-01-08", "Home", 129.99, "Shipped", "India"),
    (109, "2025-01-06", "Fashion", 21.99, "Refunded", "Canada"),
]
merge_orders_df = spark.createDataFrame(merge_orders_data, schema=orders_schema)

logger.info("Performing Upsert with MERGE...")
merge_orders_df.createOrReplaceTempView("merge_source")

spark.sql("""
MERGE INTO glue_catalog.ecommerce.orders tgt
USING merge_source src
ON tgt.order_id = src.order_id
WHEN MATCHED THEN 
  UPDATE SET 
    tgt.order_date = src.order_date,
    tgt.category = src.category,
    tgt.total_price = src.total_price,
    tgt.order_status = src.order_status,
    tgt.country = src.country
WHEN NOT MATCHED THEN 
  INSERT (order_id, order_date, category, total_price, order_status, country)
  VALUES (src.order_id, src.order_date, src.category, src.total_price, src.order_status, src.country)
""")

# Stop Spark session
spark.stop()