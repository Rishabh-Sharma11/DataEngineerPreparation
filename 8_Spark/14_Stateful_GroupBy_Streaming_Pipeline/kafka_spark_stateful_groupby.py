from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import logging

# Set up logging to console only
logging.basicConfig(
    level=logging.INFO,  # Set log level
    format="%(asctime)s - %(levelname)s - %(message)s",  # Log format
    handlers=[
        logging.StreamHandler()  # Log to the console only
    ]
)

logger = logging.getLogger(__name__)

# Create a SparkSession
spark = SparkSession.builder \
    .appName("GroupByStreaming") \
    .master("yarn") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Read the stream from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092") \
    .option("subscribe", "trx_topic_data") \
    .option("startingOffsets", "latest") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", 
            f"org.apache.kafka.common.security.plain.PlainLoginModule required username='U4ZHZEMXRQQEL62M' password='9fBSku8MpYbWKI46BEqBxbR4h1IWgauihDZnHmma5HxdLCG0CbIXMbt0njnqcIoC';") \
    .load()

logger.info("Read Stream Started.........")

# Define the schema of the JSON data
schema = StructType([
    StructField("user_id", StringType()),
    StructField("amount", IntegerType()),
    StructField("timestamp", TimestampType())
])

# Parse the JSON data and select the fields
df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Perform the aggregation
df = df.groupBy("user_id").agg(sum("amount").alias("total_amount"))

logger.info("Dataframe group by applied .........")

checkpoint_dir = "gs://streaming_checkpointing/stateful_group_by"

# Start streaming and print to console
query = df \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("checkpointLocation", checkpoint_dir) \
    .start()

logger.info("Write stream started  .........")

# Wait for the query to terminate
query.awaitTermination()