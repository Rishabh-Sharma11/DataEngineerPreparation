from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
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

spark = SparkSession.builder \
    .appName("StatelessStreamProcessing") \
    .master("yarn") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Define your schema
schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("age", IntegerType())
])

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092") \
    .option("subscribe", "user_data_topic") \
    .option("startingOffsets", "latest") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", 
            f"org.apache.kafka.common.security.plain.PlainLoginModule required username='U4ZHZEMXRQQEL62M' password='9fBSku8MpYbWKI46BEqBxbR4h1IWgauihDZnHmma5HxdLCG0CbIXMbt0njnqcIoC';") \
    .load()

logger.info("Read Stream Started.........")

# Convert the data and filter
data = df.selectExpr("CAST(value AS STRING)") \
  .select(from_json(col("value").cast("string"), schema).alias("data")) \
  .select("data.*") \
  .filter(col("age") > 25)

logger.info("Dataframe filter applied .........")

checkpoint_dir = "gs://streaming_checkpointing/stateless_processing"

# Start streaming and print to console
query = data \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .option("checkpointLocation", checkpoint_dir) \
    .trigger(processingTime="10 seconds") \
    .start()

logger.info("Write stream started  .........")

query.awaitTermination()