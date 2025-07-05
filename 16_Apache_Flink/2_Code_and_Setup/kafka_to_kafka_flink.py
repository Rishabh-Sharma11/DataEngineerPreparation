import logging
from pyflink.common import Types, Row
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaProducer, FlinkKafkaConsumer
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema
from pyflink.datastream.checkpoint_config import CheckpointStorage
from pyflink.datastream.checkpoint_config import ExternalizedCheckpointCleanup

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FlinkKafkaJob")


def run():
    # Initialize the Flink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # Adjust based on your system resources
    env.get_checkpoint_config().enable_externalized_checkpoints(
        ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
    )

    # Enable exactly-once checkpointing
    env.enable_checkpointing(10000)  # Checkpoint every 10 seconds
    checkpoint_storage = CheckpointStorage("file:///Users/shashankmishra/Desktop/flink_checkpointing")
    env.get_checkpoint_config().set_checkpoint_storage(checkpoint_storage)

    # Kafka Source Configuration
    source_type_info = Types.ROW_NAMED(
        ["order_id", "order_status", "total_price"],
        [Types.INT(), Types.STRING(), Types.FLOAT()]
    )

    deserialization_schema = JsonRowDeserializationSchema.builder() \
        .type_info(source_type_info) \
        .build()

    kafka_consumer = FlinkKafkaConsumer(
        topics="orders_src_flink",
        deserialization_schema=deserialization_schema,
        properties={
            "bootstrap.servers": "pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092",
            "group.id": "group-v1",
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.jaas.config": "org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='api_key' password='secret_key';"
        }
    )
    kafka_consumer.set_start_from_earliest()

    # Kafka Sink Configuration
    sink_type_info = Types.ROW_NAMED(
        ["order_id", "order_status", "total_price"],
        [Types.INT(), Types.STRING(), Types.FLOAT()]
    )

    serialization_schema = JsonRowSerializationSchema.builder() \
        .with_type_info(sink_type_info) \
        .build()

    kafka_producer = FlinkKafkaProducer(
        topic="orders_tgt_flink",
        serialization_schema=serialization_schema,
        producer_config={
            "bootstrap.servers": "pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092",
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.jaas.config": "org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='api_key' password='secret_key';"
        }
    )

    # Define data pipeline
    input_stream = env.add_source(kafka_consumer)

    # Log consumed messages
    def log_consumed_message(msg):
        logger.info(f"Consumed message: {msg}")
        return msg

    logged_stream = input_stream.map(log_consumed_message, output_type=source_type_info)

    # Add data validation
    def validate_message(msg):
        if msg is None:
            logger.error("Received None message")
            return None
        if any(v is None for v in [msg.order_id, msg.order_status, msg.total_price]):
            logger.error(f"Message contains None values: {msg}")
            return None
        return msg

    validated_stream = logged_stream.filter(lambda x: x is not None).map(validate_message, output_type=source_type_info)

    # Filter condition: Process only orders where total_price > 100
    filtered_stream = validated_stream.filter(lambda msg: msg.total_price > 100)

    # Log and publish the filtered messages
    def log_and_publish(msg):
        logger.info(f"Publishing message: {msg}")
        return Row(
            order_id=msg.order_id,
            order_status=msg.order_status,
            total_price=msg.total_price  # Already a float
        )

    processed_stream = filtered_stream.map(log_and_publish, output_type=sink_type_info)

    # Add sink to Kafka producer
    processed_stream.add_sink(kafka_producer)

    # Execute the Flink job
    env.execute("Flink Kafka Streaming Job")


if __name__ == "__main__":
    run()