"""
Transaction Analytics Job using PyFlink DataStream API
Processes transaction data from Kafka, detects high-value transactions,
and calculates category statistics.
"""

import json
import logging
from typing import Iterable

from pyflink.common import Types, WatermarkStrategy, Time
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.functions import MapFunction, FilterFunction, KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types as T


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Transaction:
    """Transaction data model"""
    def __init__(self, transaction_id: str, user_id: str, amount: float,
                 merchant: str, timestamp: int, category: str):
        self.transaction_id = transaction_id
        self.user_id = user_id
        self.amount = amount
        self.merchant = merchant
        self.timestamp = timestamp
        self.category = category

    def __str__(self):
        return (f"Transaction(id={self.transaction_id}, user={self.user_id}, "
                f"amount=${self.amount:.2f}, category={self.category})")

    @staticmethod
    def from_json(json_str: str):
        """Parse JSON string to Transaction object"""
        data = json.loads(json_str)
        return Transaction(
            transaction_id=data['transaction_id'],
            user_id=data['user_id'],
            amount=data['amount'],
            merchant=data['merchant'],
            timestamp=data['timestamp'],
            category=data['category']
        )


class JsonToTransactionMap(MapFunction):
    """Map JSON strings to Transaction objects"""
    def map(self, value: str):
        try:
            return Transaction.from_json(value)
        except Exception as e:
            logger.error(f"Error parsing transaction: {e}")
            return None


class HighValueFilter(FilterFunction):
    """Filter high-value transactions (potential fraud)"""
    def __init__(self, threshold: float = 1000.0):
        self.threshold = threshold

    def filter(self, transaction: Transaction) -> bool:
        return transaction is not None and transaction.amount > self.threshold


class TransactionTimestampAssigner(TimestampAssigner):
    """Extract timestamp from transaction"""
    def extract_timestamp(self, transaction: Transaction, record_timestamp: int) -> int:
        return transaction.timestamp


class CategoryStatsProcessor(KeyedProcessFunction):
    """
    Calculate statistics per category using keyed state.
    This processes transactions and maintains running statistics.
    """

    def __init__(self):
        self.count_state = None
        self.sum_state = None
        self.last_output_time = None
        self.output_interval_ms = 60000  # Output every 60 seconds

    def open(self, runtime_context):
        # Initialize state descriptors
        self.count_state = runtime_context.get_state(
            ValueStateDescriptor("count", T.LONG())
        )
        self.sum_state = runtime_context.get_state(
            ValueStateDescriptor("sum", T.DOUBLE())
        )
        self.last_output_time = runtime_context.get_state(
            ValueStateDescriptor("last_output", T.LONG())
        )

    def process_element(self, transaction: Transaction, ctx: 'KeyedProcessFunction.Context'):
        # Update state
        current_count = self.count_state.value() or 0
        current_sum = self.sum_state.value() or 0.0
        last_output = self.last_output_time.value() or 0

        self.count_state.update(current_count + 1)
        self.sum_state.update(current_sum + transaction.amount)

        # Check if we should output (every 60 seconds)
        current_time = ctx.timestamp()
        if current_time - last_output >= self.output_interval_ms:
            new_count = self.count_state.value()
            new_sum = self.sum_state.value()
            avg = new_sum / new_count if new_count > 0 else 0.0

            # Yield the statistics
            yield {
                'category': transaction.category,
                'count': new_count,
                'total_amount': new_sum,
                'avg_amount': avg,
                'window_start': last_output,
                'window_end': current_time
            }

            # Update last output time
            self.last_output_time.update(current_time)


def main():
    """Main PyFlink job"""
    logger.info("Starting Transaction Analytics Job")

    # Create streaming environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(2)

    # Enable checkpointing for fault tolerance (every 60 seconds)
    env.enable_checkpointing(60000)

    # Add JAR files for Kafka and JDBC connectors
    jar_files = [
    'file:///opt/flink/lib/flink-sql-connector-kafka-3.0.1-1.18.jar',
    'file:///opt/flink/lib/flink-connector-jdbc-3.1.1-1.17.jar',
    'file:///opt/flink/lib/postgresql-42.6.0.jar'
]

    for jar in jar_files:
        env.add_jars(jar)

    # Configure Kafka source
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:29092") \
        .set_topics("transactions") \
        .set_group_id("pyflink-consumer-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # Create watermark strategy with timestamp assigner
    watermark_strategy = WatermarkStrategy \
        .for_monotonous_timestamps() \
        .with_timestamp_assigner(TransactionTimestampAssigner())

    # Read from Kafka and parse JSON
    transactions = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Source"
    ).map(JsonToTransactionMap(), output_type=Types.PICKLED_BYTE_ARRAY())

    # Filter out None values (parsing errors)
    valid_transactions = transactions.filter(lambda x: x is not None)

    # Assign timestamps and watermarks
    transactions_with_timestamps = valid_transactions.assign_timestamps_and_watermarks(
        watermark_strategy
    )

    # Detect high-value transactions (potential fraud)
    high_value_transactions = transactions_with_timestamps.filter(
        HighValueFilter(threshold=1000.0)
    )

    # Print high-value transactions
    high_value_transactions.map(
        lambda t: f"⚠️  HIGH VALUE ALERT: {t}",
        output_type=Types.STRING()
    ).print()

    # Calculate category statistics
    category_stats = transactions_with_timestamps \
        .key_by(lambda t: t.category) \
        .process(CategoryStatsProcessor(), output_type=Types.PICKLED_BYTE_ARRAY())

    # Print category statistics
    category_stats.map(
        lambda s: f"📊 STATS: Category={s['category']}, Count={s['count']}, "
                  f"Total=${s['total_amount']:.2f}, Avg=${s['avg_amount']:.2f}",
        output_type=Types.STRING()
    ).print()

    # Execute the job
    logger.info("Submitting job to Flink cluster")
    env.execute("Transaction Analytics Job")


if __name__ == '__main__':
    main()

