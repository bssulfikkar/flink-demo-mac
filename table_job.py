"""
Transaction Analytics Job using PyFlink Table API
A simpler, SQL-like approach to stream processing.
"""

import logging
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.table.expressions import col, lit
from pyflink.table.window import Tumble

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Main PyFlink Table API job"""
    logger.info("Starting Transaction Analytics Table Job")

    # Create Table Environment
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = StreamTableEnvironment.create(environment_settings=env_settings)

    # Set parallelism
    table_env.get_config().set("parallelism.default", "2")

    # Enable checkpointing
    table_env.get_config().set("execution.checkpointing.interval", "60s")

    # Add required JAR dependencies
    table_env.get_config().set(
    "pipeline.jars",
    "file:///opt/flink/lib/flink-sql-connector-kafka-3.0.1-1.18.jar;"
    "file:///opt/flink/lib/flink-connector-jdbc-3.1.1-1.17.jar;"
    "file:///opt/flink/lib/postgresql-42.6.0.jar"
)
    # Create Kafka source table
    table_env.execute_sql("""
        CREATE TABLE transactions_source (
            transaction_id STRING,
            user_id STRING,
            amount DOUBLE,
            merchant STRING,
            ts BIGINT,
            category STRING,
            event_time AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'transactions',
            'properties.bootstrap.servers' = 'kafka:29092',
            'properties.group.id' = 'pyflink-table-consumer',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # Create PostgreSQL sink table for category statistics
    table_env.execute_sql("""
        CREATE TABLE category_stats_sink (
            category STRING,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            transaction_count BIGINT,
            total_amount DOUBLE,
            avg_amount DOUBLE,
            PRIMARY KEY (category, window_start) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/flink_demo',
            'table-name' = 'category_stats',
            'username' = 'flink',
            'password' = 'flink',
            'driver' = 'org.postgresql.Driver'
        )
    """)

    # Query 1: Detect high-value transactions
    high_value_query = """
        SELECT
            transaction_id,
            user_id,
            amount,
            category,
            merchant,
            event_time
        FROM transactions_source
        WHERE amount > 1000.0
    """

    high_value_table = table_env.sql_query(high_value_query)

    # Print high-value transactions to console
    table_env.create_temporary_view("high_value_txn", high_value_table)
    table_env.execute_sql("""
        SELECT
            CONCAT('⚠️  HIGH VALUE: $', CAST(amount AS STRING), ' - ', category) as alert
        FROM high_value_txn
    """).print()

    # Query 2: Calculate category statistics with 1-minute tumbling windows
    stats_query = """
        INSERT INTO category_stats_sink
        SELECT
            category,
            TUMBLE_START(event_time, INTERVAL '1' MINUTE) as window_start,
            TUMBLE_END(event_time, INTERVAL '1' MINUTE) as window_end,
            COUNT(*) as transaction_count,
            SUM(amount) as total_amount,
            AVG(amount) as avg_amount
        FROM transactions_source
        GROUP BY
            category,
            TUMBLE(event_time, INTERVAL '1' MINUTE)
    """

    # Execute the statistics query
    table_env.execute_sql(stats_query)

    logger.info("Table API job submitted")


if __name__ == '__main__':
    main()

