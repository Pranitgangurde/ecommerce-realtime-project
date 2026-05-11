"""Spark Structured Streaming job: Kafka → Postgres (raw + aggregates).

Run locally with:
    spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\\
                   org.postgresql:postgresql:42.7.3 \\
        src/batch/spark_jobs/clickstream_consumer.py
"""
from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    from_json,
    sum as spark_sum,
    to_timestamp,
    when,
    window,
)
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
    LongType,
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_CLICKSTREAM", "ecommerce.clickstream.v1")

ANALYTICS_JDBC_URL = (
    f"jdbc:postgresql://{os.getenv('ANALYTICS_DB_HOST', 'localhost')}:"
    f"{os.getenv('ANALYTICS_DB_PORT', '5433')}/"
    f"{os.getenv('ANALYTICS_DB_NAME', 'analytics')}"
)
JDBC_PROPS = {
    "user": os.getenv("ANALYTICS_DB_USER", "analytics_user"),
    "password": os.getenv("ANALYTICS_DB_PASSWORD", "changeme_local_only"),
    "driver": "org.postgresql.Driver",
}

# Schema must match producer schema exactly. JSON is loose — we enforce here.
event_schema = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("event_type", StringType(), nullable=False),
    StructField("user_id", LongType(), nullable=True),
    StructField("session_id", StringType(), nullable=False),
    StructField("product_id", LongType(), nullable=True),
    StructField("event_timestamp", StringType(), nullable=False),  # parse → TS below
    StructField("properties", StringType(), nullable=True),  # keep as JSON string
])


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("clickstream-consumer")
        .config("spark.sql.shuffle.partitions", "4")  # local dev; bump in prod
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints/clickstream")
        .getOrCreate()
    )


def write_raw_to_postgres(batch_df, batch_id: int) -> None:
    """foreachBatch sink — writes a micro-batch to Postgres raw table."""
    if batch_df.isEmpty():
        return

    (
        batch_df
        .write
        .mode("append")
        .jdbc(
            url=ANALYTICS_JDBC_URL,
            table="raw.clickstream_events",
            properties=JDBC_PROPS,
        )
    )


def write_aggregates_to_postgres(batch_df, batch_id: int) -> None:
    """Upsert per-minute aggregates."""
    if batch_df.isEmpty():
        return

    # Stage to temp table, then MERGE — gives us upsert semantics on Postgres
    staging_table = f"marts.revenue_per_minute_staging_{batch_id}"
    (
        batch_df
        .write
        .mode("overwrite")
        .jdbc(
            url=ANALYTICS_JDBC_URL,
            table=staging_table,
            properties=JDBC_PROPS,
        )
    )

    # Merge using psycopg2 (Spark JDBC has no native upsert)
    import psycopg2  # local import keeps Spark driver light

    conn = psycopg2.connect(
        host=os.getenv("ANALYTICS_DB_HOST", "localhost"),
        port=int(os.getenv("ANALYTICS_DB_PORT", "5433")),
        dbname=os.getenv("ANALYTICS_DB_NAME", "analytics"),
        user=JDBC_PROPS["user"],
        password=JDBC_PROPS["password"],
    )
    try:
        with conn, conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO marts.revenue_per_minute
                    (minute_bucket, event_count, unique_users, total_clicks, total_purchases)
                SELECT minute_bucket, event_count, unique_users, total_clicks, total_purchases
                FROM {staging_table}
                ON CONFLICT (minute_bucket) DO UPDATE SET
                    event_count = EXCLUDED.event_count,
                    unique_users = EXCLUDED.unique_users,
                    total_clicks = EXCLUDED.total_clicks,
                    total_purchases = EXCLUDED.total_purchases;

                DROP TABLE IF EXISTS {staging_table};
            """)
    finally:
        conn.close()


def main() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # ─── Read from Kafka ───
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ─── Parse JSON value ───
    parsed = (
        raw_stream
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), event_schema).alias("e"))
        .select("e.*")
        .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
    )

    # ─── Sink 1: raw events to Postgres ───
    raw_query = (
        parsed
        .writeStream
        .foreachBatch(write_raw_to_postgres)
        .outputMode("append")
        .option("checkpointLocation", "/tmp/spark-checkpoints/raw-sink")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # ─── Sink 2: per-minute aggregates ───
    agg = (
        parsed
        .withWatermark("event_timestamp", "2 minutes")  # tolerate 2min late events
        .groupBy(window(col("event_timestamp"), "1 minute").alias("w"))
        .agg(
            count("*").alias("event_count"),
            countDistinct("user_id").alias("unique_users"),
            spark_sum(when(col("event_type") == "page_view", 1).otherwise(0)).alias("total_clicks"),
            spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("total_purchases"),
        )
        .select(
            col("w.start").alias("minute_bucket"),
            col("event_count"),
            col("unique_users"),
            col("total_clicks"),
            col("total_purchases"),
        )
    )

    agg_query = (
        agg
        .writeStream
        .foreachBatch(write_aggregates_to_postgres)
        .outputMode("update")
        .option("checkpointLocation", "/tmp/spark-checkpoints/agg-sink")
        .trigger(processingTime="60 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()