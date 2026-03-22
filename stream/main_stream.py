from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, count, min as smin, max as smax, to_json, struct, lit
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "signup-events"

PG_URL  = "jdbc:postgresql://postgres:5432/fraud"
PG_USER = "app"
PG_PASS = "app"

# 24-hour window threshold for repeated signups per device
# Increased to 10 to avoid false positives from normal user activity
THRESHOLD = 10

# Checkpoint paths for fault tolerance and exactly-once semantics
CHECKPOINT_SIGNUPS = "/opt/stream/checkpoints/signups"
CHECKPOINT_ALERTS = "/opt/stream/checkpoints/alerts"

schema = StructType([
    StructField("event", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("device_id", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("label_abusive", BooleanType(), True),
])

def write_signups(batch_df, epoch_id):
    (batch_df
        .select(
            col("event"),
            col("user_id"),
            col("device_id"),
            col("ts").alias("ts"),
            col("label_abusive"),
            to_json(struct(*[c for c in batch_df.columns])).alias("raw_json")
        )
        .write
        .format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", "signups")
        .option("user", PG_USER)
        .option("password", PG_PASS)
        .option("driver", "org.postgresql.Driver")
        .option("stringtype", "unspecified")   # ← let server infer jsonb
        .mode("append")
        .save())

def write_alerts(batch_df, epoch_id):
    """
    Write alerts to PostgreSQL with deduplication protection.
    Uses ON CONFLICT to handle duplicate (device_id, window_start, window_end) tuples.
    If a duplicate is detected, update the signup_count and timestamps if they changed.
    """
    if batch_df.isEmpty():
        return
    
    # Create a temporary view for this batch
    batch_df.createOrReplaceTempView("temp_alerts")
    
    # Get a JDBC connection and use raw SQL for ON CONFLICT handling
    jdbc_url = PG_URL
    props = {
        "user": PG_USER,
        "password": PG_PASS,
        "driver": "org.postgresql.Driver"
    }
    
    # Use INSERT ... ON CONFLICT to handle duplicates
    # ON CONFLICT DO UPDATE will update counts if the window receives more events
    insert_sql = """
        INSERT INTO alerts (device_id, window_start, window_end, signup_count, threshold, first_seen_ts, last_seen_ts, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, NOW())
        ON CONFLICT (device_id, window_start, window_end) 
        DO UPDATE SET 
            signup_count = EXCLUDED.signup_count,
            last_seen_ts = EXCLUDED.last_seen_ts,
            created_at = NOW()
    """
    
    # Collect rows and insert them using JDBC with ON CONFLICT
    rows = batch_df.collect()
    
    # Import Java SQL libraries through Spark's JVM
    from py4j.java_gateway import java_import
    java_import(batch_df._sc._jvm, "java.sql.*")
    
    # Get connection
    conn = batch_df._sc._jvm.java.sql.DriverManager.getConnection(jdbc_url, PG_USER, PG_PASS)
    
    try:
        conn.setAutoCommit(False)
        stmt = conn.prepareStatement(insert_sql)
        
        for row in rows:
            stmt.setString(1, row.device_id)
            stmt.setTimestamp(2, batch_df._sc._jvm.java.sql.Timestamp.valueOf(str(row.window_start)))
            stmt.setTimestamp(3, batch_df._sc._jvm.java.sql.Timestamp.valueOf(str(row.window_end)))
            stmt.setInt(4, row.signup_count)
            stmt.setInt(5, row.threshold)
            stmt.setTimestamp(6, batch_df._sc._jvm.java.sql.Timestamp.valueOf(str(row.first_seen_ts)))
            stmt.setTimestamp(7, batch_df._sc._jvm.java.sql.Timestamp.valueOf(str(row.last_seen_ts)))
            stmt.addBatch()
        
        stmt.executeBatch()
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        stmt.close()
        conn.close()

if __name__ == "__main__":
    spark = (SparkSession.builder
        .appName("trial-promo-abuse-detector")
        .getOrCreate())
    spark.conf.set("spark.sql.shuffle.partitions", "8")

    # Read Kafka
    raw = (spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load())

    # Parse JSON
    df = (raw.selectExpr("CAST(value AS STRING) AS value")
            .select(from_json(col("value"), schema).alias("d")).select("d.*"))
    # Event time
    df = df.withColumn("ts", to_timestamp(col("timestamp")))

    # Write raw signups to Postgres (append) with checkpointing
    query_signups = (df.writeStream
        .foreachBatch(write_signups)
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_SIGNUPS)
        .queryName("signups_stream")
        .start())

    # Compute 24-hour tumbling window counts per device
    w = window(col("ts"), "24 hours")
    agg = (df
        .withWatermark("ts", "24 hours")
        .groupBy(col("device_id"), w)
        .agg(
            count(lit(1)).alias("signup_count"),
            smin("ts").alias("first_seen_ts"),
            smax("ts").alias("last_seen_ts"),
        )
        .where(col("signup_count") >= lit(THRESHOLD))
        .select(
            col("device_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("signup_count"),
            lit(THRESHOLD).alias("threshold"),
            col("first_seen_ts"),
            col("last_seen_ts"),
        ))

    # Write alerts with checkpointing
    query_alerts = (agg.writeStream
        .foreachBatch(write_alerts)
        .outputMode("update")
        .option("checkpointLocation", CHECKPOINT_ALERTS)
        .queryName("alerts_stream")
        .start())

    spark.streams.awaitAnyTermination()
