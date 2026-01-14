from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def main():
    # Initialize Spark Session
    # Using local[*] to run locally with all available cores
    spark = SparkSession.builder \
        .appName("WeatherWindowAggregation") \
        .getOrCreate()

    # Set log level to WARN to reduce data noise in console
    spark.sparkContext.setLogLevel("WARN")

    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
    INPUT_TOPIC = "weather_transformed"
    OUTPUT_TOPIC = "weather_aggregates"

    # Define Schema for the input JSON data
    # Based on the requirements: event_time, temperature, windspeed, alerts, city, country
    schema = StructType([
        StructField("event_time", StringType()), # Initially read as String, will cast to Timestamp
        StructField("temperature", DoubleType()),
        StructField("windspeed", DoubleType()),
        StructField("wind_alert_level", IntegerType()),
        StructField("heat_alert_level", IntegerType()),
        StructField("city", StringType()),
        StructField("country", StringType())
    ])

    # 1. Read Stream from Kafka
    print("Reading from Kafka...")
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # 2. Transform Data
    # Parse JSON value and extract columns
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Convert event_time string to TimestampType for windowing
    # Assuming ISO 8601 format in string
    # Apply Watermark to handle late data (allow up to 10 minutes delay)
    processed_df = parsed_df \
        .withColumn("timestamp", col("event_time").cast("timestamp")) \
        .withWatermark("timestamp", "10 minutes")

    # 3. Aggregations on Window
    # Sliding Window: 5 minutes window, sliding every 5 minutes (Tumbling window) for simplicity as requested "1 or 5 minutes"
    # To make it sliding (e.g. every 1 min), change second arg to "1 minute"
    # Group by: window, city, country
    
    WINDOW_DURATION = "5 minutes"
    SLIDE_DURATION = "5 minutes" # Tumbling window (non-overlapping)

    aggregates_df = processed_df \
        .groupBy(
            window(col("timestamp"), WINDOW_DURATION, SLIDE_DURATION),
            col("city"),
            col("country")
        ) \
        .agg(
            count(when(col("wind_alert_level") >= 1, 1)).alias("wind_alert_count"),
            count(when(col("heat_alert_level") >= 1, 1)).alias("heat_alert_count"),
            avg("temperature").alias("avg_temperature"),
            min("temperature").alias("min_temperature"),
            max("temperature").alias("max_temperature")
        )

    # 4. Structure Final Output
    # Flatten the window struct to window_start and window_end
    final_output_df = aggregates_df.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("city"),
        col("country"),
        col("avg_temperature"),
        col("min_temperature"),
        col("max_temperature"),
        col("wind_alert_count"),
        col("heat_alert_count")
    )

    # 5. Output Sinks
    
    # Debug Output to Console
    # outputMode "update" is suitable for aggregations (emits only updated rows)
    # outputMode "append" is compatible only if watermark is handled strictly and windows finalized
    # For debugging, "update" or "complete" (small data) is often used. 
    # With Watermark + Aggregation, append mode emits only after watermark passes window end.
    
    print("Starting Console Query...")
    console_query = final_output_df.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    # Optional: Write to Kafka
    # Requires serialization to JSON
    # To enable: uncomment the block below
    """
    kafka_output_json = final_output_df.select(
        to_json(struct([col(c) for c in final_output_df.columns])).alias("value")
    )
    
    kafka_query = kafka_output_json.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", OUTPUT_TOPIC) \
        .option("checkpointLocation", "/tmp/checkpoints/weather_aggregates") \
        .outputMode("update") \
        .start()
    """

    console_query.awaitTermination()

if __name__ == "__main__":
    main()
