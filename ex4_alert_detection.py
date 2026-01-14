from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def main():
    spark = SparkSession.builder \
        .appName("WeatherAlertDetection") \
        .getOrCreate()

    # Define validation log level
    spark.sparkContext.setLogLevel("WARN")

    # Kafka Config
    KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
    INPUT_TOPIC = "weather_stream"
    OUTPUT_TOPIC = "weather_transformed"

    # Define Schema matching Open-Meteo response
    # We are interested in the 'current_weather' struct
    weather_schema = StructType([
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("current_weather", StructType([
            StructField("temperature", DoubleType()),
            StructField("windspeed", DoubleType()),
            StructField("winddirection", DoubleType()),
            StructField("weathercode", IntegerType()),
            StructField("time", StringType())
        ]))
    ])

    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON
    parsed_df = df.select(
        from_json(col("value").cast("string"), weather_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp") # Keep original timestamp if needed
    ).select("data.*")

    # Determine Alerts
    # Wind: < 10 (level_0), 10-20 (level_1), > 20 (level_2)
    # Heat: < 25 (level_0), 25-35 (level_1), > 35 (level_2)
    
    transformed_df = parsed_df \
        .withColumn("event_time", current_timestamp()) \
        .withColumn("wind_alert_level", 
                    when(col("current_weather.windspeed") > 20, "level_2")
                    .when(col("current_weather.windspeed") >= 10, "level_1")
                    .otherwise("level_0")) \
        .withColumn("heat_alert_level",
                    when(col("current_weather.temperature") > 35, "level_2")
                    .when(col("current_weather.temperature") >= 25, "level_1")
                    .otherwise("level_0"))

    # Select relevant columns for output
    output_df = transformed_df.select(
        col("event_time").cast("string"),
        col("latitude"),
        col("longitude"),
        col("current_weather.temperature").alias("temperature"),
        col("current_weather.windspeed").alias("windspeed"),
        col("wind_alert_level"),
        col("heat_alert_level")
    )

    # Format as JSON for Kafka output
    kafka_output_df = output_df.select(
        to_json(struct(output_df.columns)).alias("value")
    )

    # Write to Kafka
    query = kafka_output_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", OUTPUT_TOPIC) \
        .option("checkpointLocation", "/tmp/checkpoints/weather_alerts") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
