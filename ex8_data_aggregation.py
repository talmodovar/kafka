#!/usr/bin/env python3
"""
Exercise 8: Data Aggregation from HDFS
Reads weather alert data from HDFS and creates aggregations for visualization.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("WeatherDataAggregation") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Configuration
    HDFS_BASE_PATH = "./hdfs-data"  # Local simulation, use "/hdfs-data" for real HDFS
    OUTPUT_PATH = "./output"  # Output directory for aggregated data

    # Define schema for the JSON data
    schema = StructType([
        StructField("event_time", StringType()),
        StructField("city", StringType()),
        StructField("country", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("temperature", DoubleType()),
        StructField("windspeed", DoubleType()),
        StructField("wind_alert_level", StringType()),  # Can be "level_0", "level_1", "level_2" or int
        StructField("heat_alert_level", StringType()),
        StructField("weather_code", IntegerType())
    ])

    print("=" * 60)
    print("Reading data from HDFS...")
    print(f"Base path: {HDFS_BASE_PATH}")
    print("=" * 60)

    # Read all JSON files recursively from HDFS
    # Pattern: /hdfs-data/{country}/{city}/alerts.json
    df = spark.read \
        .schema(schema) \
        .json(f"{HDFS_BASE_PATH}/*/*/alerts.json")

    # Show initial data info
    print(f"\nTotal records read: {df.count()}")
    print("\nSample data:")
    df.show(5, truncate=False)

    # Data cleaning and transformation
    print("\n" + "=" * 60)
    print("Cleaning and transforming data...")
    print("=" * 60)

    # Convert event_time to timestamp
    df = df.withColumn("timestamp", to_timestamp(col("event_time")))

    # Parse alert levels (handle both "level_X" format and integer format)
    df = df.withColumn(
        "wind_alert_int",
        when(col("wind_alert_level").rlike("^level_"), 
             regexp_extract(col("wind_alert_level"), r"level_(\d+)", 1).cast("int"))
        .otherwise(col("wind_alert_level").cast("int"))
    )

    df = df.withColumn(
        "heat_alert_int",
        when(col("heat_alert_level").rlike("^level_"), 
             regexp_extract(col("heat_alert_level"), r"level_(\d+)", 1).cast("int"))
        .otherwise(col("heat_alert_level").cast("int"))
    )

    # Filter out records with missing critical fields
    df_clean = df.filter(
        col("timestamp").isNotNull() &
        col("city").isNotNull() &
        col("country").isNotNull()
    )

    print(f"Records after cleaning: {df_clean.count()}")

    # ========================================
    # Aggregation 1: Temperature Evolution by City
    # ========================================
    print("\n" + "=" * 60)
    print("Aggregation 1: Temperature Evolution by City")
    print("=" * 60)

    temp_evolution = df_clean.select(
        "timestamp",
        "city",
        "country",
        "temperature"
    ).orderBy("timestamp")

    temp_evolution.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_PATH}/temperature_evolution")

    print(f"Saved to: {OUTPUT_PATH}/temperature_evolution")
    temp_evolution.show(10)

    # ========================================
    # Aggregation 2: Wind Speed Evolution by City
    # ========================================
    print("\n" + "=" * 60)
    print("Aggregation 2: Wind Speed Evolution by City")
    print("=" * 60)

    wind_evolution = df_clean.select(
        "timestamp",
        "city",
        "country",
        "windspeed"
    ).orderBy("timestamp")

    wind_evolution.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_PATH}/wind_evolution")

    print(f"Saved to: {OUTPUT_PATH}/wind_evolution")
    wind_evolution.show(10)

    # ========================================
    # Aggregation 3: Alert Counts by Type and Level
    # ========================================
    print("\n" + "=" * 60)
    print("Aggregation 3: Alert Counts by Type and Level")
    print("=" * 60)

    # Wind alerts
    wind_alerts = df_clean.groupBy("wind_alert_int") \
        .agg(count("*").alias("count")) \
        .withColumn("alert_type", lit("wind")) \
        .withColumnRenamed("wind_alert_int", "alert_level") \
        .select("alert_type", "alert_level", "count")

    # Heat alerts
    heat_alerts = df_clean.groupBy("heat_alert_int") \
        .agg(count("*").alias("count")) \
        .withColumn("alert_type", lit("heat")) \
        .withColumnRenamed("heat_alert_int", "alert_level") \
        .select("alert_type", "alert_level", "count")

    # Combine
    alert_counts = wind_alerts.union(heat_alerts) \
        .orderBy("alert_type", "alert_level")

    alert_counts.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_PATH}/alert_counts")

    print(f"Saved to: {OUTPUT_PATH}/alert_counts")
    alert_counts.show()

    # ========================================
    # Aggregation 4: Most Frequent Weather Code by Country
    # ========================================
    print("\n" + "=" * 60)
    print("Aggregation 4: Most Frequent Weather Code by Country")
    print("=" * 60)

    # Filter out null weather codes
    weather_codes = df_clean.filter(col("weather_code").isNotNull())

    # Count weather codes by country
    weather_code_counts = weather_codes.groupBy("country", "weather_code") \
        .agg(count("*").alias("count"))

    # Find most frequent weather code per country
    window_spec = Window.partitionBy("country").orderBy(desc("count"))
    
    most_frequent_codes = weather_code_counts \
        .withColumn("rank", row_number().over(window_spec)) \
        .filter(col("rank") == 1) \
        .select("country", "weather_code", "count") \
        .orderBy("country")

    most_frequent_codes.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_PATH}/weather_codes_by_country")

    print(f"Saved to: {OUTPUT_PATH}/weather_codes_by_country")
    most_frequent_codes.show()

    # ========================================
    # Summary Statistics
    # ========================================
    print("\n" + "=" * 60)
    print("Summary Statistics")
    print("=" * 60)

    summary = df_clean.groupBy("country", "city") \
        .agg(
            count("*").alias("total_alerts"),
            avg("temperature").alias("avg_temperature"),
            min("temperature").alias("min_temperature"),
            max("temperature").alias("max_temperature"),
            avg("windspeed").alias("avg_windspeed"),
            max("windspeed").alias("max_windspeed")
        ) \
        .orderBy("country", "city")

    summary.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(f"{OUTPUT_PATH}/summary_statistics")

    print(f"Saved to: {OUTPUT_PATH}/summary_statistics")
    summary.show()

    print("\n" + "=" * 60)
    print("Aggregation Complete!")
    print(f"All results saved to: {OUTPUT_PATH}/")
    print("=" * 60)

    spark.stop()

if __name__ == "__main__":
    main()
