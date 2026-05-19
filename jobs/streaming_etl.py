import os
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, year, month, quarter, dayofmonth, dayofweek, date_format, hour, minute,
    monotonically_increasing_id, lit, when, to_date
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

def process_micro_batch(df, epoch_id, spark, jdbc_url, db_properties, s3_path):
    print(f"Processing micro-batch {epoch_id} with {df.count()} records...")
    if df.isEmpty():
        return

    # 1. Write raw stream data to Data Lake (MinIO)
    df.write.mode("append").parquet(s3_path)

    # 2. Extract distinct dates for dim_date
    distinct_dt = df.select(col("tpep_pickup_datetime").alias("datetime_val")) \
        .union(df.select(col("tpep_dropoff_datetime").alias("datetime_val"))) \
        .distinct().dropna()

    new_dim_date = distinct_dt.select(
        date_format(col("datetime_val"), "yyyyMMdd").cast(IntegerType()).alias("date_sk"),
        to_date(col("datetime_val")).alias("full_date"),
        year(col("datetime_val")).alias("year"),
        quarter(col("datetime_val")).alias("quarter"),
        month(col("datetime_val")).alias("month"),
        dayofmonth(col("datetime_val")).alias("day_of_month"),
        date_format(col("datetime_val"), "EEEE").alias("day_of_week"),
        when(dayofweek(col("datetime_val")).isin([1, 7]), lit(True)).otherwise(lit(False)).alias("is_weekend")
    ).distinct()

    dim_date_types = "date_sk INTEGER, full_date DATE, year INTEGER, quarter INTEGER, month INTEGER, day_of_month INTEGER, day_of_week VARCHAR(20), is_weekend BOOLEAN"
    try:
        existing_dim_date = spark.read.jdbc(url=jdbc_url, table="dim_date", properties=db_properties)
        # Only append dates that don't already exist in the database
        dates_to_append = new_dim_date.join(existing_dim_date, "date_sk", "left_anti")
        if dates_to_append.count() > 0:
            dates_to_append.write.jdbc(url=jdbc_url, table="dim_date", mode="append", properties=db_properties)
    except Exception:
        # Table doesn't exist yet, create it
        new_dim_date.write.option("createTableColumnTypes", dim_date_types).jdbc(url=jdbc_url, table="dim_date", mode="overwrite", properties=db_properties)

    # 3. Create Fact Table
    fact_trip = df.withColumn("trip_id", monotonically_increasing_id()) \
        .withColumn("pickup_date_sk", date_format(col("tpep_pickup_datetime"), "yyyyMMdd").cast(IntegerType())) \
        .withColumn("pickup_time_block_sk", 
            when((hour(col("tpep_pickup_datetime")) >= 5) & (hour(col("tpep_pickup_datetime")) < 12), lit(1))
            .when((hour(col("tpep_pickup_datetime")) >= 12) & (hour(col("tpep_pickup_datetime")) < 17), lit(2))
            .when((hour(col("tpep_pickup_datetime")) >= 17) & (hour(col("tpep_pickup_datetime")) < 21), lit(3))
            .otherwise(lit(4))
        ) \
        .withColumn("dropoff_date_sk", date_format(col("tpep_dropoff_datetime"), "yyyyMMdd").cast(IntegerType())) \
        .withColumn("dropoff_time_block_sk", 
            when((hour(col("tpep_dropoff_datetime")) >= 5) & (hour(col("tpep_dropoff_datetime")) < 12), lit(1))
            .when((hour(col("tpep_dropoff_datetime")) >= 12) & (hour(col("tpep_dropoff_datetime")) < 17), lit(2))
            .when((hour(col("tpep_dropoff_datetime")) >= 17) & (hour(col("tpep_dropoff_datetime")) < 21), lit(3))
            .otherwise(lit(4))
        ) \
        .withColumn("exact_pickup_time", col("tpep_pickup_datetime").cast("timestamp")) \
        .withColumn("exact_dropoff_time", col("tpep_dropoff_datetime").cast("timestamp")) \
        .select(
            col("trip_id"),
            col("VendorID").cast(IntegerType()).alias("vendor_sk"),
            col("pickup_date_sk"),
            col("pickup_time_block_sk"),
            col("dropoff_date_sk"),
            col("dropoff_time_block_sk"),
            col("exact_pickup_time"),
            col("exact_dropoff_time"),
            col("RatecodeID").cast(IntegerType()).alias("rate_code_sk"),
            col("payment_type").cast(IntegerType()).alias("payment_type_sk"),
            col("store_and_fwd_flag"),
            col("pickup_longitude").cast("decimal(9,6)"),
            col("pickup_latitude").cast("decimal(9,6)"),
            col("dropoff_longitude").cast("decimal(9,6)"),
            col("dropoff_latitude").cast("decimal(9,6)"),
            col("passenger_count").cast(IntegerType()),
            col("trip_distance").cast("decimal(10,2)"),
            col("fare_amount").cast("decimal(10,2)"),
            col("extra").cast("decimal(10,2)"),
            col("mta_tax").cast("decimal(10,2)"),
            col("tip_amount").cast("decimal(10,2)"),
            col("tolls_amount").cast("decimal(10,2)"),
            col("improvement_surcharge").cast("decimal(10,2)"),
            col("total_amount").cast("decimal(10,2)")
        )

    # Write fact table (append)
    fact_trip_types = "trip_id BIGINT, vendor_sk INTEGER, pickup_date_sk INTEGER, pickup_time_block_sk INTEGER, dropoff_date_sk INTEGER, dropoff_time_block_sk INTEGER, exact_pickup_time TIMESTAMP, exact_dropoff_time TIMESTAMP, rate_code_sk INTEGER, payment_type_sk INTEGER, store_and_fwd_flag VARCHAR(1), pickup_longitude DECIMAL(9,6), pickup_latitude DECIMAL(9,6), dropoff_longitude DECIMAL(9,6), dropoff_latitude DECIMAL(9,6), passenger_count INTEGER, trip_distance DECIMAL(10,2), fare_amount DECIMAL(10,2), extra DECIMAL(10,2), mta_tax DECIMAL(10,2), tip_amount DECIMAL(10,2), tolls_amount DECIMAL(10,2), improvement_surcharge DECIMAL(10,2), total_amount DECIMAL(10,2)"
    fact_trip.write.option("createTableColumnTypes", fact_trip_types).jdbc(url=jdbc_url, table="fact_trip", mode="append", properties=db_properties)
    print(f"Successfully processed and written micro-batch {epoch_id} to DWH and Data Lake.")

def main():
    spark = SparkSession.builder \
        .appName("NYC_Taxi_Streaming_ETL") \
        .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("MINIO_ENDPOINT", "http://minio:9000")) \
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # DB Configs
    jdbc_url = f"jdbc:postgresql://postgres-dwh:{os.environ.get('DWH_PORT', '5432')}/{os.environ.get('DWH_DATABASE_DB', 'nyctaxi_dwh')}"
    db_properties = {
        "user": os.environ.get("DWH_DATABASE_USER", "dwhuser"),
        "password": os.environ.get("DWH_DATABASE_PASSWORD", "dwhpassword"),
        "driver": "org.postgresql.Driver"
    }
    
    current_date = datetime.datetime.now()
    dummy_year = current_date.strftime("%Y")
    dummy_month = current_date.strftime("%m")
    dummy_day = current_date.strftime("%d")
    s3_path = f"s3a://nyc-taxi-data/yellow_tripdata_processed/year={dummy_year}/month={dummy_month}/day={dummy_day}/"

    # JSON Schema
    schema = StructType([
        StructField("VendorID", DoubleType(), True),
        StructField("tpep_pickup_datetime", StringType(), True),
        StructField("tpep_dropoff_datetime", StringType(), True),
        StructField("passenger_count", DoubleType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("pickup_longitude", DoubleType(), True),
        StructField("pickup_latitude", DoubleType(), True),
        StructField("RatecodeID", DoubleType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("dropoff_longitude", DoubleType(), True),
        StructField("dropoff_latitude", DoubleType(), True),
        StructField("payment_type", DoubleType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True)
    ])

    kafka_broker = os.environ.get("KAFKA_BROKER", "kafka:29092")
    print(f"Connecting to Kafka at {kafka_broker}...")
    
    # Read stream from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", "nyc_taxi_trips") \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON
    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")
    
    print("Starting Streaming Query...")
    
    # Initialize static dimensions on startup to match DWH ETL
    print("Initializing static dimensions...")
    dim_vendor_types = "vendor_sk INTEGER, vendor_id INTEGER, vendor_name VARCHAR(255)"
    dim_rate_code_types = "rate_code_sk INTEGER, rate_code_id INTEGER, rate_description VARCHAR(255)"
    dim_payment_types = "payment_type_sk INTEGER, payment_type_id INTEGER, payment_description VARCHAR(255)"
    dim_time_block_types = "time_block_sk INTEGER, time_block_name VARCHAR(20)"
    
    vendor_data = [(1, 1, "Creative Mobile Technologies"), (2, 2, "VeriFone Inc.")]
    dim_vendor = spark.createDataFrame(vendor_data, ["vendor_sk", "vendor_id", "vendor_name"])
    dim_vendor.write.option("createTableColumnTypes", dim_vendor_types).jdbc(url=jdbc_url, table="dim_vendor", mode="ignore", properties=db_properties)

    rate_code_data = [(1, 1, "Standard rate"), (2, 2, "JFK"), (3, 3, "Newark"), (4, 4, "Nassau or Westchester"), (5, 5, "Negotiated fare"), (6, 6, "Group ride")]
    dim_rate_code = spark.createDataFrame(rate_code_data, ["rate_code_sk", "rate_code_id", "rate_description"])
    dim_rate_code.write.option("createTableColumnTypes", dim_rate_code_types).jdbc(url=jdbc_url, table="dim_rate_code", mode="ignore", properties=db_properties)

    payment_type_data = [(1, 1, "Credit card"), (2, 2, "Cash"), (3, 3, "No charge"), (4, 4, "Dispute"), (5, 5, "Unknown"), (6, 6, "Voided trip")]
    dim_payment_type = spark.createDataFrame(payment_type_data, ["payment_type_sk", "payment_type_id", "payment_description"])
    dim_payment_type.write.option("createTableColumnTypes", dim_payment_types).jdbc(url=jdbc_url, table="dim_payment_type", mode="ignore", properties=db_properties)

    time_block_data = [(1, "Morning"), (2, "Afternoon"), (3, "Evening"), (4, "Night")]
    dim_time_block = spark.createDataFrame(time_block_data, ["time_block_sk", "time_block_name"])
    dim_time_block.write.option("createTableColumnTypes", dim_time_block_types).jdbc(url=jdbc_url, table="dim_time_block", mode="ignore", properties=db_properties)

    # Process micro-batches
    query = parsed_df.writeStream \
        .foreachBatch(lambda df, epoch_id: process_micro_batch(df, epoch_id, spark, jdbc_url, db_properties, s3_path)) \
        .outputMode("append") \
        .trigger(processingTime='10 seconds') \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
