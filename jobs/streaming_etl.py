import os
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

    new_dim_time = distinct_dt.select(
        date_format(col("datetime_val"), "HHmmss").cast(IntegerType()).alias("time_sk"),
        date_format(col("datetime_val"), "HH:mm:ss").alias("time_of_day_full"),
        hour(col("datetime_val")).alias("hour"),
        minute(col("datetime_val")).alias("minute"),
        when((hour(col("datetime_val")) >= 5) & (hour(col("datetime_val")) < 12), lit("Morning"))
        .when((hour(col("datetime_val")) >= 12) & (hour(col("datetime_val")) < 17), lit("Afternoon"))
        .when((hour(col("datetime_val")) >= 17) & (hour(col("datetime_val")) < 21), lit("Evening"))
        .otherwise(lit("Night")).alias("time_block")
    ).distinct()

    # Load existing dims, union with new, drop duplicates, then overwrite to maintain historical dimension keys safely
    try:
        existing_dim_date = spark.read.jdbc(url=jdbc_url, table="dim_date", properties=db_properties)
        combined_dim_date = existing_dim_date.unionByName(new_dim_date, allowMissingColumns=True).distinct()
    except Exception:
        combined_dim_date = new_dim_date
    
    try:
        existing_dim_time = spark.read.jdbc(url=jdbc_url, table="dim_time", properties=db_properties)
        combined_dim_time = existing_dim_time.unionByName(new_dim_time, allowMissingColumns=True).distinct()
    except Exception:
        combined_dim_time = new_dim_time

    combined_dim_date.write.jdbc(url=jdbc_url, table="dim_date", mode="overwrite", properties=db_properties)
    combined_dim_time.write.jdbc(url=jdbc_url, table="dim_time", mode="overwrite", properties=db_properties)

    # 3. Create Fact Table
    fact_trip = df.withColumn("trip_id", monotonically_increasing_id()) \
        .withColumn("pickup_date_sk", date_format(col("tpep_pickup_datetime"), "yyyyMMdd").cast(IntegerType())) \
        .withColumn("pickup_time_sk", date_format(col("tpep_pickup_datetime"), "HHmmss").cast(IntegerType())) \
        .withColumn("dropoff_date_sk", date_format(col("tpep_dropoff_datetime"), "yyyyMMdd").cast(IntegerType())) \
        .withColumn("dropoff_time_sk", date_format(col("tpep_dropoff_datetime"), "HHmmss").cast(IntegerType())) \
        .select(
            col("trip_id"),
            col("VendorID").cast(IntegerType()).alias("vendor_sk"),
            col("pickup_date_sk"),
            col("pickup_time_sk"),
            col("dropoff_date_sk"),
            col("dropoff_time_sk"),
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
    fact_trip.write.jdbc(url=jdbc_url, table="fact_trip", mode="append", properties=db_properties)
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
    s3_path = "s3a://nyc-taxi-data/yellow_tripdata_processed/stream/"

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
    
    # Process micro-batches
    query = parsed_df.writeStream \
        .foreachBatch(lambda df, epoch_id: process_micro_batch(df, epoch_id, spark, jdbc_url, db_properties, s3_path)) \
        .outputMode("append") \
        .trigger(processingTime='10 seconds') \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
