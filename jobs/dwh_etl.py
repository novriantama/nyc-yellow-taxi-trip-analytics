import argparse
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, quarter, dayofmonth, dayofweek, date_format, hour, minute,
    unix_timestamp, monotonically_increasing_id, lit,
    when, to_date
)
from pyspark.sql.types import IntegerType, BooleanType, DecimalType

def main():
    parser = argparse.ArgumentParser(description="NYC Taxi Data Warehouse ETL")
    parser.add_argument('--year', required=True, help="Year to process (e.g., 2016)")
    parser.add_argument('--month', required=True, help="Month to process (e.g., 03)")
    args = parser.parse_args()

    # year_str = args.year
    # month_str = str(args.month).zfill(2)
    year_str = "2016"
    month_str = "03"

    # Initialize SparkSession with S3 and Postgres JDBC support
    spark = SparkSession.builder \
        .appName(f"NYC_Taxi_DWH_ETL_{year_str}_{month_str}") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    # DWH Connection Properties
    jdbc_url = f"jdbc:postgresql://postgres-dwh:{os.environ.get('DWH_PORT')}/{os.environ.get('DWH_DATABASE_DB')}"
    db_properties = {
        "user": os.environ.get("DWH_DATABASE_USER"),
        "password": os.environ.get("DWH_DATABASE_PASSWORD"),
        "driver": "org.postgresql.Driver"
    }

    input_path = f"s3a://nyc-taxi-data/yellow_tripdata_processed/year={year_str}/month={month_str}/"
    print(f"Reading processed data from {input_path}")
    
    try:
        df = spark.read.parquet(input_path)
    except Exception as e:
        print(f"Error reading data: {e}. Exiting.")
        sys.exit(1)

    print("Building Dimensions...")

    # 1. dim_vendor
    vendor_data = [
        (1, 1, "Creative Mobile Technologies"),
        (2, 2, "VeriFone Inc.")
    ]
    dim_vendor = spark.createDataFrame(vendor_data, ["vendor_sk", "vendor_id", "vendor_name"])

    # 2. dim_rate_code
    rate_code_data = [
        (1, 1, "Standard rate"),
        (2, 2, "JFK"),
        (3, 3, "Newark"),
        (4, 4, "Nassau or Westchester"),
        (5, 5, "Negotiated fare"),
        (6, 6, "Group ride")
    ]
    dim_rate_code = spark.createDataFrame(rate_code_data, ["rate_code_sk", "rate_code_id", "rate_description"])

    # 3. dim_payment_type
    payment_type_data = [
        (1, 1, "Credit card"),
        (2, 2, "Cash"),
        (3, 3, "No charge"),
        (4, 4, "Dispute"),
        (5, 5, "Unknown"),
        (6, 6, "Voided trip")
    ]
    dim_payment_type = spark.createDataFrame(payment_type_data, ["payment_type_sk", "payment_type_id", "payment_description"])

    # 4. dim_date and dim_time
    # Extract unique datetimes from both pickup and dropoff
    pickup_dt = df.select(col("tpep_pickup_datetime").alias("datetime_val"))
    dropoff_dt = df.select(col("tpep_dropoff_datetime").alias("datetime_val"))
    
    distinct_dt = pickup_dt.union(dropoff_dt).distinct().dropna()

    dim_date = distinct_dt.select(
        date_format(col("datetime_val"), "yyyyMMdd").cast(IntegerType()).alias("date_sk"),
        to_date(col("datetime_val")).alias("full_date"),
        year(col("datetime_val")).alias("year"),
        quarter(col("datetime_val")).alias("quarter"),
        month(col("datetime_val")).alias("month"),
        dayofmonth(col("datetime_val")).alias("day_of_month"),
        date_format(col("datetime_val"), "EEEE").alias("day_of_week"),
        when(dayofweek(col("datetime_val")).isin([1, 7]), lit(True)).otherwise(lit(False)).alias("is_weekend")
    ).distinct()

    time_block_data = [
        (1, "Morning"),
        (2, "Afternoon"),
        (3, "Evening"),
        (4, "Night")
    ]
    dim_time_block = spark.createDataFrame(time_block_data, ["time_block_sk", "time_block_name"])

    print("Building Fact Table...")
    
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

    print("Writing to Data Warehouse (PostgreSQL)...")

    dim_vendor_types = "vendor_sk INTEGER, vendor_id INTEGER, vendor_name VARCHAR(255)"
    dim_rate_code_types = "rate_code_sk INTEGER, rate_code_id INTEGER, rate_description VARCHAR(255)"
    dim_payment_types = "payment_type_sk INTEGER, payment_type_id INTEGER, payment_description VARCHAR(255)"
    dim_date_types = "date_sk INTEGER, full_date DATE, year INTEGER, quarter INTEGER, month INTEGER, day_of_month INTEGER, day_of_week VARCHAR(20), is_weekend BOOLEAN"
    dim_time_block_types = "time_block_sk INTEGER, time_block_name VARCHAR(20)"
    fact_trip_types = "trip_id BIGINT, vendor_sk INTEGER, pickup_date_sk INTEGER, pickup_time_block_sk INTEGER, dropoff_date_sk INTEGER, dropoff_time_block_sk INTEGER, exact_pickup_time TIMESTAMP, exact_dropoff_time TIMESTAMP, rate_code_sk INTEGER, payment_type_sk INTEGER, store_and_fwd_flag VARCHAR(1), pickup_longitude DECIMAL(9,6), pickup_latitude DECIMAL(9,6), dropoff_longitude DECIMAL(9,6), dropoff_latitude DECIMAL(9,6), passenger_count INTEGER, trip_distance DECIMAL(10,2), fare_amount DECIMAL(10,2), extra DECIMAL(10,2), mta_tax DECIMAL(10,2), tip_amount DECIMAL(10,2), tolls_amount DECIMAL(10,2), improvement_surcharge DECIMAL(10,2), total_amount DECIMAL(10,2)"

    # Write Dimensions
    print("Writing dim_vendor...")
    dim_vendor.write.option("createTableColumnTypes", dim_vendor_types).jdbc(url=jdbc_url, table="dim_vendor", mode="overwrite", properties=db_properties)
    
    print("Writing dim_rate_code...")
    dim_rate_code.write.option("createTableColumnTypes", dim_rate_code_types).jdbc(url=jdbc_url, table="dim_rate_code", mode="overwrite", properties=db_properties)
    
    print("Writing dim_payment_type...")
    dim_payment_type.write.option("createTableColumnTypes", dim_payment_types).jdbc(url=jdbc_url, table="dim_payment_type", mode="overwrite", properties=db_properties)

    print("Writing dim_date...")
    dim_date.write.option("createTableColumnTypes", dim_date_types).jdbc(url=jdbc_url, table="dim_date", mode="overwrite", properties=db_properties)

    print("Writing dim_time_block...")
    dim_time_block.write.option("createTableColumnTypes", dim_time_block_types).jdbc(url=jdbc_url, table="dim_time_block", mode="overwrite", properties=db_properties)

    print("Writing fact_trip...")
    fact_trip.write.option("createTableColumnTypes", fact_trip_types).jdbc(url=jdbc_url, table="fact_trip", mode="overwrite", properties=db_properties)

    print("DWH ETL completed successfully!")
    spark.stop()

if __name__ == "__main__":
    main()
