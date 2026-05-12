import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, dayofweek, date_format, hour, 
    unix_timestamp, monotonically_increasing_id, lit,
    when, date_trunc
)
from pyspark.sql.types import IntegerType, BooleanType

def main():
    parser = argparse.ArgumentParser(description="NYC Taxi Data Warehouse ETL")
    parser.add_argument('--year', required=True, help="Year to process (e.g., 2016)")
    parser.add_argument('--month', required=True, help="Month to process (e.g., 03)")
    args = parser.parse_args()

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
    jdbc_url = "jdbc:postgresql://postgres-dwh:5432/nyctaxi_dwh"
    db_properties = {
        "user": "dwhuser",
        "password": "dwhpassword",
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
    dim_vendor = spark.createDataFrame(vendor_data, ["vendor_id", "vendor_code", "vendor_name"])

    # 2. dim_rate_code
    rate_code_data = [
        (1, 1, "Standard rate"),
        (2, 2, "JFK"),
        (3, 3, "Newark"),
        (4, 4, "Nassau or Westchester"),
        (5, 5, "Negotiated fare"),
        (6, 6, "Group ride")
    ]
    dim_rate_code = spark.createDataFrame(rate_code_data, ["rate_code_id", "rate_code", "rate_code_description"])

    # 3. dim_payment_type
    payment_type_data = [
        (1, 1, "Credit card"),
        (2, 2, "Cash"),
        (3, 3, "No charge"),
        (4, 4, "Dispute"),
        (5, 5, "Unknown"),
        (6, 6, "Voided trip")
    ]
    dim_payment_type = spark.createDataFrame(payment_type_data, ["payment_type_id", "payment_type", "payment_description"])

    # 4. dim_datetime
    # Extract unique datetimes from both pickup and dropoff
    pickup_dt = df.select(col("tpep_pickup_datetime").alias("datetime_val"))
    dropoff_dt = df.select(col("tpep_dropoff_datetime").alias("datetime_val"))
    
    distinct_dt = pickup_dt.union(dropoff_dt).distinct().dropna()

    dim_datetime = distinct_dt.select(
        date_format(col("datetime_val"), "yyyyMMddHHmmss").cast(IntegerType()).alias("datetime_id"),
        date_trunc("day", col("datetime_val")).alias("full_date"),
        hour(col("datetime_val")).alias("hour"),
        dayofweek(col("datetime_val")).alias("day_of_week"),
        date_format(col("datetime_val"), "EEEE").alias("day_name"),
        when(dayofweek(col("datetime_val")).isin([1, 7]), lit(True)).otherwise(lit(False)).alias("is_weekend"),
        month(col("datetime_val")).alias("month"),
        year(col("datetime_val")).alias("year"),
        col("datetime_val")
    )

    print("Building Fact Table...")

    # Join to get datetime_ids and calculate duration
    fact_trips = df.withColumn("trip_id", monotonically_increasing_id()) \
        .withColumn("pickup_datetime_id", date_format(col("tpep_pickup_datetime"), "yyyyMMddHHmmss").cast(IntegerType())) \
        .withColumn("dropoff_datetime_id", date_format(col("tpep_dropoff_datetime"), "yyyyMMddHHmmss").cast(IntegerType())) \
        .withColumn("trip_duration_seconds", 
                    unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime")))

    # Select final columns for fact table
    fact_trips = fact_trips.select(
        col("trip_id"),
        col("VendorID").alias("vendor_id"),
        col("pickup_datetime_id"),
        col("dropoff_datetime_id"),
        col("RatecodeID").alias("rate_code_id"),
        col("payment_type").alias("payment_type_id"),
        col("store_and_fwd_flag"),
        col("pickup_longitude"),
        col("pickup_latitude"),
        col("dropoff_longitude"),
        col("dropoff_latitude"),
        col("passenger_count"),
        col("trip_distance"),
        col("fare_amount"),
        col("extra"),
        col("mta_tax"),
        col("improvement_surcharge"),
        col("tip_amount"),
        col("tolls_amount"),
        col("total_amount"),
        col("trip_duration_seconds")
    )

    print("Writing to Data Warehouse (PostgreSQL)...")

    # Write Dimensions
    print("Writing dim_vendor...")
    dim_vendor.write.jdbc(url=jdbc_url, table="dim_vendor", mode="overwrite", properties=db_properties)
    
    print("Writing dim_rate_code...")
    dim_rate_code.write.jdbc(url=jdbc_url, table="dim_rate_code", mode="overwrite", properties=db_properties)
    
    print("Writing dim_payment_type...")
    dim_payment_type.write.jdbc(url=jdbc_url, table="dim_payment_type", mode="overwrite", properties=db_properties)

    print("Writing dim_datetime...")
    # Drop datetime_val used for intermediate join before writing
    dim_datetime_out = dim_datetime.drop("datetime_val")
    dim_datetime_out.write.jdbc(url=jdbc_url, table="dim_datetime", mode="overwrite", properties=db_properties)

    print("Writing fact_trips...")
    # Use append mode or overwrite. Overwrite will replace everything, suitable for a complete reload.
    fact_trips.write.jdbc(url=jdbc_url, table="fact_trips", mode="overwrite", properties=db_properties)

    print("DWH ETL completed successfully!")
    spark.stop()

if __name__ == "__main__":
    main()
