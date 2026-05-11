import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

# MinIO Connection Settings
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")

def main():
    print("Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName("NYC Taxi Batch Processing") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    # The data directory is mounted to /opt/spark/data in the container
    input_file = "/opt/spark/data/yellow_tripdata_2016-03.csv"
    output_path = "s3a://nyc-taxi-data/yellow_tripdata_processed/year=2016/month=03/"

    print(f"Reading raw data from {input_file}...")
    df = spark.read.csv(input_file, header=True, inferSchema=True)
    
    print(f"Original record count: {df.count()}")

    print("Applying data cleaning rules...")
    # Basic cleaning: 
    # 1. Remove trips with 0 passengers
    # 2. Remove trips with 0 or negative distance
    clean_df = df.filter(
        (col("passenger_count") > 0) & 
        (col("trip_distance") > 0.0)
    )

    # Add a processing timestamp
    clean_df = clean_df.withColumn("processed_at", current_timestamp())

    print(f"Cleaned record count: {clean_df.count()}")

    print(f"Writing processed data to MinIO at {output_path} in Parquet format...")
    # Coalesce to reduce number of small files for optimal Data Lake performance
    clean_df.coalesce(4).write.mode("overwrite").parquet(output_path)

    print("Batch processing completed successfully!")
    spark.stop()

if __name__ == "__main__":
    main()
