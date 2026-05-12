"""
Clean Transactions - Local Read, S3 Write
Workaround for S3A configuration issues
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, upper
import sys

BUCKET_NAME = "ecommerce-data-pipeline-kausalya"

def create_spark_session():
    """Create simple Spark session"""
    spark = SparkSession.builder \
        .appName("Clean Transactions Local") \
        .config("spark.jars.packages", 
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "com.amazonaws:aws-java-sdk-bundle:1.11.901") \
        .config("spark.hadoop.fs.s3a.impl", 
                "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "200000") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .getOrCreate()
    
    return spark

def main():
    spark = create_spark_session()
    
    print("=" * 60)
    print("CLEANING TRANSACTIONS (Local Read)")
    print("=" * 60)
    
    # Read from LOCAL file (already downloaded)
    print("\n1. Reading local CSV...")
    df = spark.read.csv(
        "../data/raw/transactions.csv",
        header=True,
        inferSchema=True
    )
    
    print(f"   Records: {df.count():,}")
    
    # Clean data (same as before)
    print("\n2. Cleaning data...")
    df_clean = df.dropDuplicates(['transaction_id'])
    df_clean = df_clean.dropna(subset=['transaction_id', 'customer_id', 'product_id'])
    df_clean = df_clean.filter(
        (col('unit_price') > 0) & 
        (col('quantity') > 0)
    )
    df_clean = df_clean.withColumn('status', trim(upper(col('status'))))
    
    print(f"   Clean records: {df_clean.count():,}")
    
    # Write to S3
    print("\n3. Writing to S3...")
    output_path = f"s3a://{BUCKET_NAME}/processed/transactions/"
    
    df_clean.write \
        .mode('overwrite') \
        .option('header', 'true') \
        .csv(output_path)
    
    print("✓ Success!")
    
    spark.stop()

if __name__ == "__main__":
    main()