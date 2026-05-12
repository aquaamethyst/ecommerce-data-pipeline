"""
Test with PySpark 3.3 + proven JAR versions
"""

from pyspark.sql import SparkSession

print("Creating Spark session with PySpark 3.3...")

spark = SparkSession.builder \
    .appName("S3 Test PySpark 3.3") \
    .master("local[*]") \
    .config("spark.jars.packages", 
            "org.apache.hadoop:hadoop-aws:3.3.1,"
            "com.amazonaws:aws-java-sdk-bundle:1.11.901") \
    .config("spark.hadoop.fs.s3a.impl", 
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "200000") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("\n" + "=" * 60)
print("Testing S3 READ with PySpark 3.3")
print("=" * 60)

try:
    df = spark.read.csv(
        "s3a://ecommerce-data-pipeline-kausalya/raw/transactions/",
        header=True,
        inferSchema=True
    )
    
    count = df.count()
    print(f"\n✓ SUCCESS! Read {count:,} rows from S3")
    df.show(5)
    
    print("\n" + "=" * 60)
    print("Testing S3 WRITE")
    print("=" * 60)
    
    df.limit(10).coalesce(1).write \
        .mode('overwrite') \
        .option('header', 'true') \
        .csv('s3a://ecommerce-data-pipeline-kausalya/test/pyspark33_test/')
    
    print("✓ SUCCESS! Wrote test data to S3")
    
    print("\n" + "=" * 60)
    print("ALL TESTS PASSED WITH PYSPARK 3.3! ✓")
    print("=" * 60)
    
except Exception as e:
    print(f"\n✗ FAILED: {e}")
    import traceback
    traceback.print_exc()

spark.stop()