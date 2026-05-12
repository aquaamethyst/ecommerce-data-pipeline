"""
Test Spark locally with CSV files
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create simple Spark session (no S3)
spark = SparkSession.builder \
    .appName("Local Test") \
    .master("local[*]") \
    .getOrCreate()

# Read your local CSV
df = spark.read.csv(
    "../data/raw/transactions.csv",
    header=True,
    inferSchema=True
)

print("=" * 60)
print("SPARK LOCAL TEST")
print("=" * 60)

# Show count
print(f"\nTotal records: {df.count():,}")

# Show schema
print("\nSchema:")
df.printSchema()

# Show sample
print("\nSample data:")
df.show(5)

# Simple aggregation
print("\nTransactions by status:")
df.groupBy('status').count().show()

print("\n✓ Spark is working!")

spark.stop()