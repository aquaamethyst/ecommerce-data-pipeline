"""
Aggregate Metrics Spark Job
Creates summary tables for analytics
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, avg, count, max as spark_max, min as spark_min, round as spark_round
import sys

BUCKET_NAME = "ecommerce-data-pipeline-kausalya"  # CHANGE THIS!

def create_spark_session():
    """Create Spark session"""
    spark = SparkSession.builder \
        .appName("Aggregate Metrics") \
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
    
    return spark

def aggregate_metrics(spark):
    """Create aggregated summary tables"""
    
    print("=" * 60)
    print("AGGREGATING METRICS")
    print("=" * 60)
    
    # Read enriched data
    print(f"\n1. Reading enriched data from S3...")
    enriched = spark.read.csv(
        f"s3a://{BUCKET_NAME}/processed/enriched/",
        header=True,
        inferSchema=True
    )
    print(f"   Records: {enriched.count():,}")
    
    # Aggregate 1: Daily sales summary
    print(f"\n2. Calculating daily sales...")
    daily_sales = enriched.groupBy('transaction_date') \
        .agg(
            count('transaction_id').alias('transaction_count'),
            spark_sum('total_amount').alias('total_revenue'),
            spark_sum('profit').alias('total_profit'),
            avg('total_amount').alias('avg_order_value'),
            count(col('customer_id')).alias('unique_customers')
        ) \
        .orderBy('transaction_date')
    
    print(f"   Daily periods: {daily_sales.count()}")
    daily_sales.show(10)
    
    # Aggregate 2: Category performance
    print(f"\n3. Calculating category performance...")
    category_perf = enriched.groupBy('category') \
        .agg(
            count('transaction_id').alias('transaction_count'),
            spark_sum('total_amount').alias('total_revenue'),
            spark_sum('profit').alias('total_profit'),
            avg('profit_margin_pct').alias('avg_profit_margin'),
            avg('total_amount').alias('avg_transaction_value')
        ) \
        .orderBy(col('total_revenue').desc())
    
    category_perf.show()
    
    # Aggregate 3: Customer lifetime value
    print(f"\n4. Calculating customer lifetime value...")
    customer_ltv = enriched.groupBy('customer_id', 'customer_segment') \
        .agg(
            count('transaction_id').alias('total_transactions'),
            spark_sum('total_amount').alias('lifetime_value'),
            spark_sum('profit').alias('lifetime_profit'),
            avg('total_amount').alias('avg_order_value'),
            spark_max('transaction_date').alias('last_purchase_date')
        ) \
        .orderBy(col('lifetime_value').desc())
    
    print(f"   Customers analyzed: {customer_ltv.count():,}")
    customer_ltv.show(10)
    
    # Aggregate 4: Product performance
    print(f"\n5. Calculating product performance...")
    product_perf = enriched.groupBy('product_id', 'product_name', 'category') \
        .agg(
            count('transaction_id').alias('times_sold'),
            spark_sum('quantity').alias('total_quantity_sold'),
            spark_sum('total_amount').alias('total_revenue'),
            spark_sum('profit').alias('total_profit')
        ) \
        .orderBy(col('total_revenue').desc())
    
    product_perf.show(10)
    
    return {
        'daily_sales': daily_sales,
        'category_perf': category_perf,
        'customer_ltv': customer_ltv,
        'product_perf': product_perf
    }

def write_aggregates(aggregates):
    """Write all aggregate tables to S3"""
    
    print(f"\n6. Writing aggregate tables to S3...")
    
    for name, df in aggregates.items():
        path = f"s3a://{BUCKET_NAME}/processed/aggregates/{name}/"
        print(f"   Writing {name}...")
        
        df.write \
            .mode('overwrite') \
            .option('header', 'true') \
            .csv(path)
    
    print("✓ All aggregates written successfully!")

def main():
    """Main execution"""
    
    try:
        spark = create_spark_session()
        aggregates = aggregate_metrics(spark)
        write_aggregates(aggregates)
        
        print("\n" + "=" * 60)
        print("AGGREGATION JOB COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        
        spark.stop()
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()