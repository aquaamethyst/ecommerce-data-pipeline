"""
Enrich Data Spark Job
Joins transactions with customers and products
Calculates derived metrics
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, current_date, when, round as spark_round
import sys

# AWS S3 Configuration
BUCKET_NAME = "ecommerce-data-pipeline-kausalya"  

def create_spark_session():
    """Create Spark session with S3 configuration"""
    spark = SparkSession.builder \
        .appName("Enrich Data") \
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

def enrich_data(spark):
    """Enrich transaction data with customer and product info"""
    
    print("=" * 60)
    print("ENRICHING DATA")
    print("=" * 60)
    
    # Read cleaned transactions
    print(f"\n1. Reading cleaned transactions from S3...")
    transactions = spark.read.csv(
        f"s3a://{BUCKET_NAME}/processed/transactions/",
        header=True,
        inferSchema=True
    )
    print(f"   Transactions: {transactions.count():,}")
    
    # Read customers
    print(f"\n2. Reading customers from S3...")
    customers = spark.read.csv(
        f"s3a://{BUCKET_NAME}/raw/customers/",
        header=True,
        inferSchema=True
    )
    print(f"   Customers: {customers.count():,}")
    
    # Read products
    print(f"\n3. Reading products from S3...")
    products = spark.read.csv(
        f"s3a://{BUCKET_NAME}/raw/products/",
        header=True,
        inferSchema=True
    )
    print(f"   Products: {products.count():,}")
    
    # Join transactions with customers
    print(f"\n4. Joining transactions with customers...")
    enriched = transactions.join(
        customers,
        on='customer_id',
        how='left'
    )
    
    # Join with products
    print(f"\n5. Joining with products...")
    enriched = enriched.join(
        products,
        on='product_id',
        how='left'
    )
    
    # Calculate derived fields
    print(f"\n6. Calculating derived metrics...")
    
    # Customer lifetime (days since signup)
    enriched = enriched.withColumn(
        'customer_lifetime_days',
        datediff(col('transaction_date'), col('signup_date'))
    )
    
    # Profit per transaction
    enriched = enriched.withColumn(
        'profit',
        spark_round((col('unit_price') - col('cost')) * col('quantity'), 2)
    )
    
    # Profit margin percentage
    enriched = enriched.withColumn(
        'profit_margin_pct',
        spark_round((col('profit') / col('total_amount')) * 100, 2)
    )
    
    # Customer segment flag
    enriched = enriched.withColumn(
        'is_premium_customer',
        when(col('customer_segment') == 'Premium', True).otherwise(False)
    )
    
    # High value transaction flag
    enriched = enriched.withColumn(
        'is_high_value',
        when(col('total_amount') >= 500, True).otherwise(False)
    )
    
    # Select relevant columns
    enriched_final = enriched.select(
        # Transaction info
        'transaction_id',
        'transaction_date',
        'status',
        'quantity',
        'unit_price',
        'total_amount',
        'discount_pct',
        'shipping_cost',
        'tax_amount',
        'payment_method',
        
        # Customer info
        'customer_id',
        'first_name',
        'last_name',
        'email',
        'customer_segment',
        'country',
        'city',
        'signup_date',
        'customer_lifetime_days',
        'is_premium_customer',
        
        # Product info
        'product_id',
        'product_name',
        'category',
        'subcategory',
        'price',
        'cost',
        
        # Calculated metrics
        'profit',
        'profit_margin_pct',
        'is_high_value'
    )
    
    final_count = enriched_final.count()
    
    # Show summary
    print("\n" + "=" * 60)
    print("ENRICHMENT SUMMARY")
    print("=" * 60)
    print(f"Final enriched records: {final_count:,}")
    print(f"Columns: {len(enriched_final.columns)}")
    
    # Show sample
    print("\nSample enriched data:")
    enriched_final.show(5, truncate=False)
    
    # Show metrics
    print("\nProfit metrics:")
    enriched_final.select('total_amount', 'profit', 'profit_margin_pct') \
        .summary('count', 'mean', 'min', 'max') \
        .show()
    
    print("\nHigh-value transactions:")
    enriched_final.groupBy('is_high_value').count().show()
    
    print("\nTransactions by customer segment:")
    enriched_final.groupBy('customer_segment').count().orderBy('count', ascending=False).show()
    
    return enriched_final

def write_to_s3(df):
    """Write enriched data to S3"""
    
    output_path = f"s3a://{BUCKET_NAME}/processed/enriched/"
    print(f"\n7. Writing enriched data to {output_path}")
    
    df.write \
        .mode('overwrite') \
        .option('header', 'true') \
        .csv(output_path)
    
    print("✓ Enriched data written successfully!")

def main():
    """Main execution"""
    
    try:
        spark = create_spark_session()
        enriched_df = enrich_data(spark)
        write_to_s3(enriched_df)
        
        print("\n" + "=" * 60)
        print("ENRICHMENT JOB COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        
        spark.stop()
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()