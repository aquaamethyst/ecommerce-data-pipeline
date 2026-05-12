"""
Clean transcations spark job
Removes duplicates, validates data, handles nulls
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when,trim,upper
from pyspark.sql.types import StringType,DoubleType,IntegerType
import sys

# AWS S3 Configuration

BUCKET_NAME="ecommerce-data-pipeline-kausalya"
INPUT_PATH= f"s3a://{BUCKET_NAME}/raw/transactions/"
OUTPUT_PATH=f"s3a://{BUCKET_NAME}/processed/transactions/"

def create_spark_session():
    """Create Spark session with S3 configuration"""
    spark = SparkSession.builder \
        .appName("Clean Transactions") \
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

def clean_transactions(spark):
    """ Clean and validate transaction data"""
    print("=" * 60)
    print("CLEANING TRANSACTIONS")
    print("=" * 60)

    # Read data from S3
    print(f"\n1. Reading data from {INPUT_PATH}")
    df= spark.read.csv(INPUT_PATH,header= True, inferSchema=True)

    initial_count= df.count()
    print(f"Initial record count: {initial_count:,}")

    #Show schema
    print("\n2. Data Schema: ")
    df.printSchema()

    # Remove duplicates
    print("\n3. Removing duplicates...")
    df_deduped= df.dropDuplicates(['transaction_id'])
    duplicates_removed = initial_count - df_deduped.count()
    print(f"Duplicates removed: {duplicates_removed:,}")

    # Remove records with null critical fields
    print("\n4. Removing records with null critical fields...")
    df_clean=df_deduped.dropna(subset=['transaction_id','product_id','customer_id'])
    nulls_removed = df_deduped.count() - df_clean.count()
    print(f"Null records removed: {nulls_removed:,}")

     # Validate and fix data types
    print("\n5. Validating data quality...")
    
    # Remove negative prices/quantities
    df_validated=df_clean.filter(
        (col('unit_price')>0) &
        (col('quantity')>0) &
        (col('total_amount')>=0)
    )

    invalid_removed = df_clean.count() - df_validated.count()
    print(f"   Invalid records removed: {invalid_removed:,}")

    # Standardize status values
    df_final = df_validated.withColumn(
        'status',
        trim(upper(col('status')))
    )

    # Add data quality flag
    df_final = df_final.withColumn(
        'data_quality',
        when(
            (col('unit_price') == col('total_amount') / col('quantity')),
            'VALID'
        ).otherwise('CHECK_NEEDED')
    )
    final_count = df_final.count()
    
    # Show summary
    print("\n" + "=" * 60)
    print("CLEANING SUMMARY")
    print("=" * 60)
    print(f"Initial records:     {initial_count:,}")
    print(f"Duplicates removed:  {duplicates_removed:,}")
    print(f"Nulls removed:       {nulls_removed:,}")
    print(f"Invalid removed:     {invalid_removed:,}")
    print(f"Final records:       {final_count:,}")
    print(f"Data quality:        {(final_count/initial_count)*100:.1f}% retained")
    print("=" * 60)
    
    # Show sample
    print("\nSample of cleaned data:")
    df_final.show(5, truncate=False)
    
    # Show status distribution
    print("\nStatus distribution:")
    df_final.groupBy('status').count().orderBy('count', ascending=False).show()
    
    return df_final

def write_to_s3(df):
    """Write cleaned data back to S3"""
    
    print(f"\n6. Writing cleaned data to {OUTPUT_PATH}")
    
    df.write \
        .mode('overwrite') \
        .option('header', 'true') \
        .csv(OUTPUT_PATH)
    
    print("✓ Data written successfully!")

def main():
    """Main execution"""
    
    try:
        # Create Spark session
        spark = create_spark_session()
        
        # Clean transactions
        df_clean = clean_transactions(spark)
        
        # Write to S3
        write_to_s3(df_clean)
        
        print("\n" + "=" * 60)
        print("JOB COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        
        # Stop Spark
        spark.stop()
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()



