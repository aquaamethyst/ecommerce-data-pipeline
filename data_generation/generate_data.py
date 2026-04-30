"""
E-commerce data generator
Generates realistic fake data for customers, products and transactions
"""
from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta
import os
import boto3
from botocore.exceptions import NoCredentialsError


# initialize faker

fake=Faker()
Faker.seed(42) # for reproducability
random.seed(42)

#Configuration

NUM_CUSTOMERS=1000
NUM_PRODUCTS=100
NUM_TRANSACTIONS=10000
OUTPUT_DIR='../data/raw'

#create output dir if it doesn't exist

os.makedirs(OUTPUT_DIR, exist_ok=True)

def generate_customers(n):
    """Generate fake customer data"""
    print(f"Generating {n} customers")

    customers=[]
    customer_segments=['Premium','Standard','Basic']

    for i in range(n):
        customer={
            'customer_id': f'CUST{i:05d}',
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email':fake.email(),
            'phone': fake.phone_number(),
            'signup_date': fake.date_between(start_date='-2y', end_date='today'),
            'country':fake.country(),
            'city':fake.city(),
            'state':fake.state(),
            'zip_code': fake.zipcode(),
            'customer_segment': random.choice(customer_segments)

        }
        customers.append(customer)

    return pd.DataFrame(customers)
    

def generate_products(n):
    """Generate products with subcategory-specific constraints"""
    
    # Ultra-realistic specs per subcategory
    product_specs = {
        'Laptop': {'price': (500, 2500), 'weight': (1.2, 2.5), 'margin': 0.3},
        'Phone': {'price': (200, 1500), 'weight': (0.15, 0.25), 'margin': 0.35},
        'Tablet': {'price': (150, 1000), 'weight': (0.3, 0.7), 'margin': 0.35},
        'Headphones': {'price': (20, 400), 'weight': (0.1, 0.5), 'margin': 0.5},
        
        'Shirt': {'price': (15, 100), 'weight': (0.2, 0.4), 'margin': 0.6},
        'Pants': {'price': (30, 150), 'weight': (0.4, 0.7), 'margin': 0.6},
        'Shoes': {'price': (40, 250), 'weight': (0.5, 1.2), 'margin': 0.55},
        
        'Fiction': {'price': (10, 25), 'weight': (0.3, 0.8), 'margin': 0.35},
        'Educational': {'price': (30, 150), 'weight': (0.8, 2.5), 'margin': 0.3},
       
    }
    
    products = []
    categories = {
        'Electronics': ['Laptop', 'Phone', 'Tablet', 'Headphones'],
        'Clothing': ['Shirt', 'Pants', 'Shoes'],
        'Books': ['Fiction', 'Educational']
    }
    
    for i in range(n):
        category = random.choice(list(categories.keys()))
        subcategory = random.choice(categories[category])
        
        # Get specs for this SPECIFIC subcategory
        specs = product_specs[subcategory]
        
        price_min, price_max = specs['price']
        price = round(random.uniform(price_min, price_max), 2)
        
        cost = round(price / (1 + specs['margin']), 2)
        
        weight_min, weight_max = specs['weight']
        weight = round(random.uniform(weight_min, weight_max), 2)
        
        product = {
            'product_id': f'PROD{i:04d}',
            'product_name': f'{fake.word().title()} {subcategory}',
            'category': category,
            'subcategory': subcategory,
            'price': price,
            'cost': cost,
            'stock_quantity': random.randint(0, 500),
            'supplier_id': f'SUP{random.randint(1, 20):03d}',
            'weight_kg': weight,
            'created_date': fake.date_between(start_date='-3y', end_date='-1y')
        }
        products.append(product)
    
    return pd.DataFrame(products)

def generate_transactions(n, customers_df,products_df):
    """Generate fake transaction data"""
    transactions = []
    payment_methods = ['credit_card', 'debit_card', 'paypal', 'bank_transfer', 'apple_pay']
    statuses = ['completed', 'pending', 'cancelled', 'refunded']
    # Weight towards completed transactions
    status_weights = [0.85, 0.08, 0.05, 0.02]

    customer_ids= customers_df['customer_id'].tolist()
    product_ids=products_df['product_id'].tolist()
    product_prices=dict(zip(products_df['product_id'], products_df['price']))

    for i in range(n):
        customer_id= random.choice(customer_ids)
        product_id=random.choice(product_ids)
        quantity=random.randint(1,5)
        base_price= product_prices[product_id]

        # Add some random discount
        discount_pct = random.choice([0, 0, 0, 0.05, 0.10, 0.15, 0.20])
        final_price =round(base_price *(1-discount_pct),2)

        transaction={
            'transaction_id':f'TXN{i:07d}',
            'customer_id':customer_id,
            'product_id': product_id,
            'quantity': quantity,
            'unit_price': final_price,
            'total_amount': round(final_price * quantity,2),
            'discount_pct':discount_pct,
            'payment_method':random.choice(payment_methods),
            'status': random.choices(statuses, weights=status_weights)[0],
            'transaction_date': fake.date_time_between(start_date='-1y', end_date='now'),
            'shipping_cost': round(random.uniform(0, 20), 2) if random.random() > 0.3 else 0,
            'tax_amount': round(final_price * quantity * 0.08, 2)  # 8% tax
        }
        transactions.append(transaction)

    return pd.DataFrame(transactions)


def upload_to_s3(local_file, bucket_name, s3_path):
    """Upload a file to S3"""
    s3_client = boto3.client('s3')
    
    try:
        s3_client.upload_file(local_file, bucket_name, s3_path)
        print(f"✓ Uploaded to S3: s3://{bucket_name}/{s3_path}")
        return True
    except FileNotFoundError:
        print(f"✗ File not found: {local_file}")
        return False
    except NoCredentialsError:
        print("✗ AWS credentials not found. Run 'aws configure'")
        return False



def main():
    """Main execution function"""
    print("=" * 50)
    print("E-Commerce Data Generation Script")
    print("=" * 50)
    print()
    
    # Generate data
    customers_df = generate_customers(NUM_CUSTOMERS)
    products_df = generate_products(NUM_PRODUCTS)
    transactions_df = generate_transactions(NUM_TRANSACTIONS, customers_df, products_df)
    
    # Sort transactions by date
    transactions_df = transactions_df.sort_values('transaction_date').reset_index(drop=True)
    
    # Save to CSV
    print()
    print("Saving files...")
    
    customers_file = f'{OUTPUT_DIR}/customers.csv'
    products_file = f'{OUTPUT_DIR}/products.csv'
    transactions_file = f'{OUTPUT_DIR}/transactions.csv'
    
    customers_df.to_csv(customers_file, index=False)
    products_df.to_csv(products_file, index=False)
    transactions_df.to_csv(transactions_file, index=False)
    
    print(f"✓ Customers: {customers_file}")
    print(f"✓ Products: {products_file}")
    print(f"✓ Transactions: {transactions_file}")

    print()
    print("Uploading to S3...")
    
    BUCKET_NAME = 'ecommerce-data-pipeline-kausalya'  # Change to your bucket!
    
    upload_to_s3(customers_file, BUCKET_NAME, 'raw/customers/customers.csv')
    upload_to_s3(products_file, BUCKET_NAME, 'raw/products/products.csv')
    upload_to_s3(transactions_file, BUCKET_NAME, 'raw/transactions/transactions.csv')

    
    # Print summary statistics
    print()
    print("=" * 50)
    print("Summary Statistics")
    print("=" * 50)
    print(f"Total Customers: {len(customers_df):,}")
    print(f"Total Products: {len(products_df):,}")
    print(f"Total Transactions: {len(transactions_df):,}")
    print()
    print(f"Date Range: {transactions_df['transaction_date'].min()} to {transactions_df['transaction_date'].max()}")
    print(f"Total Revenue: ${transactions_df['total_amount'].sum():,.2f}")
    print(f"Average Order Value: ${transactions_df['total_amount'].mean():,.2f}")
    print()
    print("Transaction Status Breakdown:")
    print(transactions_df['status'].value_counts())
    print()
    print("Top 5 Product Categories:")
    category_sales = transactions_df.merge(products_df[['product_id', 'category']], on='product_id')
    print(category_sales['category'].value_counts().head())
    print()
    print("=" * 50)
    print("Data generation complete! ✓")
    print("=" * 50)

if __name__ == "__main__":
    main()