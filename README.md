# E-Commerce Data Pipeline

A complete end-to-end data engineering project that simulates a production-grade e-commerce analytics platform.

## Project Overview

This project demonstrates a full data pipeline from data generation through analytics-ready models, showcasing modern data engineering tools and practices.

### Business Context

Simulates a growing e-commerce company that needs to:
- Track customer behavior and purchasing patterns
- Monitor product performance across categories
- Generate daily sales reports and analytics
- Support business intelligence and decision-making

## Architecture
```
Data Generation → AWS S3 → Spark Processing → Airflow Orchestration → dbt Transformation → Analytics
(Python)         (Storage)    (PySpark)         (Scheduling)        (Snowflake)      (BI Ready)
```

## Pipeline Stages

### Stage 1: Data Generation
- **Technology**: Python, Faker, Pandas
- **Output**: Realistic fake e-commerce data
  - 1,000 customers with demographics and segments
  - 100 products across 5 categories (Electronics, Clothing, Books, Sports, Home & Garden)
  - 10,000 transactions with realistic patterns
- **Files**: CSV format (customers, products, transactions)
- **Features**: 
  - Weighted transaction statuses (85% completed, 8% pending, 5% cancelled, 2% refunded)
  - Random discounts and promotions
  - Conditional shipping costs
  - Realistic product pricing and margins

### Stage 2: Cloud Storage 
- **Technology**: AWS S3, boto3
- **Achievement**: Data uploaded to cloud data lake
- **Bucket Structure**: 
```
  s3://ecommerce-data-pipeline-kausalya/
  ├── raw/
  │   ├── customers/
  │   │   └── customers.csv
  │   ├── products/
  │   │   └── products.csv
  │   └── transactions/
  │       └── transactions.csv
  ├── processed/
  └── staging/
```
- **Features**:
  - Automated upload with boto3 Python SDK
  - AWS CLI configured for command-line access
  - Proper IAM credentials and security
  - Data encrypted at rest (SSE-S3)
  
### Stage 3: Data Processing 
- **Technology**: Apache Spark (PySpark)
- **Processing Jobs**:
  - Data quality checks and validation
  - Deduplication and cleaning
  - Data enrichment (joining customers, products, transactions)
  - Aggregations (daily sales, customer lifetime value, product performance)
- **Output**: Processed data in S3 staging area

### Stage 4: Orchestration
- **Technology**: Apache Airflow
- **DAG Workflow**:
  1. Generate daily incremental data
  2. Upload to S3
  3. Trigger Spark processing jobs
  4. Run data quality checks
  5. Load to data warehouse
  6. Execute dbt models
  7. Send success/failure notifications
- **Schedule**: Daily at 2 AM UTC

### Stage 5: Data Warehouse & Transformation
- **Technology**: Snowflake + dbt
- **Data Models**:
  - **Staging Layer**: Raw data from S3
  - **Intermediate Layer**: Cleaned and joined data
  - **Marts Layer**: 
    - `fct_sales` - Sales fact table
    - `dim_customers` - Customer dimension
    - `dim_products` - Product dimension
    - `dim_date` - Date dimension
  - **Metrics Layer**:
    - Daily revenue by category
    - Customer cohort analysis
    - Product performance metrics
    - Customer lifetime value

### Stage 6: Analytics & Visualization
- **Technology**: Metabase / Tableau
- **Dashboards**:
  - Executive summary (revenue, orders, customers)
  - Sales performance by category/product
  - Customer segmentation analysis
  - Inventory and stock monitoring

## Tech Stack

### Current
- **Python 3.8+** - Data generation and scripting
- **Pandas** - Data manipulation
- **Faker** - Realistic fake data generation
- **boto3** - AWS SDK for Python
- **AWS S3** - Cloud data lake storage
- **AWS CLI** - Command-line S3 management
- **Git/GitHub** - Version control
- **Apache Spark** - Distributed data processing
- **Apache Airflow** - Workflow orchestration
- **Snowflake** - Cloud data warehouse
- **dbt (Data Build Tool)** - Analytics engineering
- **Metabase** - Data visualization

## Project Structure
```
ecommerce-data-pipeline/
├── data_generation/
│   └── generate_data.py           # Generates data + uploads to S3
├── spark_jobs/
│   ├── clean_transactions.py      # Data cleaning
│   ├── enrich_data.py             # Data enrichment
│   └── aggregate_metrics.py       # Aggregations
├── airflow/
│   └── dags/
│       └── ecommerce_pipeline.py  # Pipeline orchestration
├── dbt_project/
│   └── models/                    # dbt transformations
│       ├── staging/
│       ├── intermediate/
│       └── marts/
├── data/
│   └── raw/                       # Generated CSV files
├── docs/
│   ├── architecture.md            # Architecture decisions
│   └── data_dictionary.md         # Data documentation
├── venv/                          # Virtual environment
├── requirements.txt               # Python dependencies
└── README.md                      # This file

```
### How to Run (Data Generation)

1. **Clone the repository**
```bash
   git clone https://github.com/aquaamethyst/ecommerce-data-pipeline.git
   cd ecommerce-data-pipeline
```

2. **Set up virtual environment**
```bash
   python -m venv venv
   source venv/bin/activate  # Mac/Linux
   # or
   venv\Scripts\activate  # Windows
```

3. **Install dependencies**
```bash
   pip install -r requirements.txt
```

4. **Generate data**
```bash
   cd data_generation
   python generate_data.py
```

### Sample Output
```
==================================================
E-Commerce Data Generation Script
Generating 1000 customers...
Generating 100 products...
Generating 10000 transactions...
Saving files...
✓ Customers: ../data/raw/customers.csv
✓ Products: ../data/raw/products.csv
✓ Transactions: ../data/raw/transactions.csv
==================================================
Summary Statistics
Total Customers: 1,000
Total Products: 100
Total Transactions: 10,000
Total Revenue: $2,456,789.50
Average Order Value: $245.68
Transaction Status Breakdown:
completed    8,500
pending        800
cancelled      500
refunded       200
```

## AWS S3 Setup

### Prerequisites
- AWS account (free tier)
- AWS CLI installed
- AWS credentials configured

### Setup Instructions

1. **Install AWS CLI**
```bash
   # Mac
   brew install awscli
   
   # Windows
   # Download from https://aws.amazon.com/cli/
   
   # Linux
   curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
   unzip awscliv2.zip
   sudo ./aws/install
```

2. **Configure AWS credentials**
```bash
   aws configure
```
   
   Enter:
   - AWS Access Key ID
   - AWS Secret Access Key
   - Default region (e.g., `us-east-1`)
   - Default output format: `json`

3. **Install boto3**
```bash
   pip install boto3
   pip freeze > requirements.txt
```

4. **Update bucket name in script**
   
   Edit `data_generation/generate_data.py`:
```python
   BUCKET_NAME = 'your-bucket-name-here'
```

5. **Run script (generates data AND uploads to S3)**
```bash
   cd data_generation
   python generate_data.py
```

### Manual Upload (Optional)

Upload files manually using AWS CLI:
```bash
# Upload individual file
aws s3 cp data/raw/customers.csv s3://your-bucket/raw/customers/

# Sync entire directory
aws s3 sync data/raw/ s3://your-bucket/raw/
```

### Verify Upload

Check files in S3:
```bash
aws s3 ls s3://your-bucket/raw/ --recursive
```

Or visit AWS Console: https://s3.console.aws.amazon.com/
## Data Schema

### Customers
- `customer_id` (PK)
- `first_name`, `last_name`, `email`, `phone`
- `signup_date`, `country`, `city`, `state`, `zip_code`
- `customer_segment` (Premium, Standard, Basic)

### Products
- `product_id` (PK)
- `product_name`, `category`, `subcategory`
- `price`, `cost`, `stock_quantity`
- `supplier_id`, `weight_kg`, `created_date`

### Transactions
- `transaction_id` (PK)
- `customer_id` (FK), `product_id` (FK)
- `quantity`, `unit_price`, `total_amount`
- `discount_pct`, `payment_method`, `status`
- `transaction_date`, `shipping_cost`, `tax_amount`

## What I'm Learning

- Python programming fundamentals
- Working with Pandas DataFrames
- Data modeling and relationships
- Git version control and GitHub
- Creating realistic test data
- AWS cloud services (S3, IAM)
- Infrastructure as code
- Apache Spark and distributed computing
- Workflow orchestration with Airflow
- Analytics engineering with dbt
- Cloud data warehousing (Snowflake)
- Data visualization and BI tools

## Why This Project?

After taking a career break for family, I'm transitioning into data engineering. This project demonstrates:
- **End-to-end thinking**: Understanding the full data lifecycle
- **Modern tools**: Using industry-standard technologies
- **Real-world scenarios**: Solving actual business problems
- **Best practices**: Code quality, documentation, version control

## Future Enhancements

- Add real-time streaming with Kafka
- Implement data quality monitoring with Great Expectations
- Add CI/CD pipeline with GitHub Actions
- Create cost optimization analysis for AWS resources
- Build machine learning models for customer segmentation

## Author

Built by Kausalya Subramanian as part of my data engineering portfolio.

**Connect with me:**
- GitHub: [@aquaamethyst](https://github.com/aquaamethyst)
- LinkedIn: [https://www.linkedin.com/in/kausalyas/]
- Email: [diyas94@gmail.com]

## Acknowledgments

Building this project while learning data engineering concepts. Feedback and suggestions are welcome!

## License

This project is open source and available under the MIT License.
