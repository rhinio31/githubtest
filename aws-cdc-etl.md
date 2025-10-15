# AWS Iceberg CDC Pipeline - Complete Implementation Guide

## Prerequisites
- AWS Account with appropriate permissions
- AWS CLI configured
- Python 3.8+ installed locally
- Basic knowledge of AWS services (S3, Glue, Athena)

---

## Step 1: Set Up S3 Bucket Structure

### 1.1 Create S3 Buckets

```bash
# Set your variables
BUCKET_NAME="your-company-data-lake"
AWS_REGION="us-east-1"

# Create main bucket
aws s3 mb s3://${BUCKET_NAME} --region ${AWS_REGION}

# Create folder structure
aws s3api put-object --bucket ${BUCKET_NAME} --key raw/
aws s3api put-object --bucket ${BUCKET_NAME} --key iceberg/
aws s3api put-object --bucket ${BUCKET_NAME} --key scripts/
aws s3api put-object --bucket ${BUCKET_NAME} --key logs/
```

### 1.2 S3 Folder Structure
```
s3://your-company-data-lake/
├── raw/                          # Landing zone for extracted data
│   ├── customers/
│   │   ├── full_load/
│   │   │   └── customers_20241015.parquet
│   │   └── incremental/
│   │       ├── 2024-10-15-14/
│   │       │   └── customers_delta.parquet
│   │       └── 2024-10-15-15/
│   ├── orders/
│   │   ├── full_load/
│   │   └── incremental/
│   └── products/
├── iceberg/                      # Iceberg table storage
│   ├── customers/
│   │   ├── data/
│   │   └── metadata/
│   ├── orders/
│   └── products/
├── scripts/                      # Glue job scripts
│   ├── full_load_to_iceberg.py
│   └── incremental_cdc_to_iceberg.py
└── logs/                         # Processing logs
```

---

## Step 2: Create IAM Roles and Policies

### 2.1 Create Glue Service Role

```bash
# Create trust policy file
cat > glue-trust-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "glue.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

# Create IAM role
aws iam create-role \
  --role-name GlueIcebergCDCRole \
  --assume-role-policy-document file://glue-trust-policy.json

# Attach AWS managed policies
aws iam attach-role-policy \
  --role-name GlueIcebergCDCRole \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

# Create custom S3 policy
cat > glue-s3-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::${BUCKET_NAME}",
        "arn:aws:s3:::${BUCKET_NAME}/*"
      ]
    }
  ]
}
EOF

aws iam put-role-policy \
  --role-name GlueIcebergCDCRole \
  --policy-name GlueS3Access \
  --policy-document file://glue-s3-policy.json
```

### 2.2 Get Role ARN (save this for later)
```bash
aws iam get-role --role-name GlueIcebergCDCRole --query 'Role.Arn' --output text
```

---

## Step 3: Create Glue Database

```bash
# Create Glue database
aws glue create-database \
  --database-input '{
    "Name": "iceberg_cdc_db",
    "Description": "Database for Iceberg tables with CDC"
  }'

# Verify
aws glue get-database --name iceberg_cdc_db
```

---

## Step 4: Create Python Extraction Script (Enhanced)

### 4.1 Update Your Existing Python Script

```python
# data_extractor.py
import boto3
import pandas as pd
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataExtractor:
    def __init__(self, bucket_name, aws_region='us-east-1'):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3', region_name=aws_region)
        
    def extract_full_load(self, table_name, data_source_connection):
        """
        Extract full table and upload to S3
        """
        logger.info(f"Starting full load for {table_name}")
        
        # Your existing extraction logic here
        # Example:
        df = pd.read_sql(f"SELECT * FROM {table_name}", data_source_connection)
        
        # Add metadata column
        df['header_operation'] = 'INSERT'
        df['extracted_at'] = datetime.now()
        
        # Convert to Parquet
        table = pa.Table.from_pandas(df)
        
        # Upload to S3
        s3_key = f"raw/{table_name}/full_load/{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        
        # Write to buffer
        import io
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression='snappy')
        buffer.seek(0)
        
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=s3_key,
            Body=buffer.getvalue()
        )
        
        logger.info(f"Full load completed: s3://{self.bucket_name}/{s3_key}")
        return s3_key
    
    def extract_incremental(self, table_name, data_source_connection, last_extract_time):
        """
        Extract incremental changes (INSERT, UPDATE, DELETE)
        """
        logger.info(f"Starting incremental load for {table_name}")
        
        # Your CDC query - adjust based on your source database
        # This example assumes CDC table or audit columns
        query = f"""
        SELECT *, 
               CASE 
                   WHEN operation_type = 'I' THEN 'INSERT'
                   WHEN operation_type = 'U' THEN 'UPDATE'
                   WHEN operation_type = 'D' THEN 'DELETE'
               END as header_operation
        FROM {table_name}_cdc
        WHERE change_timestamp > '{last_extract_time}'
        """
        
        df = pd.read_sql(query, data_source_connection)
        
        if df.empty:
            logger.info(f"No changes found for {table_name}")
            return None
        
        df['extracted_at'] = datetime.now()
        
        # Convert to Parquet
        table = pa.Table.from_pandas(df)
        
        # Upload to S3 with timestamp partition
        timestamp_partition = datetime.now().strftime('%Y-%m-%d-%H')
        s3_key = f"raw/{table_name}/incremental/{timestamp_partition}/{table_name}_delta_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        
        import io
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression='snappy')
        buffer.seek(0)
        
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=s3_key,
            Body=buffer.getvalue()
        )
        
        logger.info(f"Incremental load completed: s3://{self.bucket_name}/{s3_key}")
        logger.info(f"Records processed: {len(df)}")
        
        return s3_key

# Usage example
if __name__ == "__main__":
    extractor = DataExtractor(bucket_name="your-company-data-lake")
    
    # Your database connection
    # connection = create_your_db_connection()
    
    # Full load (initial)
    # extractor.extract_full_load("customers", connection)
    
    # Incremental (hourly)
    # last_time = get_last_extract_time()
    # extractor.extract_incremental("customers", connection, last_time)
```

---

## Step 5: Create Glue Job Scripts

### 5.1 Full Load to Iceberg Script

```python
# full_load_to_iceberg.py
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
import logging

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'database_name',
    'table_name',
    's3_source_path',
    'iceberg_warehouse_path'
])

# Initialize Spark with Iceberg configuration
spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", args['iceberg_warehouse_path']) \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .getOrCreate()

sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger.info(f"Starting full load for table: {args['table_name']}")

# Read source data from S3
logger.info(f"Reading from: {args['s3_source_path']}")
df_source = spark.read.parquet(args['s3_source_path'])

logger.info(f"Source records count: {df_source.count()}")

# Add processing metadata
df_processed = df_source \
    .withColumn("_loaded_at", current_timestamp()) \
    .withColumn("_load_type", lit("FULL"))

# Drop header_operation if exists (not needed in final table)
if "header_operation" in df_processed.columns:
    df_processed = df_processed.drop("header_operation")

# Create Iceberg table
table_identifier = f"glue_catalog.{args['database_name']}.{args['table_name']}"

logger.info(f"Creating Iceberg table: {table_identifier}")

df_processed.writeTo(table_identifier) \
    .using("iceberg") \
    .tableProperty("format-version", "2") \
    .tableProperty("write.parquet.compression-codec", "snappy") \
    .tableProperty("write.metadata.compression-codec", "gzip") \
    .createOrReplace()

final_count = spark.table(table_identifier).count()
logger.info(f"Iceberg table created with {final_count} records")

job.commit()
logger.info("Job completed successfully")
```

### 5.2 Incremental CDC to Iceberg Script

```python
# incremental_cdc_to_iceberg.py
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'database_name',
    'table_name',
    's3_incremental_path',
    'primary_keys',  # comma-separated list: "id" or "id,version"
    'iceberg_warehouse_path'
])

# Parse primary keys
primary_keys = args['primary_keys'].split(',')
logger.info(f"Primary keys: {primary_keys}")

# Initialize Spark with Iceberg
spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", args['iceberg_warehouse_path']) \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .getOrCreate()

sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger.info(f"Starting incremental CDC processing for: {args['table_name']}")

# Read incremental data
logger.info(f"Reading incremental data from: {args['s3_incremental_path']}")
df_cdc = spark.read.parquet(args['s3_incremental_path'])

record_count = df_cdc.count()
logger.info(f"CDC records to process: {record_count}")

if record_count == 0:
    logger.info("No records to process. Exiting.")
    job.commit()
    sys.exit(0)

# Show operation breakdown
df_cdc.groupBy("header_operation").count().show()

# Add processing metadata
df_cdc = df_cdc.withColumn("_processed_at", current_timestamp())

# Create temp view for MERGE
df_cdc.createOrReplaceTempView("cdc_changes")

table_identifier = f"glue_catalog.{args['database_name']}.{args['table_name']}"

# Build JOIN condition for primary keys
join_condition = " AND ".join([f"target.{pk} = source.{pk}" for pk in primary_keys])

# Get all columns except metadata columns
all_columns = [c for c in df_cdc.columns if c not in ['header_operation', 'extracted_at', '_processed_at']]
update_set = ", ".join([f"target.{col} = source.{col}" for col in all_columns])

# MERGE query with all CDC operations
merge_query = f"""
MERGE INTO {table_identifier} AS target
USING cdc_changes AS source
ON {join_condition}

WHEN MATCHED AND source.header_operation = 'DELETE' THEN
  DELETE

WHEN MATCHED AND source.header_operation = 'UPDATE' THEN
  UPDATE SET {update_set}, target._processed_at = source._processed_at

WHEN NOT MATCHED AND source.header_operation IN ('INSERT', 'UPDATE') THEN
  INSERT *
"""

logger.info(f"Executing MERGE query")
logger.info(f"MERGE conditions: {join_condition}")

spark.sql(merge_query)

# Get statistics
final_count = spark.table(table_identifier).count()
logger.info(f"Table now contains {final_count} records")

# Optional: Show recent changes
logger.info("Sample of recent changes:")
spark.sql(f"""
    SELECT * FROM {table_identifier}
    ORDER BY _processed_at DESC
    LIMIT 10
""").show(truncate=False)

job.commit()
logger.info("CDC processing completed successfully")
```

### 5.3 Upload Scripts to S3

```bash
# Upload both scripts
aws s3 cp full_load_to_iceberg.py s3://${BUCKET_NAME}/scripts/
aws s3 cp incremental_cdc_to_iceberg.py s3://${BUCKET_NAME}/scripts/
```

---

## Step 6: Create Glue Jobs

### 6.1 Create Full Load Glue Job

```bash
# Get your IAM role ARN
ROLE_ARN=$(aws iam get-role --role-name GlueIcebergCDCRole --query 'Role.Arn' --output text)

# Create full load job
aws glue create-job \
  --name iceberg-full-load-customers \
  --role ${ROLE_ARN} \
  --command '{
    "Name": "glueetl",
    "ScriptLocation": "s3://'${BUCKET_NAME}'/scripts/full_load_to_iceberg.py",
    "PythonVersion": "3"
  }' \
  --default-arguments '{
    "--job-language": "python",
    "--enable-metrics": "true",
    "--enable-spark-ui": "true",
    "--spark-event-logs-path": "s3://'${BUCKET_NAME}'/logs/spark-logs/",
    "--enable-job-insights": "true",
    "--enable-glue-datacatalog": "true",
    "--enable-continuous-cloudwatch-log": "true",
    "--datalake-formats": "iceberg",
    "--conf": "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
    "--database_name": "iceberg_cdc_db",
    "--table_name": "customers",
    "--s3_source_path": "s3://'${BUCKET_NAME}'/raw/customers/full_load/",
    "--iceberg_warehouse_path": "s3://'${BUCKET_NAME}'/iceberg/"
  }' \
  --glue-version "4.0" \
  --worker-type "G.1X" \
  --number-of-workers 2 \
  --timeout 60 \
  --max-retries 0
```

### 6.2 Create Incremental CDC Glue Job

```bash
aws glue create-job \
  --name iceberg-incremental-cdc-customers \
  --role ${ROLE_ARN} \
  --command '{
    "Name": "glueetl",
    "ScriptLocation": "s3://'${BUCKET_NAME}'/scripts/incremental_cdc_to_iceberg.py",
    "PythonVersion": "3"
  }' \
  --default-arguments '{
    "--job-language": "python",
    "--enable-metrics": "true",
    "--enable-spark-ui": "true",
    "--spark-event-logs-path": "s3://'${BUCKET_NAME}'/logs/spark-logs/",
    "--enable-job-insights": "true",
    "--enable-glue-datacatalog": "true",
    "--enable-continuous-cloudwatch-log": "true",
    "--datalake-formats": "iceberg",
    "--conf": "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
    "--database_name": "iceberg_cdc_db",
    "--table_name": "customers",
    "--s3_incremental_path": "s3://'${BUCKET_NAME}'/raw/customers/incremental/",
    "--primary_keys": "customer_id",
    "--iceberg_warehouse_path": "s3://'${BUCKET_NAME}'/iceberg/"
  }' \
  --glue-version "4.0" \
  --worker-type "G.1X" \
  --number-of-workers 2 \
  --timeout 60 \
  --max-retries 1
```

---

## Step 7: Set Up EventBridge (CloudWatch Events) for Automation

### 7.1 Create EventBridge Rule for Incremental Processing

```bash
# Create rule for hourly incremental processing
aws events put-rule \
  --name trigger-iceberg-incremental-hourly \
  --description "Trigger Iceberg CDC processing every hour" \
  --schedule-expression "rate(1 hour)"

# Add Glue job as target
aws events put-targets \
  --rule trigger-iceberg-incremental-hourly \
  --targets '[
    {
      "Id": "1",
      "Arn": "arn:aws:glue:'${AWS_REGION}':'$(aws sts get-caller-identity --query Account --output text)':job/iceberg-incremental-cdc-customers",
      "RoleArn": "'${ROLE_ARN}'"
    }
  ]'
```

### 7.2 Alternative: S3 Event Notification (Recommended)

Create Lambda function to trigger Glue job when new files arrive:

```python
# lambda_trigger_glue.py
import json
import boto3
import os
from urllib.parse import unquote_plus

glue_client = boto3.client('glue')

def lambda_handler(event, context):
    """
    Triggered by S3 event when new file is uploaded to incremental folder
    """
    
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        
        print(f"New file detected: s3://{bucket}/{key}")
        
        # Extract table name from path
        # Expected: raw/customers/incremental/2024-10-15-14/file.parquet
        parts = key.split('/')
        if len(parts) >= 3 and parts[0] == 'raw' and parts[2] == 'incremental':
            table_name = parts[1]
            
            # Trigger appropriate Glue job
            job_name = f"iceberg-incremental-cdc-{table_name}"
            
            try:
                response = glue_client.start_job_run(
                    JobName=job_name,
                    Arguments={
                        '--s3_incremental_path': f"s3://{bucket}/raw/{table_name}/incremental/"
                    }
                )
                
                print(f"Started Glue job {job_name}, Run ID: {response['JobRunId']}")
                
            except Exception as e:
                print(f"Error starting Glue job: {str(e)}")
                raise
    
    return {
        'statusCode': 200,
        'body': json.dumps('Processing complete')
    }
```

Deploy Lambda:

```bash
# Create Lambda execution role
aws iam create-role \
  --role-name LambdaTriggerGlueRole \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"Service": "lambda.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }]
  }'

# Attach policies
aws iam attach-role-policy \
  --role-name LambdaTriggerGlueRole \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

aws iam put-role-policy \
  --role-name LambdaTriggerGlueRole \
  --policy-name GlueStartJobAccess \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Action": ["glue:StartJobRun"],
      "Resource": "*"
    }]
  }'

# Create Lambda function (after creating deployment package)
# Zip the lambda function
zip lambda_trigger_glue.zip lambda_trigger_glue.py

LAMBDA_ROLE_ARN=$(aws iam get-role --role-name LambdaTriggerGlueRole --query 'Role.Arn' --output text)

aws lambda create-function \
  --function-name TriggerIcebergCDC \
  --runtime python3.11 \
  --role ${LAMBDA_ROLE_ARN} \
  --handler lambda_trigger_glue.lambda_handler \
  --zip-file fileb://lambda_trigger_glue.zip \
  --timeout 30

# Add S3 trigger permission
aws lambda add-permission \
  --function-name TriggerIcebergCDC \
  --statement-id s3-trigger \
  --action lambda:InvokeFunction \
  --principal s3.amazonaws.com \
  --source-arn arn:aws:s3:::${BUCKET_NAME}

# Configure S3 notification
aws s3api put-bucket-notification-configuration \
  --bucket ${BUCKET_NAME} \
  --notification-configuration '{
    "LambdaFunctionConfigurations": [{
      "LambdaFunctionArn": "arn:aws:lambda:'${AWS_REGION}':'$(aws sts get-caller-identity --query Account --output text)':function:TriggerIcebergCDC",
      "Events": ["s3:ObjectCreated:*"],
      "Filter": {
        "Key": {
          "FilterRules": [
            {"Name": "prefix", "Value": "raw/"},
            {"Name": "suffix", "Value": ".parquet"}
          ]
        }
      }
    }]
  }'
```

---

## Step 8: Testing the Pipeline

### 8.1 Prepare Test Data

```python
# create_test_data.py
import pandas as pd
import boto3
from datetime import datetime
import io

s3_client = boto3.client('s3')
bucket_name = "your-company-data-lake"

# Create sample full load data
customers_full = pd.DataFrame({
    'customer_id': [1, 2, 3, 4, 5],
    'name': ['Alice Smith', 'Bob Jones', 'Carol White', 'David Brown', 'Eve Davis'],
    'email': ['alice@email.com', 'bob@email.com', 'carol@email.com', 'david@email.com', 'eve@email.com'],
    'country': ['USA', 'UK', 'Canada', 'USA', 'Australia'],
    'created_date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05'],
    'header_operation': 'INSERT'
})

# Save full load to S3
buffer = io.BytesIO()
customers_full.to_parquet(buffer, index=False, compression='snappy')
buffer.seek(0)

s3_client.put_object(
    Bucket=bucket_name,
    Key=f"raw/customers/full_load/customers_{datetime.now().strftime('%Y%m%d')}.parquet",
    Body=buffer.getvalue()
)

print("Full load test data created")

# Create incremental CDC data
customers_incremental = pd.DataFrame({
    'customer_id': [2, 3, 6],
    'name': ['Bob Jones Updated', 'Carol White', 'Frank Miller'],  # Bob updated
    'email': ['bob.jones@newemail.com', 'carol@email.com', 'frank@email.com'],
    'country': ['UK', 'Canada', 'Germany'],
    'created_date': ['2024-01-02', '2024-01-03', '2024-10-15'],
    'header_operation': ['UPDATE', 'DELETE', 'INSERT']  # Update Bob, Delete Carol, Insert Frank
})

timestamp = datetime.now().strftime('%Y-%m-%d-%H')
buffer = io.BytesIO()
customers_incremental.to_parquet(buffer, index=False, compression='snappy')
buffer.seek(0)

s3_client.put_object(
    Bucket=bucket_name,
    Key=f"raw/customers/incremental/{timestamp}/customers_delta.parquet",
    Body=buffer.getvalue()
)

print("Incremental CDC test data created")
```

Run the test data creation:
```bash
python create_test_data.py
```

### 8.2 Run Full Load Job

```bash
# Start full load job
aws glue start-job-run --job-name iceberg-full-load-customers

# Monitor job
aws glue get-job-runs --job-name iceberg-full-load-customers --max-results 1
```

### 8.3 Run Incremental CDC Job

```bash
# Start incremental job
aws glue start-job-run --job-name iceberg-incremental-cdc-customers

# Monitor job
aws glue get-job-runs --job-name iceberg-incremental-cdc-customers --max-results 1
```

---

## Step 9: Query with Athena

### 9.1 Configure Athena

```bash
# Create Athena workgroup with output location
aws athena create-work-group \
  --name iceberg-workgroup \
  --configuration '{
    "ResultConfigurationUpdates": {
      "OutputLocation": "s3://'${BUCKET_NAME}'/athena-results/"
    },
    "EngineVersion": {
      "SelectedEngineVersion": "Athena engine version 3"
    }
  }'
```

### 9.2 Query Iceberg Table via Athena Console

Go to Athena Console and run:

```sql
-- Check table exists
SHOW TABLES IN iceberg_cdc_db;

-- Query current data
SELECT * FROM iceberg_cdc_db.customers
ORDER BY _processed_at DESC;

-- Verify CDC operations worked:
-- 1. Bob Jones should be updated (new email)
-- 2. Carol White should be deleted (not in results)
-- 3. Frank Miller should be inserted (new customer)

-- Count records
SELECT COUNT(*) as total_customers 
FROM iceberg_cdc_db.customers;

-- Time travel - query as of full load
SELECT * FROM iceberg_cdc_db.customers
FOR SYSTEM_TIME AS OF TIMESTAMP '2024-10-15 12:00:00';

-- View table history
SELECT * FROM iceberg_cdc_db.customers.history;

-- View snapshots
SELECT * FROM iceberg_cdc_db.customers.snapshots
ORDER BY committed_at DESC;
```

---

## Step 10: Monitoring and Maintenance

### 10.1 Create CloudWatch Dashboard

```bash
# Create dashboard for monitoring
aws cloudwatch put-dashboard \
  --dashboard-name IcebergCDCPipeline \
  --dashboard-body '{
    "widgets": [
      {
        "type": "metric",
        "properties": {
          "metrics": [
            ["AWS/Glue", "glue.driver.aggregate.numCompletedTasks", {"stat": "Sum"}],
            [".", "glue.driver.aggregate.numFailedTasks", {"stat": "Sum"}]
          ],
          "period": 300,
          "stat": "Sum",
          "region": "'${AWS_REGION}'",
          "title": "Glue Job Tasks"
        }
      },
      {
        "type": "log",
        "properties": {
          "query": "SOURCE '\''/aws-glue/jobs/output'\'' | fields @timestamp, @message | filter @message like /ERROR/ | sort @timestamp desc | limit 20",
          "region": "'${AWS_REGION}'",
          "title": "Recent Errors"
        }
      }
    ]
  }'
```

### 10.2 Create CloudWatch Alarms

```bash
# Alarm for Glue job failures
aws cloudwatch put-metric-alarm \
  --alarm-name IcebergCDC-JobFailures \
  --alarm-description "Alert when Glue CDC jobs fail" \
  --metric-name glue.driver.aggregate.numFailedTasks \
  --namespace AWS/Glue \
  --statistic Sum \
  --period 300 \
  --evaluation-periods 1 \
  --threshold 1 \
  --comparison-operator GreaterThanThreshold \
  --treat-missing-data notBreaching

# Create SNS topic for notifications
aws sns create-topic --name IcebergCDCAlerts

# Subscribe your email
aws sns subscribe \
  --topic-arn arn:aws:sns:${AWS_REGION}:$(aws sts get-caller-identity --query Account --output text):IcebergCDCAlerts \
  --protocol email \
  --notification-endpoint your-email@example.com

# Link alarm to SNS
aws cloudwatch put-metric-alarm \
  --alarm-name IcebergCDC-JobFailures \
  --alarm-actions arn:aws:sns:${AWS_REGION}:$(aws sts get-caller-identity --query Account --output text):IcebergCDCAlerts
```

### 10.3 Create Maintenance Glue Job

```python
# iceberg_maintenance.py
import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'database_name',
    'table_name',
    'iceberg_warehouse_path',
    'days_to_retain'
])

spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", args['iceberg_warehouse_path']) \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .getOrCreate()

table_identifier = f"glue_catalog.{args['database_name']}.{args['table_name']}"

logger.info(f"Starting maintenance for: {table_identifier}")

# 1. Rewrite data files (compact small files)
logger.info("Compacting small files...")
spark.sql(f"""
    CALL glue_catalog.system.rewrite_data_files(
        table => '{args['database_name']}.{args['table_name']}',
        options => map('target-file-size-bytes', '134217728')
    )
""")

# 2. Rewrite manifests
logger.info("Rewriting manifests...")
spark.sql(f"""
    CALL glue_catalog.system.rewrite_manifests(
        '{args['database_name']}.{args['table_name']}'
    )
""")

# 3. Expire old snapshots
logger.info(f"Expiring snapshots older than {args['days_to_retain']} days...")
spark.sql(f"""
    CALL glue_catalog.system.expire_snapshots(
        table => '{args['database_name']}.{args['table_name']}',
        older_than => DATE_SUB(current_date(), {args['days_to_retain']}),
        retain_last => 5
    )
""")

# 4. Remove orphan files
logger.info("Removing orphan files...")
spark.sql(f"""
    CALL glue_catalog.system.remove_orphan_files(
        table => '{args['database_name']}.{args['table_name']}',
        older_than => TIMESTAMP '{args['days_to_retain']} days'
    )
""")

logger.info("Maintenance completed successfully")

# Show table statistics
logger.info("Table statistics:")
spark.sql(f"SELECT * FROM {table_identifier}.snapshots ORDER BY committed_at DESC LIMIT 5").show()

spark.stop()
```

Upload and create maintenance job:

```bash
# Upload script
aws s3 cp iceberg_maintenance.py s3://${BUCKET_NAME}/scripts/

# Create maintenance job
aws glue create-job \
  --name iceberg-maintenance-customers \
  --role ${ROLE_ARN} \
  --command '{
    "Name": "glueetl",
    "ScriptLocation": "s3://'${BUCKET_NAME}'/scripts/iceberg_maintenance.py",
    "PythonVersion": "3"
  }' \
  --default-arguments '{
    "--job-language": "python",
    "--datalake-formats": "iceberg",
    "--conf": "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
    "--database_name": "iceberg_cdc_db",
    "--table_name": "customers",
    "--iceberg_warehouse_path": "s3://'${BUCKET_NAME}'/iceberg/",
    "--days_to_retain": "7"
  }' \
  --glue-version "4.0" \
  --worker-type "G.1X" \
  --number-of-workers 2

# Schedule maintenance job (weekly)
aws events put-rule \
  --name trigger-iceberg-maintenance-weekly \
  --schedule-expression "cron(0 2 ? * SUN *)"

aws events put-targets \
  --rule trigger-iceberg-maintenance-weekly \
  --targets '[{
    "Id": "1",
    "Arn": "arn:aws:glue:'${AWS_REGION}':'$(aws sts get-caller-identity --query Account --output text)':job/iceberg-maintenance-customers",
    "RoleArn": "'${ROLE_ARN}'"
  }]'
```

---

## Step 11: Advanced Features

### 11.1 Partition Evolution

```python
# Add partitioning to existing table
spark.sql("""
    ALTER TABLE glue_catalog.iceberg_cdc_db.customers
    ADD PARTITION FIELD month(created_date)
""")

# Drop partition
spark.sql("""
    ALTER TABLE glue_catalog.iceberg_cdc_db.customers
    DROP PARTITION FIELD month(created_date)
""")
```

### 11.2 Schema Evolution

```python
# Add new column
spark.sql("""
    ALTER TABLE glue_catalog.iceberg_cdc_db.customers
    ADD COLUMN loyalty_points INT
""")

# Rename column
spark.sql("""
    ALTER TABLE glue_catalog.iceberg_cdc_db.customers
    RENAME COLUMN name TO full_name
""")

# Drop column
spark.sql("""
    ALTER TABLE glue_catalog.iceberg_cdc_db.customers
    DROP COLUMN loyalty_points
""")
```

### 11.3 Time Travel Queries

```sql
-- Query table at specific timestamp
SELECT * FROM iceberg_cdc_db.customers
FOR SYSTEM_TIME AS OF TIMESTAMP '2024-10-15 14:00:00';

-- Query table at specific snapshot ID
SELECT * FROM iceberg_cdc_db.customers
FOR SYSTEM_VERSION AS OF 123456789;

-- Rollback to previous snapshot
CALL glue_catalog.system.rollback_to_snapshot(
    'iceberg_cdc_db.customers', 
    123456789
);
```

### 11.4 Row-Level Security with Views

```sql
-- Create secure view for specific country
CREATE OR REPLACE VIEW iceberg_cdc_db.customers_usa AS
SELECT customer_id, name, email, created_date
FROM iceberg_cdc_db.customers
WHERE country = 'USA';

-- Create view with current timestamp filter
CREATE OR REPLACE VIEW iceberg_cdc_db.customers_recent AS
SELECT *
FROM iceberg_cdc_db.customers
WHERE _processed_at >= current_timestamp - INTERVAL '7' DAY;
```

---

## Step 12: Multi-Table Pipeline Setup

### 12.1 Create Orchestration with Step Functions

```json
{
  "Comment": "Iceberg CDC Pipeline Orchestration",
  "StartAt": "ProcessAllTables",
  "States": {
    "ProcessAllTables": {
      "Type": "Parallel",
      "Branches": [
        {
          "StartAt": "ProcessCustomers",
          "States": {
            "ProcessCustomers": {
              "Type": "Task",
              "Resource": "arn:aws:states:::glue:startJobRun.sync",
              "Parameters": {
                "JobName": "iceberg-incremental-cdc-customers"
              },
              "End": true
            }
          }
        },
        {
          "StartAt": "ProcessOrders",
          "States": {
            "ProcessOrders": {
              "Type": "Task",
              "Resource": "arn:aws:states:::glue:startJobRun.sync",
              "Parameters": {
                "JobName": "iceberg-incremental-cdc-orders"
              },
              "End": true
            }
          }
        },
        {
          "StartAt": "ProcessProducts",
          "States": {
            "ProcessProducts": {
              "Type": "Task",
              "Resource": "arn:aws:states:::glue:startJobRun.sync",
              "Parameters": {
                "JobName": "iceberg-incremental-cdc-products"
              },
              "End": true
            }
          }
        }
      ],
      "Next": "NotifyCompletion"
    },
    "NotifyCompletion": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "arn:aws:sns:us-east-1:123456789012:IcebergCDCAlerts",
        "Message": "All CDC jobs completed successfully"
      },
      "End": true
    }
  }
}
```

Save as `step_functions_definition.json` and create:

```bash
# Create IAM role for Step Functions
aws iam create-role \
  --role-name StepFunctionsIcebergRole \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"Service": "states.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }]
  }'

# Attach policy
aws iam put-role-policy \
  --role-name StepFunctionsIcebergRole \
  --policy-name StepFunctionsExecutionPolicy \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": ["glue:StartJobRun", "glue:GetJobRun"],
        "Resource": "*"
      },
      {
        "Effect": "Allow",
        "Action": ["sns:Publish"],
        "Resource": "*"
      }
    ]
  }'

# Create state machine
STEP_ROLE_ARN=$(aws iam get-role --role-name StepFunctionsIcebergRole --query 'Role.Arn' --output text)

aws stepfunctions create-state-machine \
  --name IcebergCDCOrchestrator \
  --definition file://step_functions_definition.json \
  --role-arn ${STEP_ROLE_ARN}

# Schedule Step Functions execution
aws events put-rule \
  --name trigger-iceberg-orchestrator-hourly \
  --schedule-expression "rate(1 hour)"

aws events put-targets \
  --rule trigger-iceberg-orchestrator-hourly \
  --targets '[{
    "Id": "1",
    "Arn": "arn:aws:states:'${AWS_REGION}':'$(aws sts get-caller-identity --query Account --output text)':stateMachine:IcebergCDCOrchestrator",
    "RoleArn": "'${STEP_ROLE_ARN}'"
  }]'
```

---

## Step 13: Performance Optimization

### 13.1 Configure Glue Job for Better Performance

```bash
# Update Glue job with optimized settings
aws glue update-job \
  --job-name iceberg-incremental-cdc-customers \
  --job-update '{
    "NumberOfWorkers": 5,
    "WorkerType": "G.2X",
    "DefaultArguments": {
      "--enable-metrics": "true",
      "--enable-spark-ui": "true",
      "--enable-continuous-cloudwatch-log": "true",
      "--enable-auto-scaling": "true",
      "--datalake-formats": "iceberg",
      "--conf": "spark.sql.adaptive.enabled=true --conf spark.sql.adaptive.coalescePartitions.enabled=true --conf spark.sql.iceberg.vectorization.enabled=true"
    }
  }'
```

### 13.2 Optimize Iceberg Table Properties

```sql
-- Set table properties for better performance
ALTER TABLE iceberg_cdc_db.customers
SET TBLPROPERTIES (
  'write.parquet.compression-codec' = 'snappy',
  'write.metadata.compression-codec' = 'gzip',
  'commit.manifest.target-size-bytes' = '8388608',
  'commit.manifest-merge.enabled' = 'true',
  'write.target-file-size-bytes' = '134217728',
  'write.metadata.metrics.default' = 'full'
);
```

---

## Step 14: Data Quality Checks

### 14.1 Create Data Quality Glue Job

```python
# data_quality_checks.py
import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, isnull
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'database_name',
    'table_name',
    'iceberg_warehouse_path'
])

spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", args['iceberg_warehouse_path']) \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .getOrCreate()

table_identifier = f"glue_catalog.{args['database_name']}.{args['table_name']}"
df = spark.table(table_identifier)

logger.info(f"Running data quality checks on: {table_identifier}")

# Check 1: Record count
total_records = df.count()
logger.info(f"Total records: {total_records}")

# Check 2: Null checks for critical columns
null_checks = df.select([
    count(when(isnull(c), c)).alias(c) for c in df.columns
])
logger.info("Null counts per column:")
null_checks.show()

# Check 3: Duplicate primary keys
duplicates = df.groupBy("customer_id").count().filter(col("count") > 1)
duplicate_count = duplicates.count()
logger.info(f"Duplicate primary keys: {duplicate_count}")

if duplicate_count > 0:
    logger.warning("Found duplicate records:")
    duplicates.show()
    raise Exception("Data quality check failed: Duplicate primary keys found")

# Check 4: Data freshness
logger.info("Latest data timestamp:")
df.select("_processed_at").orderBy(col("_processed_at").desc()).limit(1).show()

logger.info("All data quality checks passed")
spark.stop()
```

---

## Step 15: Troubleshooting Guide

### Common Issues and Solutions

#### Issue 1: Glue Job Fails with "Table Not Found"
```bash
# Verify table exists in Glue Catalog
aws glue get-table --database-name iceberg_cdc_db --name customers

# Check Iceberg metadata
aws s3 ls s3://${BUCKET_NAME}/iceberg/customers/metadata/
```

#### Issue 2: MERGE Statement Fails
```python
# Check primary key uniqueness in source data
df_cdc.groupBy("customer_id").count().filter(col("count") > 1).show()

# Verify header_operation values
df_cdc.groupBy("header_operation").count().show()
```

#### Issue 3: Slow Query Performance
```sql
-- Check table statistics
DESCRIBE EXTENDED iceberg_cdc_db.customers;

-- Run maintenance
CALL glue_catalog.system.rewrite_data_files('iceberg_cdc_db.customers');
```

#### Issue 4: S3 Permission Errors
```bash
# Verify IAM role permissions
aws iam get-role-policy --role-name GlueIcebergCDCRole --policy-name GlueS3Access

# Test S3 access
aws s3 ls s3://${BUCKET_NAME}/raw/ --profile your-profile
```

---

## Step 16: Cost Optimization

### 16.1 Enable S3 Intelligent Tiering

```bash
aws s3api put-bucket-intelligent-tiering-configuration \
  --bucket ${BUCKET_NAME} \
  --id IcebergArchive \
  --intelligent-tiering-configuration '{
    "Id": "IcebergArchive",
    "Status": "Enabled",
    "Tierings": [
      {
        "Days": 90,
        "AccessTier": "ARCHIVE_ACCESS"
      },
      {
        "Days": 180,
        "AccessTier": "DEEP_ARCHIVE_ACCESS"
      }
    ]
  }'
```

### 16.2 Set S3 Lifecycle Policy

```bash
cat > lifecycle_policy.json << EOF
{
  "Rules": [
    {
      "Id": "DeleteOldRawFiles",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "raw/"
      },
      "Expiration": {
        "Days": 30
      }
    },
    {
      "Id": "TransitionLogs",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "logs/"
      },
      "Transitions": [
        {
          "Days": 7,
          "StorageClass": "STANDARD_IA"
        },
        {
          "Days": 30,
          "StorageClass": "GLACIER"
        }
      ]
    }
  ]
}
EOF

aws s3api put-bucket-lifecycle-configuration \
  --bucket ${BUCKET_NAME} \
  --lifecycle-configuration file://lifecycle_policy.json
```

### 16.3 Monitor Costs

```bash
# Create cost anomaly detector
aws ce create-anomaly-monitor \
  --anomaly-monitor '{
    "MonitorName": "IcebergPipelineCostMonitor",
    "MonitorType": "DIMENSIONAL",
    "MonitorDimension": "SERVICE"
  }'
```

---

## Step 17: Security Best Practices

### 17.1 Enable S3 Bucket Encryption

```bash
aws s3api put-bucket-encryption \
  --bucket ${BUCKET_NAME} \
  --server-side-encryption-configuration '{
    "Rules": [{
      "ApplyServerSideEncryptionByDefault": {
        "SSEAlgorithm": "AES256"
      },
      "BucketKeyEnabled": true
    }]
  }'
```

### 17.2 Enable S3 Versioning

```bash
aws s3api put-bucket-versioning \
  --bucket ${BUCKET_NAME} \
  --versioning-configuration Status=Enabled
```

### 17.3 Enable CloudTrail Logging

```bash
aws cloudtrail create-trail \
  --name iceberg-pipeline-trail \
  --s3-bucket-name ${BUCKET_NAME} \
  --is-multi-region-trail

aws cloudtrail start-logging --name iceberg-pipeline-trail
```

---

## Step 18: Documentation and Runbook

### Daily Operations Checklist

1. **Morning Check** (9 AM)
   ```sql
   -- Check record counts
   SELECT COUNT(*) FROM iceberg_cdc_db.customers;
   
   -- Check latest data freshness
   SELECT MAX(_processed_at) as last_update 
   FROM iceberg_cdc_db.customers;
   
   -- Check for processing errors
   SELECT * FROM iceberg_cdc_db.customers.snapshots 
   WHERE committed_at >= current_date 
   ORDER BY committed_at DESC;
   ```

2. **Monitor Glue Jobs**
   ```bash
   # Check recent job runs
   aws glue get-job-runs \
     --job-name iceberg-incremental-cdc-customers \
     --max-results 10
   
   # Check for failures
   aws glue get-job-runs \
     --job-name iceberg-incremental-cdc-customers \
     --max-results 10 \
     --query "JobRuns[?JobRunState=='FAILED']"
   ```

3. **Weekly Maintenance** (Sunday 2 AM - Automated)
   - Compact small files
   - Expire old snapshots
   - Remove orphan files

### Emergency Procedures

#### Rollback Procedure
```sql
-- 1. Find snapshot ID to rollback to
SELECT snapshot_id, committed_at, summary
FROM iceberg_cdc_db.customers.snapshots
ORDER BY committed_at DESC
LIMIT 20;

-- 2. Rollback to specific snapshot
CALL glue_catalog.system.rollback_to_snapshot(
    'iceberg_cdc_db.customers',
    1234567890123456789  -- snapshot_id from step 1
);

-- 3. Verify rollback
SELECT COUNT(*), MAX(_processed_at) 
FROM iceberg_cdc_db.customers;
```

#### Recover from Failed CDC Job
```bash
# 1. Identify failed files
aws s3 ls s3://${BUCKET_NAME}/raw/customers/incremental/ --recursive

# 2. Re-run job with specific path
aws glue start-job-run \
  --job-name iceberg-incremental-cdc-customers \
  --arguments '{
    "--s3_incremental_path": "s3://'${BUCKET_NAME}'/raw/customers/incremental/2024-10-15-14/"
  }'

# 3. Monitor completion
aws glue get-job-run \
  --job-name iceberg-incremental-cdc-customers \
  --run-id <run-id-from-step-2>
```

---

## Complete Example: End-to-End Test

### Full Workflow Test Script

```bash
#!/bin/bash
# complete_test.sh

set -e

BUCKET_NAME="your-company-data-lake"
DATABASE_NAME="iceberg_cdc_db"
TABLE_NAME="customers"

echo "========================================="
echo "Iceberg CDC Pipeline - Complete Test"
echo "========================================="

# Step 1: Upload test full load data
echo "Step 1: Uploading full load test data..."
python3 << EOF
import pandas as pd
import boto3
from datetime import datetime
import io

s3 = boto3.client('s3')
df = pd.DataFrame({
    'customer_id': range(1, 101),
    'name': [f'Customer {i}' for i in range(1, 101)],
    'email': [f'customer{i}@test.com' for i in range(1, 101)],
    'country': ['USA'] * 50 + ['UK'] * 50,
    'created_date': ['2024-10-01'] * 100,
    'header_operation': 'INSERT'
})

buffer = io.BytesIO()
df.to_parquet(buffer, index=False)
buffer.seek(0)

s3.put_object(
    Bucket='${BUCKET_NAME}',
    Key='raw/${TABLE_NAME}/full_load/${TABLE_NAME}_test.parquet',
    Body=buffer.getvalue()
)
print("✓ Full load data uploaded")
EOF

# Step 2: Run full load job
echo "Step 2: Running full load Glue job..."
JOB_RUN_ID=$(aws glue start-job-run \
  --job-name iceberg-full-load-${TABLE_NAME} \
  --query 'JobRunId' \
  --output text)

echo "Job Run ID: $JOB_RUN_ID"
echo "Waiting for job completion..."

while true; do
  STATUS=$(aws glue get-job-run \
    --job-name iceberg-full-load-${TABLE_NAME} \
    --run-id $JOB_RUN_ID \
    --query 'JobRun.JobRunState' \
    --output text)
  
  echo "Current status: $STATUS"
  
  if [ "$STATUS" == "SUCCEEDED" ]; then
    echo "✓ CDC processing completed successfully"
    break
  elif [ "$STATUS" == "FAILED" ]; then
    echo "✗ CDC processing failed"
    exit 1
  fi
  
  sleep 30
done

# Step 6: Verify CDC results
echo "Step 6: Verifying CDC results..."

# Expected count: 100 (original) - 5 (deleted) + 5 (inserted) = 100
QUERY_ID=$(aws athena start-query-execution \
  --query-string "SELECT COUNT(*) as count FROM ${DATABASE_NAME}.${TABLE_NAME}" \
  --query-execution-context "Database=${DATABASE_NAME}" \
  --result-configuration "OutputLocation=s3://${BUCKET_NAME}/athena-results/" \
  --query 'QueryExecutionId' \
  --output text)

sleep 10

FINAL_COUNT=$(aws athena get-query-results \
  --query-execution-id $QUERY_ID \
  --query 'ResultSet.Rows[1].Data[0].VarCharValue' \
  --output text)

echo "Final record count: $FINAL_COUNT"

# Verify updates
QUERY_ID=$(aws athena start-query-execution \
  --query-string "SELECT COUNT(*) FROM ${DATABASE_NAME}.${TABLE_NAME} WHERE email LIKE 'updated%'" \
  --query-execution-context "Database=${DATABASE_NAME}" \
  --result-configuration "OutputLocation=s3://${BUCKET_NAME}/athena-results/" \
  --query 'QueryExecutionId' \
  --output text)

sleep 10

UPDATED_COUNT=$(aws athena get-query-results \
  --query-execution-id $QUERY_ID \
  --query 'ResultSet.Rows[1].Data[0].VarCharValue' \
  --output text)

echo "Updated records: $UPDATED_COUNT (expected: 10)"

# Verify new inserts
QUERY_ID=$(aws athena start-query-execution \
  --query-string "SELECT COUNT(*) FROM ${DATABASE_NAME}.${TABLE_NAME} WHERE email LIKE 'new%'" \
  --query-execution-context "Database=${DATABASE_NAME}" \
  --result-configuration "OutputLocation=s3://${BUCKET_NAME}/athena-results/" \
  --query 'QueryExecutionId' \
  --output text)

sleep 10

NEW_COUNT=$(aws athena get-query-results \
  --query-execution-id $QUERY_ID \
  --query 'ResultSet.Rows[1].Data[0].VarCharValue' \
  --output text)

echo "New records: $NEW_COUNT (expected: 5)"

# Step 7: Test time travel
echo "Step 7: Testing time travel..."
QUERY_ID=$(aws athena start-query-execution \
  --query-string "SELECT snapshot_id, committed_at FROM ${DATABASE_NAME}.${TABLE_NAME}.snapshots ORDER BY committed_at DESC LIMIT 5" \
  --query-execution-context "Database=${DATABASE_NAME}" \
  --result-configuration "OutputLocation=s3://${BUCKET_NAME}/athena-results/" \
  --query 'QueryExecutionId' \
  --output text)

sleep 10

echo "Recent snapshots:"
aws athena get-query-results \
  --query-execution-id $QUERY_ID \
  --output table

echo "========================================="
echo "✓ All tests completed successfully!"
echo "========================================="
echo ""
echo "Summary:"
echo "  - Full load: 100 records loaded"
echo "  - CDC operations: 10 updates, 5 deletes, 5 inserts"
echo "  - Final count: $FINAL_COUNT records"
echo "  - Updated records: $UPDATED_COUNT"
echo "  - New records: $NEW_COUNT"
echo ""
echo "Next steps:"
echo "  1. Query data in Athena: SELECT * FROM ${DATABASE_NAME}.${TABLE_NAME}"
echo "  2. View history: SELECT * FROM ${DATABASE_NAME}.${TABLE_NAME}.snapshots"
echo "  3. Monitor jobs in Glue Console"
EOF

chmod +x complete_test.sh
```

---

## Step 19: Production Deployment Checklist

### Pre-Production Validation

```bash
#!/bin/bash
# pre_production_validation.sh

echo "Running pre-production validation..."

# 1. Verify IAM roles
echo "Checking IAM roles..."
aws iam get-role --role-name GlueIcebergCDCRole || echo "ERROR: Role not found"

# 2. Verify S3 buckets
echo "Checking S3 bucket structure..."
aws s3 ls s3://${BUCKET_NAME}/raw/ || echo "ERROR: Raw folder not found"
aws s3 ls s3://${BUCKET_NAME}/iceberg/ || echo "ERROR: Iceberg folder not found"

# 3. Verify Glue database
echo "Checking Glue database..."
aws glue get-database --name iceberg_cdc_db || echo "ERROR: Database not found"

# 4. Verify Glue jobs exist
echo "Checking Glue jobs..."
aws glue get-job --job-name iceberg-full-load-customers || echo "ERROR: Full load job not found"
aws glue get-job --job-name iceberg-incremental-cdc-customers || echo "ERROR: CDC job not found"

# 5. Verify EventBridge rules
echo "Checking EventBridge rules..."
aws events describe-rule --name trigger-iceberg-incremental-hourly || echo "ERROR: EventBridge rule not found"

# 6. Test Athena connectivity
echo "Testing Athena..."
aws athena list-work-groups --query 'WorkGroups[?Name==`iceberg-workgroup`]' || echo "ERROR: Workgroup not found"

# 7. Verify CloudWatch alarms
echo "Checking CloudWatch alarms..."
aws cloudwatch describe-alarms --alarm-names IcebergCDC-JobFailures || echo "ERROR: Alarm not found"

echo "Validation complete!"
```

### Production Deployment Steps

1. **Tag resources for production**
```bash
# Tag S3 bucket
aws s3api put-bucket-tagging \
  --bucket ${BUCKET_NAME} \
  --tagging 'TagSet=[{Key=Environment,Value=Production},{Key=Project,Value=IcebergCDC}]'

# Tag Glue jobs
aws glue tag-resource \
  --resource-arn arn:aws:glue:${AWS_REGION}:$(aws sts get-caller-identity --query Account --output text):job/iceberg-full-load-customers \
  --tags-to-add Environment=Production,Project=IcebergCDC
```

2. **Enable production monitoring**
```bash
# Enable detailed CloudWatch metrics
aws glue update-job \
  --job-name iceberg-incremental-cdc-customers \
  --job-update '{
    "DefaultArguments": {
      "--enable-metrics": "true",
      "--enable-continuous-cloudwatch-log": "true",
      "--enable-spark-ui": "true",
      "--continuous-log-logGroup": "/aws-glue/jobs/production"
    }
  }'
```

3. **Document runbook procedures**
```markdown
# Production Runbook

## Emergency Contacts
- Data Engineering Lead: lead@company.com
- DevOps On-Call: oncall@company.com
- AWS Support Case: https://console.aws.amazon.com/support

## Critical Procedures

### Stop All Processing
aws events disable-rule --name trigger-iceberg-incremental-hourly

### Enable All Processing
aws events enable-rule --name trigger-iceberg-incremental-hourly

### View Current Status
aws glue get-job-runs --job-name iceberg-incremental-cdc-customers --max-results 5
```

---

## Step 20: Scaling Considerations

### Handle Large Tables (>1TB)

```python
# large_table_full_load.py
# Modified full load script for large tables with partitioning

import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'database_name',
    'table_name',
    's3_source_path',
    'iceberg_warehouse_path',
    'partition_column'
])

spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", args['iceberg_warehouse_path']) \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.files.maxPartitionBytes", "134217728") \
    .getOrCreate()

df = spark.read.parquet(args['s3_source_path'])

# Add year/month partitions for date column
if args['partition_column'] in df.columns:
    df = df.withColumn("year", year(args['partition_column'])) \
           .withColumn("month", month(args['partition_column']))

table_identifier = f"glue_catalog.{args['database_name']}.{args['table_name']}"

# Create with partitioning
df.writeTo(table_identifier) \
    .using("iceberg") \
    .partitionedBy("year", "month") \
    .tableProperty("format-version", "2") \
    .tableProperty("write.parquet.compression-codec", "snappy") \
    .tableProperty("write.target-file-size-bytes", "134217728") \
    .tableProperty("write.distribution-mode", "hash") \
    .createOrReplace()

spark.stop()
```

### Parallel Processing for Multiple Tables

```python
# parallel_cdc_processor.py
import concurrent.futures
import boto3
from typing import List

glue_client = boto3.client('glue')

def process_table_cdc(table_name: str) -> dict:
    """Process CDC for a single table"""
    try:
        response = glue_client.start_job_run(
            JobName=f'iceberg-incremental-cdc-{table_name}',
            Arguments={
                '--table_name': table_name
            }
        )
        return {
            'table': table_name,
            'status': 'started',
            'job_run_id': response['JobRunId']
        }
    except Exception as e:
        return {
            'table': table_name,
            'status': 'failed',
            'error': str(e)
        }

def process_all_tables(tables: List[str], max_workers: int = 5):
    """Process CDC for multiple tables in parallel"""
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_table_cdc, table): table for table in tables}
        
        results = []
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            results.append(result)
            print(f"Table {result['table']}: {result['status']}")
        
        return results

if __name__ == "__main__":
    tables = ['customers', 'orders', 'products', 'payments', 'shipments']
    results = process_all_tables(tables)
    
    # Print summary
    succeeded = sum(1 for r in results if r['status'] == 'started')
    failed = sum(1 for r in results if r['status'] == 'failed')
    
    print(f"\nProcessing Summary:")
    print(f"  Succeeded: {succeeded}")
    print(f"  Failed: {failed}")
```

---

## Step 21: Advanced Athena Queries

### Useful Production Queries

```sql
-- 1. Query with partition pruning
SELECT customer_id, name, email
FROM iceberg_cdc_db.customers
WHERE year = 2024 AND month = 10
  AND country = 'USA'
LIMIT 100;

-- 2. Incremental query (only recent changes)
SELECT *
FROM iceberg_cdc_db.customers
WHERE _processed_at >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR;

-- 3. Compare snapshots (what changed between two points)
WITH snapshot_1 AS (
  SELECT * FROM iceberg_cdc_db.customers
  FOR SYSTEM_VERSION AS OF 123456789
),
snapshot_2 AS (
  SELECT * FROM iceberg_cdc_db.customers
  FOR SYSTEM_VERSION AS OF 987654321
)
SELECT 
  COALESCE(s1.customer_id, s2.customer_id) as customer_id,
  s1.name as old_name,
  s2.name as new_name,
  CASE 
    WHEN s1.customer_id IS NULL THEN 'INSERTED'
    WHEN s2.customer_id IS NULL THEN 'DELETED'
    ELSE 'UPDATED'
  END as change_type
FROM snapshot_1 s1
FULL OUTER JOIN snapshot_2 s2 ON s1.customer_id = s2.customer_id
WHERE s1.customer_id IS NULL 
   OR s2.customer_id IS NULL 
   OR s1.name != s2.name;

-- 4. Audit trail query
SELECT 
  snapshot_id,
  committed_at,
  summary['added-records'] as records_added,
  summary['deleted-records'] as records_deleted,
  summary['total-records'] as total_records,
  summary['total-data-files'] as data_files
FROM iceberg_cdc_db.customers.snapshots
ORDER BY committed_at DESC
LIMIT 30;

-- 5. Data quality check
SELECT 
  country,
  COUNT(*) as customer_count,
  COUNT(DISTINCT email) as unique_emails,
  MIN(_processed_at) as first_load,
  MAX(_processed_at) as last_update
FROM iceberg_cdc_db.customers
GROUP BY country
ORDER BY customer_count DESC;

-- 6. Find orphaned records (in target but not in source)
-- Useful for validation
SELECT t.*
FROM iceberg_cdc_db.customers t
LEFT JOIN source_database.customers s ON t.customer_id = s.customer_id
WHERE s.customer_id IS NULL;

-- 7. Performance metrics
SELECT 
  DATE_TRUNC('hour', _processed_at) as hour,
  COUNT(*) as records_processed,
  COUNT(DISTINCT customer_id) as unique_customers
FROM iceberg_cdc_db.customers
WHERE _processed_at >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY DATE_TRUNC('hour', _processed_at)
ORDER BY hour DESC;
```

---

## Step 22: Disaster Recovery Plan

### Backup Strategy

```bash
#!/bin/bash
# backup_iceberg_metadata.sh

BUCKET_NAME="your-company-data-lake"
BACKUP_BUCKET="your-company-dr-backup"
DATABASE_NAME="iceberg_cdc_db"

echo "Starting Iceberg metadata backup..."

# 1. Backup Glue Catalog metadata
aws glue get-database --name ${DATABASE_NAME} > database_backup.json

aws glue get-tables --database-name ${DATABASE_NAME} > tables_backup.json

# Upload to backup bucket
aws s3 cp database_backup.json s3://${BACKUP_BUCKET}/metadata/$(date +%Y%m%d)/
aws s3 cp tables_backup.json s3://${BACKUP_BUCKET}/metadata/$(date +%Y%m%d)/

# 2. Sync Iceberg metadata files
aws s3 sync \
  s3://${BUCKET_NAME}/iceberg/ \
  s3://${BACKUP_BUCKET}/iceberg-backup/$(date +%Y%m%d)/ \
  --exclude "*/data/*" \
  --include "*/metadata/*"

# 3. Export Glue job definitions
for job in iceberg-full-load-customers iceberg-incremental-cdc-customers; do
  aws glue get-job --job-name ${job} > ${job}_backup.json
  aws s3 cp ${job}_backup.json s3://${BACKUP_BUCKET}/glue-jobs/$(date +%Y%m%d)/
done

echo "Backup completed successfully"
```

### Recovery Procedure

```bash
#!/bin/bash
# restore_iceberg_pipeline.sh

BACKUP_BUCKET="your-company-dr-backup"
BACKUP_DATE="20241015"  # Date of backup to restore
BUCKET_NAME="your-company-data-lake"
DATABASE_NAME="iceberg_cdc_db"

echo "Starting recovery from backup ${BACKUP_DATE}..."

# 1. Restore Glue database
aws glue create-database \
  --database-input "$(aws s3 cp s3://${BACKUP_BUCKET}/metadata/${BACKUP_DATE}/database_backup.json - | jq '.Database')"

# 2. Restore Iceberg metadata
aws s3 sync \
  s3://${BACKUP_BUCKET}/iceberg-backup/${BACKUP_DATE}/ \
  s3://${BUCKET_NAME}/iceberg/ \
  --delete

# 3. Restore Glue Catalog tables
# Tables will automatically register from Iceberg metadata

# 4. Verify restoration
echo "Verifying restoration..."
aws glue get-tables --database-name ${DATABASE_NAME} --max-results 100

echo "Recovery completed. Please verify data in Athena."
```

---

## Step 23: Migration from Existing System

### Migrate from Current Athena Tables to Iceberg

```python
# migrate_to_iceberg.py
import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'source_database',
    'source_table',
    'target_database',
    'target_table',
    'iceberg_warehouse_path'
])

spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", args['iceberg_warehouse_path']) \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .getOrCreate()

print(f"Migrating {args['source_database']}.{args['source_table']} to Iceberg...")

# Read existing table
df = spark.table(f"{args['source_database']}.{args['source_table']}")

record_count = df.count()
print(f"Source table has {record_count} records")

# Create Iceberg table
target_table = f"glue_catalog.{args['target_database']}.{args['target_table']}"

df.writeTo(target_table) \
    .using("iceberg") \
    .tableProperty("format-version", "2") \
    .create()

# Verify
target_count = spark.table(target_table).count()
print(f"Target Iceberg table has {target_count} records")

if record_count == target_count:
    print("✓ Migration successful!")
else:
    print(f"✗ Migration issue: source={record_count}, target={target_count}")

spark.stop()
```

---

## Step 24: Performance Benchmarking

### Benchmark Query Performance

```sql
-- Create test to compare query performance

-- Benchmark 1: Full table scan
SELECT COUNT(*) 
FROM iceberg_cdc_db.customers;

-- Benchmark 2: Filtered query
SELECT *
FROM iceberg_cdc_db.customers
WHERE country = 'USA'
  AND created_date >= DATE '2024-01-01';

-- Benchmark 3: Aggregation
SELECT country, COUNT(*) as customer_count
FROM iceberg_cdc_db.customers
GROUP BY country;

-- Benchmark 4: Join query
SELECT c.customer_id, c.name, COUNT(o.order_id) as order_count
FROM iceberg_cdc_db.customers c
LEFT JOIN iceberg_cdc_db.orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name
ORDER BY order_count DESC
LIMIT 100;
```

### Monitor Performance Metrics

```python
# performance_monitor.py
import boto3
from datetime import datetime, timedelta

cloudwatch = boto3.client('cloudwatch')

def get_glue_job_metrics(job_name, hours=24):
    """Get performance metrics for Glue job"""
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=hours)
    
    metrics = cloudwatch.get_metric_statistics(
        Namespace='AWS/Glue',
        MetricName='glue.driver.aggregate.elapsedTime',
        Dimensions=[{'Name': 'JobName', 'Value': job_name}],
        StartTime=start_time,
        EndTime=end_time,
        Period=3600,
        Statistics=['Average', 'Maximum', 'Minimum']
    )
    
    return metrics

# Usage
metrics = get_glue_job_metrics('iceberg-incremental-cdc-customers')
for datapoint in metrics['Datapoints']:
    print(f"Time: {datapoint['Timestamp']}")
    print(f"  Avg: {datapoint['Average']:.2f}s")
    print(f"  Max: {datapoint['Maximum']:.2f}s")
```

---

## Final Checklist

### ✅ Implementation Complete

- [ ] S3 buckets created and configured
- [ ] IAM roles and policies set up
- [ ] Glue database created
- [ ] Full load Glue job created and tested
- [ ] Incremental CDC Glue job created and tested
- [ ] EventBridge/Lambda triggers configured
- [ ] Athena workgroup configured
- [ ] Monitoring and alarms set up
- [ ] Maintenance job scheduled
- [ ] Documentation completed
- [ ] Backup strategy implemented
- [ ] Team trained on operations

### 📊 Success Metrics

Monitor these KPIs:
- **Data Freshness**: Time between source change and Iceberg update < 1 hour
- **Job Success Rate**: > 99%
- **Query Performance**: P95 latency < 30 seconds
- **Data Quality**: 0 duplicate primary keys
- **Cost**: Track monthly AWS costs (Glue DPUs, S3 storage, Athena queries)

### 📞 Support Resources

- **AWS Documentation**: https://docs.aws.amazon.com/glue/
- **Iceberg Documentation**: https://iceberg.apache.org/docs/latest/
- **AWS Support**: Open case in AWS Console
- **Community**: Apache Iceberg Slack

---

## Congratulations! 🎉

You now have a production-ready Iceberg CDC pipeline on AWS that handles:
- ✅ Full and incremental data loads
- ✅ INSERT, UPDATE, and DELETE operations
- ✅ ACID transactions
- ✅ Time travel capabilities
- ✅ Automated processing
- ✅ Monitoring and alerting
- ✅ Disaster recovery

Your data pipeline is scalable, maintainable, and follows AWS best practices!SUCCEEDED" ]; then
    echo "✓ Full load completed successfully"
    break
  elif [ "$STATUS" == "FAILED" ]; then
    echo "✗ Full load failed"
    exit 1
  fi
  
  sleep 30
done

# Step 3: Verify full load in Athena
echo "Step 3: Verifying full load..."
QUERY_ID=$(aws athena start-query-execution \
  --query-string "SELECT COUNT(*) as count FROM ${DATABASE_NAME}.${TABLE_NAME}" \
  --query-execution-context "Database=${DATABASE_NAME}" \
  --result-configuration "OutputLocation=s3://${BUCKET_NAME}/athena-results/" \
  --query 'QueryExecutionId' \
  --output text)

sleep 10

aws athena get-query-results \
  --query-execution-id $QUERY_ID \
  --query 'ResultSet.Rows[1].Data[0].VarCharValue' \
  --output text

echo "✓ Full load verified"

# Step 4: Upload incremental CDC data
echo "Step 4: Uploading incremental CDC data..."
python3 << EOF
import pandas as pd
import boto3
from datetime import datetime
import io

s3 = boto3.client('s3')

# UPDATE 10 records, DELETE 5 records, INSERT 5 new records
df = pd.DataFrame({
    'customer_id': list(range(1, 11)) + list(range(11, 16)) + list(range(101, 106)),
    'name': [f'Customer {i} Updated' for i in range(1, 11)] + 
            [f'Customer {i}' for i in range(11, 16)] +
            [f'New Customer {i}' for i in range(101, 106)],
    'email': [f'updated{i}@test.com' for i in range(1, 11)] +
             [f'customer{i}@test.com' for i in range(11, 16)] +
             [f'new{i}@test.com' for i in range(101, 106)],
    'country': ['USA'] * 20,
    'created_date': ['2024-10-01'] * 10 + ['2024-10-01'] * 5 + ['2024-10-15'] * 5,
    'header_operation': ['UPDATE'] * 10 + ['DELETE'] * 5 + ['INSERT'] * 5
})

timestamp = datetime.now().strftime('%Y-%m-%d-%H')
buffer = io.BytesIO()
df.to_parquet(buffer, index=False)
buffer.seek(0)

s3.put_object(
    Bucket='${BUCKET_NAME}',
    Key=f'raw/${TABLE_NAME}/incremental/{timestamp}/${TABLE_NAME}_delta.parquet',
    Body=buffer.getvalue()
)
print("✓ Incremental CDC data uploaded")
print("  - 10 UPDATEs")
print("  - 5 DELETEs")
print("  - 5 INSERTs")
EOF

# Step 5: Run incremental CDC job
echo "Step 5: Running incremental CDC Glue job..."
CDC_RUN_ID=$(aws glue start-job-run \
  --job-name iceberg-incremental-cdc-${TABLE_NAME} \
  --query 'JobRunId' \
  --output text)

echo "CDC Job Run ID: $CDC_RUN_ID"
echo "Waiting for CDC job completion..."

while true; do
  STATUS=$(aws glue get-job-run \
    --job-name iceberg-incremental-cdc-${TABLE_NAME} \
    --run-id $CDC_RUN_ID \
    --query 'JobRun.JobRunState' \
    --output text)
  
  echo "Current status: $STATUS"
  
  if [ "$STATUS" == "
