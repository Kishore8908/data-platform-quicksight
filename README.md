# 🏗️ Data Platform with QuickSight Self-Serve Analytics

An end-to-end AWS data platform that automates the full journey from
raw data ingestion to self-serve analytics in Amazon QuickSight.

Built using **Kinesis → Glue → Iceberg → Redshift → QuickSight**
with config-driven automation and zero manual onboarding.

---

## 🚀 What This Does

When a new dataset is added to the platform:

1. **Raw data** arrives via Amazon Kinesis Data Streams
2. **AWS Glue** (orchestrated by Airflow) transforms and builds custom tables
3. **Apache Iceberg** stores data as lakehouse tables on S3
4. **Amazon Redshift** mounts Iceberg tables as external tables
5. **A config file** is uploaded to S3 defining permissions
6. **AWS Lambda** automatically triggers and:
   - Creates a QuickSight folder named after the database
   - Creates a QuickSight dataset using Redshift direct query
   - Adds the dataset to the correct folder
   - Applies user/group permissions from the config

**Result:** Users log into QuickSight and find their datasets already there — correctly permissioned and organised.

---

## 🏛️ Architecture

```
                    ┌─────────────────────────────────────┐
                    │         DATA SOURCES                 │
                    │  (Apps, APIs, Databases, Files)      │
                    └──────────────────┬──────────────────┘
                                       │
                                       ▼
                    ┌─────────────────────────────────────┐
                    │     Amazon Kinesis Data Streams      │
                    │     (real-time ingestion layer)      │
                    └──────────────────┬──────────────────┘
                                       │
                                       ▼
                    ┌─────────────────────────────────────┐
                    │         Apache Airflow               │
                    │    (DAG orchestration — config       │
                    │     driven, one DAG per pipeline)    │
                    └──────────────────┬──────────────────┘
                                       │
                                       ▼
                    ┌─────────────────────────────────────┐
                    │           AWS Glue ETL               │
                    │  (transform → deduplicate → audit)   │
                    └──────────────────┬──────────────────┘
                                       │
                                       ▼
                    ┌─────────────────────────────────────┐
                    │       Apache Iceberg on S3           │
                    │  (schema evolution · time travel     │
                    │   · ACID · compaction · snapshots)   │
                    └──────────────────┬──────────────────┘
                                       │
                                       ▼
                    ┌─────────────────────────────────────┐
                    │        Amazon Redshift               │
                    │   (external tables via Spectrum      │
                    │    · RLS · CLS · direct query)       │
                    └──────────────────┬──────────────────┘
                                       │
                          ┌────────────┴──────────────┐
                          │                           │
                          ▼                           ▼
          ┌───────────────────────┐   ┌──────────────────────────┐
          │   S3 Permission       │   │     AWS Lambda            │
          │   Config Upload       │──▶│  (auto-triggered by S3)  │
          │  (new table added)    │   │                           │
          └───────────────────────┘   │  1. Create QS folder      │
                                      │  2. Create QS dataset     │
                                      │  3. Add to folder         │
                                      │  4. Apply permissions     │
                                      └──────────────┬───────────┘
                                                     │
                                                     ▼
                                      ┌──────────────────────────┐
                                      │    Amazon QuickSight      │
                                      │  (self-serve analytics)   │
                                      │                           │
                                      │  Users log in → datasets  │
                                      │  already there ✅         │
                                      └──────────────────────────┘
```

---

## 📂 Project Structure

```
data-platform-quicksight/
├── kinesis/
│   └── producer.py              # Kinesis stream producer
├── glue/
│   └── etl_job.py               # Glue ETL job (Iceberg writer)
├── airflow/
│   └── data_platform_dag.py     # Config-driven DAG factory
├── lambda/
│   └── quicksight_onboarder.py  # QS automation Lambda
├── config/
│   ├── pipelines.json           # Pipeline configs for Airflow
│   └── permission_config_sample.json  # Sample permission config
├── docs/
│   └── architecture.md          # Detailed architecture notes
├── requirements.txt
└── README.md
```

---

## ⚡ Quick Start

### Prerequisites
- AWS account with appropriate IAM permissions
- Python 3.10+
- Apache Airflow 2.7+

### 1. Clone the repo
```bash
git clone https://github.com/Biswajit107927/data-platform-quicksight.git
cd data-platform-quicksight
pip install -r requirements.txt
```

### 2. Configure your pipelines
Edit `config/pipelines.json` with your S3 paths, database names, and schedules.

### 3. Deploy the Lambda
```bash
cd lambda
zip quicksight_onboarder.zip quicksight_onboarder.py
aws lambda create-function \
  --function-name quicksight-onboarder \
  --runtime python3.11 \
  --zip-file fileb://quicksight_onboarder.zip \
  --handler quicksight_onboarder.lambda_handler \
  --role arn:aws:iam::YOUR_ACCOUNT:role/lambda-qs-role \
  --environment Variables="{
    AWS_ACCOUNT_ID=YOUR_ACCOUNT_ID,
    REDSHIFT_CLUSTER_ID=YOUR_CLUSTER,
    REDSHIFT_DATABASE=YOUR_DB,
    REDSHIFT_SECRET_ARN=YOUR_SECRET,
    REDSHIFT_IAM_ROLE=YOUR_ROLE
  }"
```

### 4. Add S3 trigger to Lambda
Configure S3 to trigger the Lambda when a new config file is uploaded to your config bucket.

### 5. Upload a permission config
```bash
aws s3 cp config/permission_config_sample.json \
  s3://your-config-bucket/configs/marketing_db/campaign_spend.json
```

Lambda triggers automatically and onboards the table to QuickSight.

---

## 🔧 Key Components

### Kinesis Producer (`kinesis/producer.py`)
- Publishes records to Kinesis Data Streams
- Supports single record and batch publishing (up to 500 per call)
- Adds ingestion metadata (`_ingested_at`, `_record_id`)
- Configurable partition key field

### Glue ETL Job (`glue/etl_job.py`)
- Reads raw JSON from S3 landing zone
- Deduplicates by `_record_id`
- Adds ETL audit columns
- Writes to Apache Iceberg with automatic compaction
- Manages Iceberg snapshots (7-day retention)

### Airflow DAG Factory (`airflow/data_platform_dag.py`)
- Config-driven — one template creates DAGs for all pipelines
- Built-in SLA monitoring per pipeline
- Automatic retry with exponential backoff
- Email alerting on failure
- S3KeySensor waits for source data before triggering

### QuickSight Lambda (`lambda/quicksight_onboarder.py`)
- Triggered automatically by S3 config upload
- Idempotent — safe to run multiple times
- Creates folders, datasets, and permissions atomically
- Supports both user and group permissions
- Direct query mode — no SPICE import needed

---

## 📋 Permission Config Format

```json
{
  "database": "marketing_db",
  "table": "campaign_spend",
  "redshift_schema": "marketing",
  "folder_name": "Marketing Analytics",
  "dataset_display_name": "Campaign Spend Dashboard",
  "permissions": [
    {
      "principal": "arn:aws:quicksight:us-east-1:ACCOUNT:user/default/USERNAME",
      "actions": [
        "quicksight:DescribeDataSet",
        "quicksight:QueryDataSet"
      ]
    },
    {
      "principal": "arn:aws:quicksight:us-east-1:ACCOUNT:group/default/GROUP",
      "actions": [
        "quicksight:DescribeDataSet",
        "quicksight:QueryDataSet"
      ]
    }
  ]
}
```

---

## 🔐 IAM Permissions Required

### Lambda Execution Role
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "quicksight:CreateFolder",
        "quicksight:DescribeFolder",
        "quicksight:CreateFolderMembership",
        "quicksight:CreateDataSet",
        "quicksight:DescribeDataSet",
        "quicksight:UpdateDataSetPermissions"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": "arn:aws:s3:::your-config-bucket/*"
    }
  ]
}
```

---

## 📈 Complexity & Scale

| Component | Scale |
|---|---|
| Kinesis throughput | Up to 1MB/s per shard — add shards to scale |
| Glue job | Scales horizontally with DPU allocation |
| Iceberg table size | Petabyte scale on S3 |
| Redshift | Scales with cluster size or Serverless |
| Lambda | Concurrent executions — one per config file |
| QuickSight | Up to 10,000 datasets per account |

---

## 🔮 Production Extensions

- **CDK stack** — deploy all infrastructure as code
- **Schema Registry** — enforce data contracts at ingestion
- **Dead Letter Queue** — handle Lambda failures gracefully
- **CloudWatch dashboards** — monitor pipeline health
- **Row-Level Security** — Redshift RLS policies per user group
- **Column-Level Security** — mask PII fields in QuickSight datasets

---

## 👤 Author

**Biswajit Praharaj** — Senior Data Engineer
10+ years building production data infrastructure at Amazon
Specialising in AWS data platforms, Apache Iceberg, Airflow, and Redshift

🔗 [LinkedIn](https://www.linkedin.com/in/bpraharaj/)
🐙 [GitHub](https://github.com/Biswajit107927)
