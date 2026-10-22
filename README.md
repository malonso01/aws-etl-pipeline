# AWS ETL Pipeline — Credit Card Fraud Detection

Cloud data pipeline built on AWS Free Tier that ingests raw transaction data, catalogs it automatically, and enables SQL analytics — no servers to manage.

## Why This Project

Most data engineering work doesn't happen on a single machine. It happens in the cloud, where data volumes are massive, infrastructure scales automatically, and teams need to query petabytes without maintaining databases. This project replicates that real-world setup using the same AWS stack that banks and fintechs use in production — but entirely on the Free Tier.

AWS was chosen over Azure or GCP because it is the most widely adopted cloud platform and its Free Tier covers everything needed here at zero cost.

## Architecture
creditcard.csv → Amazon S3 → AWS Glue → Amazon Athena
(143 MB) (raw storage) (catalog) (SQL queries)

| Service | Role |
|---|---|
| **Amazon S3** | Object storage for the raw CSV — cheap, durable, unlimited scale |
| **AWS Glue Crawler** | Scans the CSV, infers schema (31 columns + types), registers it in the Data Catalog |
| **AWS Glue Data Catalog** | Dictionary that tells Athena what data exists in S3 and how to read it |
| **Amazon Athena** | Serverless SQL engine — queries the CSV directly in S3, no database needed |

## Why S3 + Glue + Athena

A traditional database like PostgreSQL works fine for a single CSV on a laptop. It breaks at scale. This stack solves three problems at once:

- **Volume** — S3 stores terabytes of raw data without schema constraints; Athena scans only what the query needs
- **No maintenance** — Glue and Athena are serverless; there is no server to patch, restart or pay for when idle
- **Automation-ready** — the Glue crawler can be scheduled to run on new data arrivals, the whole pipeline becomes event-driven with no manual steps

## Dataset

**creditcard.csv** — 284,807 European credit card transactions from September 2013 (Kaggle).

| Column | Description |
|---|---|
| `Time` | Seconds elapsed since first transaction |
| `V1–V28` | PCA-anonymized features (original features are confidential) |
| `Amount` | Transaction amount in USD |
| `Class` | 0 = legitimate, 1 = fraud |

## Project Structure
aws-etl-pipeline/
├── pipeline.py # Python script — full pipeline via boto3 (OOP)
└── README.md

## pipeline.py — OOP Design

The script uses four classes, each with a single responsibility:
FraudDetectionPipeline ← orchestrator: wires the three layers together
├── S3Layer ← connects to S3, lists files in the bucket
├── GlueLayer ← reads table schema and crawler status from the Data Catalog
└── AthenaLayer ← executes SQL queries, polls for completion, returns results

OOP was chosen over standalone functions so that each AWS service is self-contained — if S3's API changes, only `S3Layer` needs updating. The code is also easier for another engineer to read and extend (add Redshift, SageMaker, etc.) without touching existing logic.

## Results

Queries run via Athena on the raw CSV in S3:

### Class Distribution
| Class | Count | Share |
|---|---|---|
| 0 — Legitimate | 284,315 | 99.827% |
| 1 — Fraud | 492 | 0.173% |

Fraud transactions represent less than 0.2% of all activity — a classic extreme class imbalance problem in financial data.

### Average Transaction Amount by Class
| Class | Avg Amount | Max Amount |
|---|---|---|
| Legitimate | $88.29 | $25,691.16 |
| Fraud | $122.21 | $2,125.87 |

Fraud transactions average ~$40 more than legitimate ones. Fraudsters tend to target mid-range amounts — high enough to extract value, low enough to avoid immediate flags.

## How to Run

**Prerequisites:** Python 3.8+, boto3, AWS CLI configured with S3 + Glue + Athena permissions.

```bash
pip install boto3
aws configure   # enter Access Key, Secret, region: eu-west-1, output: json
python pipeline.py
```

The script runs three Athena queries against the live data in S3 and prints results to the terminal.

## AWS Region

All resources deployed in **eu-west-1 (Europe — Ireland)** — the primary AWS region for financial services firms in Dublin (Fidelity, EY, SAP, Medtronic).

---

*Portfolio project — targeting graduate Data Engineering roles in financial services (Galway, 2027).*