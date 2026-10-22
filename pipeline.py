"""
AWS ETL Pipeline — Credit Card Fraud Detection
================================================
Pipeline: S3 → Glue Data Catalog → Athena (SQL)

Author : Miguel Alonso
Region : eu-west-1 (Europa - Irlanda)
Dataset: creditcard.csv — 284,807 European card transactions (Kaggle)
"""

import boto3
import time


# ══════════════════════════════════════════════════════════════════════════
# S3Layer — Raw data storage
# ══════════════════════════════════════════════════════════════════════════
class S3Layer:
    """Handles raw data stored in Amazon S3."""

    def __init__(self, bucket: str, region: str):
        self.bucket = bucket
        self.client = boto3.client("s3", region_name=region)

    def list_files(self) -> list[dict]:
        response = self.client.list_objects_v2(Bucket=self.bucket)
        return response.get("Contents", [])

    def report(self):
        print("=" * 60)
        print("STEP 1 — S3: Raw Data Storage")
        print("=" * 60)
        for obj in self.list_files():
            size_mb = obj["Size"] / (1024 * 1024)
            print(f"  File  : {obj['Key']}")
            print(f"  Size  : {size_mb:.1f} MB")
            print(f"  Stored: {obj['LastModified'].strftime('%Y-%m-%d %H:%M UTC')}")


# ══════════════════════════════════════════════════════════════════════════
# GlueLayer — Serverless ETL + Data Catalog
# ══════════════════════════════════════════════════════════════════════════
class GlueLayer:
    """Handles AWS Glue Data Catalog: database, table schema and crawler."""

    def __init__(self, database: str, table: str, crawler: str, region: str):
        self.database = database
        self.table    = table
        self.crawler  = crawler
        self.client   = boto3.client("glue", region_name=region)

    def get_schema(self) -> list[dict]:
        t = self.client.get_table(DatabaseName=self.database, Name=self.table)
        return t["Table"]["StorageDescriptor"]["Columns"]

    def get_location(self) -> str:
        t = self.client.get_table(DatabaseName=self.database, Name=self.table)
        return t["Table"]["StorageDescriptor"]["Location"]

    def get_crawler_status(self) -> dict:
        c = self.client.get_crawler(Name=self.crawler)
        return c["Crawler"].get("LastCrawl", {})

    def report(self):
        print("\n" + "=" * 60)
        print("STEP 2 — Glue: Data Catalog")
        print("=" * 60)

        db = self.client.get_database(Name=self.database)
        print(f"  Database : {db['Database']['Name']}")
        print(f"  Created  : {db['Database']['CreateTime'].strftime('%Y-%m-%d')}")

        cols = self.get_schema()
        print(f"\n  Table    : {self.table}")
        print(f"  Columns  : {len(cols)}")
        print(f"  Location : {self.get_location()}")
        print("\n  Schema (first 5 + last):")
        for col in cols[:5]:
            print(f"    {col['Name']:<10} {col['Type']}")
        print(f"    {'...':}")
        print(f"    {cols[-1]['Name']:<10} {cols[-1]['Type']}")

        last = self.get_crawler_status()
        print(f"\n  Crawler  : {self.crawler}")
        print(f"  Status   : {last.get('Status', 'N/A')}")


# ══════════════════════════════════════════════════════════════════════════
# AthenaLayer — Serverless SQL on S3
# ══════════════════════════════════════════════════════════════════════════
class AthenaLayer:
    """Runs SQL queries via Amazon Athena and returns structured results."""

    def __init__(self, database: str, output_location: str, region: str):
        self.database        = database
        self.output_location = output_location
        self.client          = boto3.client("athena", region_name=region)

    def query(self, sql: str) -> list[dict]:
        """Execute SQL and return rows as list of dicts."""
        qid = self.client.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": self.database},
            ResultConfiguration={"OutputLocation": self.output_location},
        )["QueryExecutionId"]

        while True:
            state = self.client.get_query_execution(
                QueryExecutionId=qid
            )["QueryExecution"]["Status"]["State"]
            if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            time.sleep(1)

        if state != "SUCCEEDED":
            raise RuntimeError(f"Query {state}: {sql[:60]}")

        rows    = self.client.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
        headers = [c["VarCharValue"] for c in rows[0]["Data"]]
        return [
            {headers[i]: rows[j]["Data"][i].get("VarCharValue", "")
             for i in range(len(headers))}
            for j in range(1, len(rows))
        ]

    def report(self):
        print("\n" + "=" * 60)
        print("STEP 3 — Athena: SQL Analytics")
        print("=" * 60)

        # Query 1 — Class distribution
        print("\n  Query 1: Class Distribution")
        rows  = self.query(
            "SELECT TRIM(Class) AS Class, COUNT(*) AS total "
            "FROM creditcard GROUP BY TRIM(Class) ORDER BY TRIM(Class);"
        )
        total = sum(int(r["total"]) for r in rows)
        for r in rows:
            label = "Fraud" if r["Class"] == "1" else "Legitimate"
            pct   = int(r["total"]) / total * 100
            print(f"    Class {r['Class']} ({label:<11}): {int(r['total']):>7,}  ({pct:.3f}%)")

        # Query 2 — Average amount by class
        print("\n  Query 2: Average Transaction Amount by Class")
        rows = self.query(
            "SELECT TRIM(Class) AS Class, "
            "ROUND(AVG(Amount), 2) AS avg_amount, "
            "ROUND(MAX(Amount), 2) AS max_amount "
            "FROM creditcard GROUP BY TRIM(Class) ORDER BY TRIM(Class);"
        )
        for r in rows:
            label = "Fraud" if r["Class"] == "1" else "Legitimate"
            print(f"    Class {r['Class']} ({label:<11}): avg=${r['avg_amount']:<8}  max=${r['max_amount']}")

        # Query 3 — Overview
        print("\n  Query 3: Dataset Overview")
        rows = self.query(
            "SELECT COUNT(*) AS total_transactions, "
            "SUM(CASE WHEN TRIM(Class)='1' THEN 1 ELSE 0 END) AS fraud_count, "
            "ROUND(SUM(CASE WHEN TRIM(Class)='1' THEN 1 ELSE 0 END)*100.0/COUNT(*),4) AS fraud_pct "
            "FROM creditcard;"
        )
        if rows:
            r = rows[0]
            print(f"    Total transactions : {int(r['total_transactions']):>9,}")
            print(f"    Fraud transactions : {int(r['fraud_count']):>9,}")
            print(f"    Fraud rate         :  {r['fraud_pct']}%")


# ══════════════════════════════════════════════════════════════════════════
# FraudDetectionPipeline — Orchestrator
# ══════════════════════════════════════════════════════════════════════════
class FraudDetectionPipeline:
    """
    Orchestrates the full ETL pipeline:
        S3 (raw) → Glue (catalog) → Athena (SQL analytics)
    """

    REGION   = "eu-west-1"
    BUCKET   = "fraud-detection-pipeline-miguel"
    DATABASE = "fraud_detection_db"
    TABLE    = "creditcard"
    CRAWLER  = "fraud-detection-crawler"
    RESULTS  = "s3://athena-results-miguel/"

    def __init__(self):
        self.s3     = S3Layer(self.BUCKET, self.REGION)
        self.glue   = GlueLayer(self.DATABASE, self.TABLE, self.CRAWLER, self.REGION)
        self.athena = AthenaLayer(self.DATABASE, self.RESULTS, self.REGION)

    def run(self):
        print("\nFraud Detection ETL Pipeline — Miguel Alonso\n")
        self.s3.report()
        self.glue.report()
        self.athena.report()
        print("\n" + "=" * 60)
        print("Pipeline complete.")
        print("=" * 60)


# ══════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    pipeline = FraudDetectionPipeline()
    pipeline.run()