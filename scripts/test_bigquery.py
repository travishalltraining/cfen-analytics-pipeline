import os
from datetime import datetime, timezone

from google.cloud import bigquery

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET_ID = "cfen_analytics"
TABLE_ID = "pipeline_test"

client = bigquery.Client(project=PROJECT_ID)

table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

rows = [
    {
        "test_time": datetime.now(timezone.utc).isoformat(),
        "source": "github_actions",
        "message": "GitHub successfully wrote to BigQuery.",
    }
]

errors = client.insert_rows_json(table_ref, rows)

if errors:
    raise RuntimeError(f"BigQuery insert failed: {errors}")

print("Success: wrote test row to BigQuery.")
