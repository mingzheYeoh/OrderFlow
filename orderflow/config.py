"""Runtime configuration, read from the environment.

Every setting defaults to the local docker-compose stack, so a fresh clone runs
without a .env file. Nothing here is imported at module scope by the DAG, which
keeps Airflow's parser from needing credentials just to list tasks.
"""

import os

# --- source API -------------------------------------------------------------
API_BASE = os.getenv("ORDERFLOW_API_BASE", "https://dummyjson.com")
PAGE_SIZE = int(os.getenv("ORDERFLOW_PAGE_SIZE", "30"))
HTTP_TIMEOUT = float(os.getenv("ORDERFLOW_HTTP_TIMEOUT", "10"))
HTTP_RETRIES = int(os.getenv("ORDERFLOW_HTTP_RETRIES", "4"))

# --- warehouse --------------------------------------------------------------
DATABASE_URL = os.getenv(
    "ORDERFLOW_DATABASE_URL",
    "postgresql+psycopg2://orderflow:orderflow@localhost:55432/orderflow",
)

# --- raw snapshot archive ---------------------------------------------------
S3_BUCKET = os.getenv("ORDERFLOW_S3_BUCKET", "orderflow-raw")
# Set to a MinIO address for local runs; leave unset to talk to real AWS S3.
S3_ENDPOINT_URL = os.getenv("ORDERFLOW_S3_ENDPOINT_URL") or None
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

RESOURCES = ("products", "users", "carts")
