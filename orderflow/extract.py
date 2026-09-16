"""Fetch paginated JSON from the source API and archive it to S3 before use.

The archive is the point of this module. Anything downstream reads the snapshot,
never the API, so a wrong transform is a replay rather than an incident -- and
the source is never asked for history it does not promise to keep.
"""

import json
import logging

import boto3
import requests
from botocore.exceptions import ClientError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from . import config

log = logging.getLogger(__name__)


def session():
    """A requests Session that retries idempotent reads with backoff.

    urllib3's Retry already handles backoff, jitter and Retry-After; a hand
    rolled loop would be more code and worse at all three.
    """
    retry = Retry(
        total=config.HTTP_RETRIES,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        respect_retry_after_header=True,
    )
    s = requests.Session()
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def fetch_all(resource, http=None):
    """Return every record for `resource`, following skip/limit pagination."""
    http = http or session()
    url = f"{config.API_BASE}/{resource}"
    rows, skip, total = [], 0, None

    while total is None or skip < total:
        response = http.get(
            url,
            params={"limit": config.PAGE_SIZE, "skip": skip},
            timeout=config.HTTP_TIMEOUT,
        )
        response.raise_for_status()
        page = response.json()
        batch = page.get(resource) or []
        if not batch:
            # A `total` larger than the rows actually served would otherwise
            # spin here forever. Trust the page, not the header.
            log.warning("%s: empty page at skip=%s, stopping early", resource, skip)
            break
        rows.extend(batch)
        total = page.get("total", len(rows))
        skip += len(batch)
        log.info("%s: fetched %s/%s", resource, len(rows), total)

    return rows


# --- raw snapshot archive ---------------------------------------------------


def s3():
    return boto3.client(
        "s3",
        endpoint_url=config.S3_ENDPOINT_URL,
        region_name=config.AWS_REGION,
    )


def ensure_bucket(client=None):
    """Verify the archive bucket exists.

    Created automatically only when an endpoint override is set, i.e. against
    local MinIO. Silently conjuring buckets in a real AWS account is a surprise
    nobody asked for.
    """
    client = client or s3()
    try:
        client.head_bucket(Bucket=config.S3_BUCKET)
        return
    except ClientError as exc:
        if exc.response["Error"]["Code"] not in ("404", "NoSuchBucket", "403"):
            raise
        if not config.S3_ENDPOINT_URL:
            raise RuntimeError(
                f"S3 bucket {config.S3_BUCKET!r} is not reachable. Create it, or "
                f"set ORDERFLOW_S3_ENDPOINT_URL to run against local MinIO."
            ) from exc
    client.create_bucket(Bucket=config.S3_BUCKET)
    log.info("created bucket %s", config.S3_BUCKET)


def snapshot_key(resource, run_date, run_id):
    """Date-partitioned so two runs on the same day stay distinguishable."""
    return f"raw/{resource}/dt={run_date}/{run_id}.json"


def archive(resource, rows, run_date, run_id, client=None):
    client = client or s3()
    key = snapshot_key(resource, run_date, run_id)
    client.put_object(
        Bucket=config.S3_BUCKET,
        Key=key,
        Body=json.dumps(rows, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json",
    )
    log.info("archived %s rows to s3://%s/%s", len(rows), config.S3_BUCKET, key)
    return key


def read_snapshot(key, client=None):
    """The replay path: every downstream stage reads this, not the API."""
    client = client or s3()
    body = client.get_object(Bucket=config.S3_BUCKET, Key=key)["Body"].read()
    return json.loads(body)


def ingest(resource, run_date, run_id):
    """Fetch and archive one resource. Returns the snapshot key."""
    ensure_bucket()
    rows = fetch_all(resource)
    return archive(resource, rows, run_date, run_id)
