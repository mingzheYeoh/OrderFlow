"""Run the pipeline without Airflow.

The same functions the DAG calls, in the same order. Useful for a first run, and
for reproducing a production failure locally with `--snapshot` against the
archived key from the run that broke.
"""

import argparse
import logging
import sys
import uuid
from datetime import date

from . import config, db, extract, load, report


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="python -m orderflow",
        description="Ingest, validate and load one or more resources.",
    )
    parser.add_argument("--resource", choices=(*config.RESOURCES, "all"), default="all")
    parser.add_argument("--run-id", help="defaults to a fresh dated id")
    parser.add_argument("--run-date", help="snapshot partition, defaults to today")
    parser.add_argument(
        "--snapshot",
        help="replay this archived S3 key instead of calling the API "
             "(requires a single --resource)",
    )
    parser.add_argument("--skip-report", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s"
    )

    resources = config.RESOURCES if args.resource == "all" else (args.resource,)
    if args.snapshot and len(resources) != 1:
        parser.error("--snapshot replays one resource; pass --resource too")

    run_date = args.run_date or date.today().isoformat()
    run_id = args.run_id or f"{run_date}T{uuid.uuid4().hex[:8]}"
    logging.info("run_id=%s run_date=%s", run_id, run_date)

    db.init_schema()
    for resource in resources:
        key = args.snapshot or extract.ingest(resource, run_date, run_id)
        load.load(resource, key, run_id)

    if not args.skip_report:
        report.refresh(run_id)

    logging.info("done: run_id=%s", run_id)
    return 0


if __name__ == "__main__":
    sys.exit(main())
