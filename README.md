# OrderFlow

API to analytics data pipeline. Pulls products, users and carts from
[DummyJSON](https://dummyjson.com), archives every raw response to S3, validates
and loads into PostgreSQL, and refreshes reporting tables — orchestrated by
Airflow.

The happy path is the easy part. Most of what is here exists for the rerun, the
partial failure and the question "is this number wrong, and since when?"

```
                    ┌──────────────┐
  DummyJSON  ──────▶│   ingest     │──────▶  S3  raw/<resource>/dt=<date>/<run_id>.json
   (paginated,      └──────────────┘         │
    retried)                                 │  every downstream stage reads
                                             │  the snapshot, never the API
                                             ▼
                    ┌──────────────┐    ┌──────────────┐
                    │  validate    │───▶│ rejected_rows│  (reason + full payload)
                    └──────┬───────┘    └──────────────┘
                           │
                           ▼
                    ┌──────────────┐
                    │ stg_* tables │──▶ upsert ──▶ products / users / carts / cart_items
                    └──────────────┘
                           │
                           ▼
                    ┌──────────────────────────────────┐
                    │ rpt_category_performance          │
                    │ rpt_top_products                  │
                    └──────────────────────────────────┘
```

## Quickstart

```bash
docker compose up -d --build
docker compose logs airflow | grep -i "password"   # Airflow admin login
```

Airflow at http://localhost:8080, MinIO console at http://localhost:9001
(`minioadmin` / `minioadmin`). Unpause the `orderflow` DAG, or trigger it.

To run the same pipeline without Airflow:

```bash
docker compose up -d postgres minio
pip install -r requirements.txt
set -a && source .env.example && set +a          # or export the vars yourself
python -m orderflow                              # all three resources
python -m orderflow --resource carts             # just one
```

## Design decisions

**Raw snapshots are the source of truth for replay.** Every response is written
to `s3://$BUCKET/raw/<resource>/dt=<date>/<run_id>.json` before anything parses
it. Loading reads the snapshot, not the API. A wrong transform discovered two
weeks later is a backfill, not an incident — and the source is never asked for
history it does not promise to keep. Replay a specific run with:

```bash
python -m orderflow --resource carts --snapshot raw/carts/dt=2026-09-16/<run_id>.json
```

**Every write is rerunnable.** Airflow retries, people clear tasks, backfills
replay a week. Loads land in a truncated `stg_*` table and move into the target
with `INSERT ... ON CONFLICT DO UPDATE`, so running a task twice leaves the same
rows as running it once. Rejects for a run are cleared before being rewritten,
for the same reason.

**Cart lines are keyed by line, not by product.** The first version keyed
`cart_items` on `(cart_id, product_id)`, and validation duly rejected every cart
that listed one product on two lines — 12 of 208, about 6% of revenue. The
rejects table is what caught it: pulling one of those carts out of its raw
snapshot showed it reconciled *exactly*, to the cent and to the unit. The source
models a cart as a list of lines, and a shopper can add the same thing twice. The
data was right and the primary key was wrong, so the key changed and the rule
went away. Had those lines been quietly merged instead of logged, the 6% would
still be wrong today and nothing would ever have said so.

**Cart lines are replaced, not upserted.** A cart can lose a line between runs.
Upserting only adds or updates, so a removed line would sit in the warehouse
forever, quietly inflating every revenue figure that touches it. `cart_items`
for the incoming cart ids are deleted and reinserted inside the load transaction.

**Bad rows are recorded, never dropped.** Validation checks required fields,
duplicate ids within a batch, and whether a cart's line items reconcile against
its header totals (`price × quantity`, the line sum against `total`, and the
`totalQuantity` / `totalProducts` counters). Failures go to `rejected_rows` with
the reason *and the full payload* — a reason alone tells you a row failed, not
whether the source or the rule was wrong. On the current dataset every one of
the 610 records loads clean; the rejects table earned its keep by being wrong
once, which is the point.

**Money is `NUMERIC`.** Cart totals are reconciled to the cent; binary floating
point loses that argument. The one cent of tolerance in
`validate.CART_TOTAL_TOLERANCE` absorbs the source's own per-line rounding.

**Sensitive fields are never loaded.** The users endpoint serves `password`,
`ssn`, `ein`, bank card numbers and crypto wallets. None are selected in
`load._shape_user`. The cheapest way to not leak a field is to never load it,
synthetic or not.

**The run log survives the rollback.** `pipeline_runs` is written on its own
connection, so when a load transaction rolls back the row explaining why is
still there.

## Tables

| Table | Purpose |
|---|---|
| `products`, `users`, `carts`, `cart_items` | loaded warehouse tables |
| `stg_*` | one batch, truncated per run, inspectable after a failure |
| `rejected_rows` | run id, resource, reason, full payload |
| `pipeline_runs` | per stage: status, rows fetched / loaded / rejected, snapshot key |
| `rpt_category_performance`, `rpt_top_products` | reporting tables, rebuilt each run |

## Tests

```bash
pytest
```

Covers the validation rules and the shaping layer — no database, no network,
because that logic is pure functions over dicts. That is most of why it is.

## Layout

```
orderflow/
  config.py     environment-driven settings
  db.py         engine, schema; stg_* derived from targets so they cannot drift
  extract.py    pagination, retries, S3 archive + replay
  validate.py   the rules
  load.py       shaping, staging, upserts, rejects, run log
  report.py     reporting table refresh
  __main__.py   run the pipeline without Airflow
dags/
  orderflow_dag.py
```

## Known limits

- Staging tables are shared, so the DAG is capped at one active run
  (`max_active_runs=1`). Running in parallel would need run-scoped staging names.
- Reporting tables are truncate-and-rebuild. They are small and derived; an
  incremental merge would be more moving parts guarding less.
