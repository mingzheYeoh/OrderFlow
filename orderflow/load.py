"""Load a raw snapshot into PostgreSQL through staging tables and upserts.

Rerunning a task is the normal case, not the exception: Airflow retries, someone
clears a task, a backfill replays a week. Every write here is therefore keyed so
that running it twice leaves the same rows behind as running it once.
"""

import logging
from contextlib import contextmanager

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from . import db, extract, validate

log = logging.getLogger(__name__)


# --- shaping ----------------------------------------------------------------
# Source field -> column. Explicit rather than a generic camelCase converter,
# because this is also where fields get deliberately left behind.


def _shape_product(row, run_id):
    return {
        "id": row["id"],
        "title": row["title"],
        "category": row.get("category"),
        "brand": row.get("brand"),
        "price": row.get("price"),
        "discount_percentage": row.get("discountPercentage"),
        "rating": row.get("rating"),
        "stock": row.get("stock"),
        "sku": row.get("sku"),
        "thumbnail": row.get("thumbnail"),
        "source_run_id": run_id,
    }


def _shape_user(row, run_id):
    # password, ssn, ein, bank and crypto are present in the payload and are
    # deliberately not selected. Synthetic today is still a habit tomorrow.
    address = row.get("address") or {}
    company = row.get("company") or {}
    return {
        "id": row["id"],
        "first_name": row.get("firstName"),
        "last_name": row.get("lastName"),
        "email": row.get("email"),
        "gender": row.get("gender"),
        "age": row.get("age"),
        "city": address.get("city"),
        "state": address.get("state"),
        "country": address.get("country"),
        "company": company.get("name"),
        "role": row.get("role"),
        "source_run_id": run_id,
    }


def _shape_cart(row, run_id):
    return {
        "id": row["id"],
        "user_id": row.get("userId"),
        "total": row.get("total"),
        "discounted_total": row.get("discountedTotal"),
        "total_products": row.get("totalProducts"),
        "total_quantity": row.get("totalQuantity"),
        "source_run_id": run_id,
    }


def _shape_cart_items(cart, run_id):
    # line_no, not the product id, is what makes a line unique: one cart can
    # carry the same product on two lines and still reconcile to the cent.
    return [
        {
            "cart_id": cart["id"],
            "line_no": line_no,
            "product_id": item["id"],
            "title": item.get("title"),
            "price": item.get("price"),
            "quantity": item.get("quantity"),
            "total": item.get("total"),
            "discount_percentage": item.get("discountPercentage"),
            "discounted_total": item.get("discountedTotal"),
            "source_run_id": run_id,
        }
        for line_no, item in enumerate(cart.get("products") or [], start=1)
    ]


SHAPERS = {
    "products": _shape_product,
    "users": _shape_user,
    "carts": _shape_cart,
}


# --- run log ----------------------------------------------------------------


@contextmanager
def run_log(run_id, resource, stage, snapshot_key=None):
    """Record a stage attempt, on its own connection.

    Separate from the load transaction on purpose: if the load rolls back, the
    log entry explaining why must survive the rollback.
    """
    engine = db.engine()
    with engine.begin() as conn:
        log_id = conn.execute(
            db.pipeline_runs.insert()
            .values(run_id=run_id, resource=resource, stage=stage,
                    status="running", snapshot_key=snapshot_key)
            .returning(db.pipeline_runs.c.id)
        ).scalar_one()

    stats = {"rows_fetched": None, "rows_loaded": None, "rows_rejected": None}
    try:
        yield stats
    except Exception as exc:
        _finish(log_id, "failed", stats, f"{type(exc).__name__}: {exc}"[:2000])
        raise
    _finish(log_id, "succeeded", stats)


def _finish(log_id, status, stats, message=None):
    with db.engine().begin() as conn:
        conn.execute(
            db.pipeline_runs.update()
            .where(db.pipeline_runs.c.id == log_id)
            .values(status=status, finished_at=func.now(), message=message, **stats)
        )


# --- writes -----------------------------------------------------------------


def _stage(conn, name, rows):
    """Refill a staging table. Truncated first so a shorter rerun cannot leave
    rows from the previous attempt behind."""
    staging = db.STAGING[name]
    conn.execute(delete(staging))
    if rows:
        conn.execute(staging.insert(), rows)
    return staging


def _upsert_from_staging(conn, name):
    target = db.TARGETS[name]
    staging = db.STAGING[name]
    columns = [c.name for c in staging.columns]
    keys = [c.name for c in target.primary_key.columns]

    stmt = pg_insert(target).from_select(
        columns, select(*[staging.c[c] for c in columns])
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=keys,
        set_={c: stmt.excluded[c] for c in columns if c not in keys}
        | {"loaded_at": func.now()},
    )
    return conn.execute(stmt).rowcount


def _replace_cart_items(conn, carts, run_id):
    """Cart lines are replaced, not upserted.

    A cart can lose a line between runs. Upserting only ever adds or updates, so
    the removed line would sit in the warehouse forever, quietly inflating every
    revenue number that touches it.
    """
    rows = [item for cart in carts for item in _shape_cart_items(cart, run_id)]
    staging = _stage(conn, "cart_items", rows)
    if not rows:
        return 0

    target = db.TARGETS["cart_items"]
    conn.execute(
        delete(target).where(
            target.c.cart_id.in_(select(staging.c.cart_id).distinct())
        )
    )
    columns = [c.name for c in staging.columns]
    conn.execute(
        target.insert().from_select(columns, select(*[staging.c[c] for c in columns]))
    )
    return len(rows)


def _record_rejects(conn, run_id, resource, rejects):
    # Clear this run's previous rejects first, so a retry replaces them rather
    # than stacking a second copy.
    conn.execute(
        delete(db.rejected_rows).where(
            db.rejected_rows.c.run_id == run_id,
            db.rejected_rows.c.resource == resource,
        )
    )
    if not rejects:
        return
    conn.execute(
        db.rejected_rows.insert(),
        [
            {
                "run_id": run_id,
                "resource": r.resource,
                "source_id": None if r.source_id is None else str(r.source_id),
                "reason": r.reason,
                "payload": r.payload,
            }
            for r in rejects
        ],
    )


def load(resource, snapshot_key, run_id):
    """Validate a snapshot and load it. Returns the stage's row counts."""
    # Reading the snapshot happens inside the log context: an unreadable or
    # missing key is exactly the failure the run log exists to explain.
    with run_log(run_id, resource, "load", snapshot_key) as stats:
        rows = extract.read_snapshot(snapshot_key)
        clean, rejects = validate.validate(resource, rows)
        stats["rows_fetched"] = len(rows)
        stats["rows_rejected"] = len(rejects)

        with db.engine().begin() as conn:
            _record_rejects(conn, run_id, resource, rejects)
            _stage(conn, resource, [SHAPERS[resource](r, run_id) for r in clean])
            stats["rows_loaded"] = _upsert_from_staging(conn, resource)
            if resource == "carts":
                _replace_cart_items(conn, clean, run_id)

    log.info(
        "%s: loaded %s, rejected %s of %s rows",
        resource, stats["rows_loaded"], stats["rows_rejected"], stats["rows_fetched"],
    )
    return stats
