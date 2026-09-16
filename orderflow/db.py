"""Warehouse schema and engine.

Money is Numeric, never Float: cart totals are reconciled against line items to
the cent, and binary floating point loses that argument.

Staging tables are derived from their target rather than declared twice, so a
column added to `products` cannot drift out of sync with `stg_products`.
"""

import functools

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Integer,
    MetaData,
    Numeric,
    PrimaryKeyConstraint,
    Table,
    Text,
    create_engine,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB

from . import config

metadata = MetaData()


@functools.lru_cache(maxsize=1)
def engine():
    """Process-wide engine. Airflow runs each task in its own process, so the
    pool is per-task and does not need sharing across workers."""
    return create_engine(config.DATABASE_URL, pool_pre_ping=True, future=True)


# --- target tables ----------------------------------------------------------

products = Table(
    "products",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("title", Text, nullable=False),
    Column("category", Text),
    Column("brand", Text),
    Column("price", Numeric(12, 2)),
    Column("discount_percentage", Numeric(6, 2)),
    Column("rating", Numeric(4, 2)),
    Column("stock", Integer),
    Column("sku", Text),
    Column("thumbnail", Text),
    Column("source_run_id", Text),
    Column("loaded_at", DateTime(timezone=True), server_default=func.now()),
)

# The source exposes password, ssn, ein, bank card numbers and crypto wallets.
# None of it is selected below: the cheapest way to not leak a field is to never
# load it, even when the data is synthetic.
users = Table(
    "users",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("first_name", Text),
    Column("last_name", Text),
    Column("email", Text),
    Column("gender", Text),
    Column("age", Integer),
    Column("city", Text),
    Column("state", Text),
    Column("country", Text),
    Column("company", Text),
    Column("role", Text),
    Column("source_run_id", Text),
    Column("loaded_at", DateTime(timezone=True), server_default=func.now()),
)

carts = Table(
    "carts",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("user_id", Integer),
    Column("total", Numeric(12, 2)),
    Column("discounted_total", Numeric(12, 2)),
    Column("total_products", Integer),
    Column("total_quantity", Integer),
    Column("source_run_id", Text),
    Column("loaded_at", DateTime(timezone=True), server_default=func.now()),
)

# Keyed by line, not by product. The source legitimately puts the same product
# on two lines of one cart, and those carts reconcile perfectly against their
# header totals. Keying on (cart_id, product_id) would make correct data look
# like a constraint violation.
cart_items = Table(
    "cart_items",
    metadata,
    Column("cart_id", Integer, nullable=False),
    Column("line_no", Integer, nullable=False),
    Column("product_id", Integer, nullable=False),
    Column("title", Text),
    Column("price", Numeric(12, 2)),
    Column("quantity", Integer),
    Column("total", Numeric(12, 2)),
    Column("discount_percentage", Numeric(6, 2)),
    Column("discounted_total", Numeric(12, 2)),
    Column("source_run_id", Text),
    Column("loaded_at", DateTime(timezone=True), server_default=func.now()),
    PrimaryKeyConstraint("cart_id", "line_no"),
)

# --- operational tables -----------------------------------------------------

# Rejected rows are kept whole. A reason string without the payload tells you a
# row failed but not whether the source or the rule was wrong.
rejected_rows = Table(
    "rejected_rows",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("run_id", Text, nullable=False),
    Column("resource", Text, nullable=False),
    Column("source_id", Text),
    Column("reason", Text, nullable=False),
    Column("payload", JSONB),
    Column("rejected_at", DateTime(timezone=True), server_default=func.now()),
)

pipeline_runs = Table(
    "pipeline_runs",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("run_id", Text, nullable=False),
    Column("resource", Text),
    Column("stage", Text, nullable=False),
    Column("status", Text, nullable=False),
    Column("rows_fetched", Integer),
    Column("rows_loaded", Integer),
    Column("rows_rejected", Integer),
    Column("snapshot_key", Text),
    Column("message", Text),
    Column("started_at", DateTime(timezone=True), server_default=func.now()),
    Column("finished_at", DateTime(timezone=True)),
)

# --- reporting tables -------------------------------------------------------

rpt_category_performance = Table(
    "rpt_category_performance",
    metadata,
    Column("category", Text, primary_key=True),
    Column("product_count", Integer),
    Column("avg_price", Numeric(12, 2)),
    Column("units_in_carts", Integer),
    Column("cart_revenue", Numeric(14, 2)),
    Column("refreshed_at", DateTime(timezone=True), server_default=func.now()),
)

rpt_top_products = Table(
    "rpt_top_products",
    metadata,
    Column("product_id", Integer, primary_key=True),
    Column("title", Text),
    Column("category", Text),
    Column("units", Integer),
    Column("revenue", Numeric(14, 2)),
    Column("refreshed_at", DateTime(timezone=True), server_default=func.now()),
)

TARGETS = {
    "products": products,
    "users": users,
    "carts": carts,
    "cart_items": cart_items,
}


def _staging(table):
    """Mirror `table` as an unconstrained stg_ table.

    No primary key on purpose: a batch carrying duplicate ids must land in
    staging so the upsert can be the thing that collapses them, rather than the
    insert blowing up halfway and leaving a partial batch behind.
    """
    return Table(
        f"stg_{table.name}",
        metadata,
        *[Column(c.name, c.type) for c in table.columns if c.name != "loaded_at"],
    )


STAGING = {name: _staging(t) for name, t in TARGETS.items()}


def init_schema():
    """Create anything missing. Safe to run on every DAG run."""
    metadata.create_all(engine())
