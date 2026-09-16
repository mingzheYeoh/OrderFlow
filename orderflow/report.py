"""Rebuild the reporting tables from the loaded warehouse tables.

Plain SQL rather than the Core expression language: these are read models whose
whole value is being readable by whoever asks where a number came from.

Truncate-and-insert inside one transaction. The tables are small and derived, so
an incremental merge would be more moving parts guarding less.
"""

import logging

from sqlalchemy import text

from . import db, load

log = logging.getLogger(__name__)

# Product stats and cart stats are aggregated separately before being joined.
# Doing it in one pass would fan products out across their cart lines and turn
# AVG(price) into an average weighted by how often something was ordered.
CATEGORY_PERFORMANCE = """
INSERT INTO rpt_category_performance
    (category, product_count, avg_price, units_in_carts, cart_revenue)
WITH product_stats AS (
    SELECT category,
           COUNT(*)                AS product_count,
           ROUND(AVG(price), 2)    AS avg_price
    FROM products
    GROUP BY category
),
cart_stats AS (
    SELECT p.category,
           SUM(ci.quantity) AS units,
           SUM(ci.total)    AS revenue
    FROM cart_items ci
    JOIN products p ON p.id = ci.product_id
    GROUP BY p.category
)
SELECT ps.category,
       ps.product_count,
       ps.avg_price,
       COALESCE(cs.units, 0),
       COALESCE(cs.revenue, 0)
FROM product_stats ps
LEFT JOIN cart_stats cs USING (category)
"""

# LEFT JOIN, not JOIN: a cart line whose product never loaded still represents
# revenue. Inner-joining it away would make the totals quietly disagree with
# the carts table.
TOP_PRODUCTS = """
INSERT INTO rpt_top_products (product_id, title, category, units, revenue)
SELECT ci.product_id,
       COALESCE(MAX(p.title), MAX(ci.title)),
       MAX(p.category),
       SUM(ci.quantity),
       SUM(ci.total)
FROM cart_items ci
LEFT JOIN products p ON p.id = ci.product_id
GROUP BY ci.product_id
ORDER BY SUM(ci.total) DESC
LIMIT 50
"""

REFRESHES = {
    "rpt_category_performance": CATEGORY_PERFORMANCE,
    "rpt_top_products": TOP_PRODUCTS,
}


def refresh(run_id):
    """Rebuild every reporting table. Safe to rerun; safe to run twice."""
    with load.run_log(run_id, None, "refresh_reporting") as stats:
        rows = 0
        with db.engine().begin() as conn:
            for table, statement in REFRESHES.items():
                # Table names come from the dict above, never from input.
                conn.execute(text(f"TRUNCATE {table}"))
                rows += conn.execute(text(statement)).rowcount
        stats["rows_loaded"] = rows

    log.info("refreshed %s reporting tables, %s rows", len(REFRESHES), rows)
    return stats
