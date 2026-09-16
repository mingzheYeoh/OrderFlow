"""Validation rules applied between the raw snapshot and the warehouse.

A row either loads or is written to `rejected_rows` with the reason it failed.
Nothing is dropped quietly: a pipeline that discards rows without saying so
looks healthy in every dashboard right up until someone reconciles by hand.
"""

from dataclasses import dataclass
from typing import Any

# Only the fields the warehouse actually depends on. Requiring every field the
# source happens to send turns a harmless upstream change into an outage.
REQUIRED = {
    "products": ("id", "title", "price"),
    "users": ("id", "email"),
    "carts": ("id", "userId", "products", "total"),
}

# The source rounds per-line discounted totals, so cart-level sums drift by a
# cent. Anything wider than that means the line items and the recorded header
# genuinely disagree, which is a data problem rather than a rounding artefact.
CART_TOTAL_TOLERANCE = 0.01


@dataclass(frozen=True)
class Reject:
    resource: str
    source_id: Any
    reason: str
    payload: dict


def validate(resource, rows):
    """Split `rows` into (clean, rejects).

    Stops at the first failed rule per row: the first reason is the actionable
    one, and a row failing four checks is still one bad row.
    """
    clean, rejects, seen = [], [], set()

    for row in rows:
        reason = _first_failure(resource, row, seen)
        if reason is not None:
            rejects.append(Reject(resource, row.get("id"), reason, row))
            continue
        seen.add(row["id"])
        clean.append(row)

    return clean, rejects


def _first_failure(resource, row, seen):
    if not isinstance(row, dict):
        return f"expected an object, got {type(row).__name__}"

    missing = [k for k in REQUIRED[resource] if row.get(k) in (None, "", [])]
    if missing:
        return f"missing required field(s): {', '.join(missing)}"

    if row["id"] in seen:
        # Within-batch duplicates, not warehouse duplicates: the upsert handles
        # re-seeing an id across runs, but two rows with one id in a single
        # batch means the source contradicted itself and the upsert would just
        # pick whichever landed last.
        return f"duplicate id {row['id']} within the same batch"

    if resource == "carts":
        return check_cart_totals(row)

    return None


def check_cart_totals(cart):
    """Reconcile a cart's line items against its recorded header totals.

    Returns a reason string, or None when the cart is internally consistent.
    """
    items = cart.get("products") or []
    if not items:
        return "cart has no line items"

    # Note: the same product appearing on two lines is NOT an error. Those carts
    # reconcile exactly against their header totals -- the source models a cart
    # as a list of lines, not a set of products, and `cart_items` is keyed by
    # line to match. Rejecting them cost ~6% of carts before the key was fixed.
    for item in items:
        missing = [k for k in ("id", "price", "quantity", "total") if item.get(k) is None]
        if missing:
            return f"line item {item.get('id')}: missing {', '.join(missing)}"

        expected = round(item["price"] * item["quantity"], 2)
        if abs(expected - item["total"]) > CART_TOTAL_TOLERANCE:
            return (
                f"line item {item['id']}: price*quantity is {expected} "
                f"but total is {item['total']}"
            )

    line_sum = round(sum(i["total"] for i in items), 2)
    if abs(line_sum - cart["total"]) > CART_TOTAL_TOLERANCE:
        return f"line items sum to {line_sum} but cart total is {cart['total']}"

    quantity = sum(i["quantity"] for i in items)
    if cart.get("totalQuantity") is not None and quantity != cart["totalQuantity"]:
        return (
            f"line items hold {quantity} units "
            f"but totalQuantity is {cart['totalQuantity']}"
        )

    if cart.get("totalProducts") is not None and len(items) != cart["totalProducts"]:
        return (
            f"cart has {len(items)} line items "
            f"but totalProducts is {cart['totalProducts']}"
        )

    return None
