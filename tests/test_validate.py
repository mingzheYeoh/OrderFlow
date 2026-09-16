"""The rules are the part worth testing: everything else is I/O.

Run with `pytest` from the repo root. No database and no network -- validation
and shaping are pure functions over dicts, which is most of why they are.
"""

import pytest

from orderflow import load, validate


def cart(**overrides):
    base = {
        "id": 1,
        "userId": 7,
        "total": 60.0,
        "discountedTotal": 55.0,
        "totalProducts": 2,
        "totalQuantity": 3,
        "products": [
            {"id": 10, "title": "A", "price": 10.0, "quantity": 2, "total": 20.0},
            {"id": 11, "title": "B", "price": 40.0, "quantity": 1, "total": 40.0},
        ],
    }
    return base | overrides


def test_clean_batch_passes():
    clean, rejects = validate.validate("carts", [cart()])
    assert rejects == []
    assert len(clean) == 1


def test_missing_required_field_names_the_field():
    rows = [{"id": 5, "title": "A"}]  # products require a price
    clean, rejects = validate.validate("products", rows)
    assert clean == []
    assert "price" in rejects[0].reason


def test_duplicate_id_within_batch_is_rejected_once():
    rows = [{"id": 5, "title": "A", "price": 1}, {"id": 5, "title": "B", "price": 2}]
    clean, rejects = validate.validate("products", rows)
    # First wins, second is rejected: dropping both would lose a good row.
    assert [r["title"] for r in clean] == ["A"]
    assert "duplicate id 5" in rejects[0].reason


def test_line_item_that_does_not_multiply_out_is_rejected():
    bad = cart()
    bad["products"][0]["total"] = 25.0  # 10.00 * 2 != 25.00
    _, rejects = validate.validate("carts", [bad])
    assert "price*quantity" in rejects[0].reason


def test_line_items_that_do_not_sum_to_the_header_are_rejected():
    _, rejects = validate.validate("carts", [cart(total=99.0)])
    assert "cart total is 99.0" in rejects[0].reason


def test_penny_rounding_is_tolerated():
    # The source rounds per line; rejecting on a cent would reject real carts.
    assert validate.check_cart_totals(cart(total=60.01)) is None


def test_rounding_beyond_tolerance_is_not():
    assert validate.check_cart_totals(cart(total=60.05)) is not None


def repeated_product_cart():
    """Real shape from the source: one product, two lines, totals still reconcile."""
    return cart(
        totalProducts=2,
        totalQuantity=4,
        total=40.0,
        products=[
            {"id": 10, "title": "A", "price": 10.0, "quantity": 2, "total": 20.0},
            {"id": 10, "title": "A", "price": 10.0, "quantity": 2, "total": 20.0},
        ],
    )


def test_repeated_product_line_is_valid_when_it_reconciles():
    # Rejecting these cost ~6% of carts until cart_items was rekeyed by line.
    # The data was right; the primary key was wrong.
    _, rejects = validate.validate("carts", [repeated_product_cart()])
    assert rejects == []


def test_repeated_product_lines_get_distinct_keys():
    lines = load._shape_cart_items(repeated_product_cart(), run_id="test")
    keys = [(line["cart_id"], line["line_no"]) for line in lines]
    assert len(set(keys)) == len(lines) == 2
    assert {line["product_id"] for line in lines} == {10}


def test_quantity_header_mismatch_is_rejected():
    _, rejects = validate.validate("carts", [cart(totalQuantity=99)])
    assert "totalQuantity is 99" in rejects[0].reason


def test_empty_cart_is_rejected():
    _, rejects = validate.validate("carts", [cart(products=[])])
    assert rejects[0].reason


def test_rejects_keep_the_payload():
    # A reason without the row tells you something failed, not what.
    _, rejects = validate.validate("carts", [cart(total=99.0)])
    assert rejects[0].payload["id"] == 1
    assert rejects[0].source_id == 1


@pytest.mark.parametrize("field", ["password", "ssn", "ein", "bank", "crypto"])
def test_sensitive_user_fields_are_never_shaped_into_a_column(field):
    row = {
        "id": 1,
        "firstName": "A",
        "lastName": "B",
        "email": "a@b.c",
        "password": "hunter2",
        "ssn": "123-45-6789",
        "ein": "99-9999999",
        "bank": {"cardNumber": "4111111111111111"},
        "crypto": {"wallet": "0xdead"},
        "address": {"city": "KL", "state": "WP", "country": "MY"},
        "company": {"name": "Acme"},
    }
    shaped = load._shape_user(row, run_id="test")
    assert field not in shaped
    assert "hunter2" not in str(shaped.values())
