"""Utils test."""

from datetime import datetime

from bs4 import BeautifulSoup


def test_normalize_key_canonical_variants(utils_mod):
    nk = utils_mod.normalize_key

    assert nk("Batch Number") == "batch_number"
    assert nk("batch number:") == "batch_number"
    assert nk("Batch No.") == "batch_number"
    assert nk("PRODUCT NAME") == "product_name"
    assert nk("Expiry Date") == "expiry_date"
    assert nk("Stated Manufacturer") == "stated_manufacturer"


def test_parse_date_handles_listing_and_month_year(utils_mod):
    parse_date = utils_mod.parse_date

    # NAFDAC listing style
    assert parse_date("15-Oct-25") == datetime(2025, 10, 15)
    assert parse_date("15-October-2025") == datetime(2025, 10, 15)

    # ISO-ish
    assert parse_date("2026-01-09") == datetime(2026, 1, 9)

    # Month/year with - or /
    assert parse_date("10-2020") == datetime(2020, 10, 1)
    assert parse_date("10/2020") == datetime(2020, 10, 1)

    # None/empty
    assert parse_date("") is None
    assert parse_date(None) is None


def test_table_to_grid_supports_thead_and_th(utils_mod):
    html = """
    <table>
      <thead>
        <tr><th>Product Name</th><th>Batch Number</th><th>Expiry Date</th></tr>
      </thead>
      <tbody>
        <tr><td>Darzalex</td><td>PKS1F01</td><td>10-2026</td></tr>
      </tbody>
    </table>
    """
    soup = BeautifulSoup(html, "html.parser")
    grid = utils_mod.table_to_grid(soup.select_one("table"))

    assert grid[0] == ["Product Name", "Batch Number", "Expiry Date"]
    assert grid[1] == ["Darzalex", "PKS1F01", "10-2026"]
