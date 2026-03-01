"""Unit tests for src.scrapers.utils."""

from datetime import datetime
from pathlib import Path

import pytest
from bs4 import BeautifulSoup


# --- clean_text ---


def test_clean_text_returns_none_for_empty_input(utils_mod):
    assert utils_mod.clean_text(None) is None
    assert utils_mod.clean_text("") is None


def test_clean_text_collapses_whitespace(utils_mod):
    assert utils_mod.clean_text("  hello   world  ") == "hello world"
    assert utils_mod.clean_text("a\t\n\tb") == "a b"


def test_clean_text_returns_none_for_whitespace_only(utils_mod):
    assert utils_mod.clean_text("   \t\n  ") is None


def test_clean_text_preserves_single_word(utils_mod):
    assert utils_mod.clean_text("Darzalex") == "Darzalex"


# --- cell_text ---


def test_cell_text_joins_nested_content(utils_mod):
    html = '<td><p>Line 1</p><p>Line 2</p></td>'
    soup = BeautifulSoup(html, "html.parser")
    cell = soup.select_one("td")
    assert utils_mod.cell_text(cell) == "Line 1 Line 2"


def test_cell_text_cleans_whitespace(utils_mod):
    html = '<td>  messy   text  </td>'
    soup = BeautifulSoup(html, "html.parser")
    cell = soup.select_one("td")
    assert utils_mod.cell_text(cell) == "messy text"


# --- normalize_key ---


def test_normalize_key_canonical_variants(utils_mod):
    nk = utils_mod.normalize_key

    assert nk("Batch Number") == "batch_number"
    assert nk("batch number:") == "batch_number"
    assert nk("Batch No.") == "batch_number"
    assert nk("PRODUCT NAME") == "product_name"
    assert nk("Expiry Date") == "expiry_date"
    assert nk("Stated Manufacturer") == "stated_manufacturer"


def test_normalize_key_empty_input(utils_mod):
    assert utils_mod.normalize_key("") is None
    assert utils_mod.normalize_key(None) is None


def test_normalize_key_partial_match(utils_mod):
    assert utils_mod.normalize_key("Product Name (Brand)") == "product_name"
    assert utils_mod.normalize_key("Batch number here") == "batch_number"


def test_normalize_key_return_none_fallback(utils_mod):
    assert utils_mod.normalize_key("Unknown Field", return_none=True) is None


def test_normalize_key_snake_case_fallback(utils_mod):
    assert utils_mod.normalize_key("Some Random Field") == "some_random_field"


# --- select_one_text ---


def test_select_one_text_found(utils_mod):
    html = "<div><h1>Title</h1><p>Body</p></div>"
    soup = BeautifulSoup(html, "html.parser")
    assert utils_mod.select_one_text(soup, "h1") == "Title"
    assert utils_mod.select_one_text(soup, "p") == "Body"


def test_select_one_text_not_found(utils_mod):
    html = "<div><p>Only this</p></div>"
    soup = BeautifulSoup(html, "html.parser")
    assert utils_mod.select_one_text(soup, "h1") is None


def test_select_one_text_empty_selector(utils_mod):
    soup = BeautifulSoup("<div>x</div>", "html.parser")
    assert utils_mod.select_one_text(soup, "") is None


# --- absolutize ---


def test_absolutize_relative_path(utils_mod):
    assert utils_mod.absolutize("https://example.com/page/", "sub") == "https://example.com/page/sub"
    assert utils_mod.absolutize("https://example.com/", "recalls/123") == "https://example.com/recalls/123"


def test_absolutize_absolute_url(utils_mod):
    base = "https://example.com/page/"
    href = "https://other.com/path"
    assert utils_mod.absolutize(base, href) == "https://other.com/path"


# --- parse_date ---


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


def test_parse_date_invalid_returns_none(utils_mod):
    assert utils_mod.parse_date("not-a-date") is None
    assert utils_mod.parse_date("32-13-2020") is None


# --- remove_trademarks ---


def test_remove_trademarks(utils_mod):
    assert utils_mod.remove_trademarks("Darzalex®") == "Darzalex"
    assert utils_mod.remove_trademarks("Drug™ and Pill©") == "Drug and Pill"
    assert utils_mod.remove_trademarks("Clean") == "Clean"


# --- table_to_grid ---


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


def test_table_to_grid_empty_table(utils_mod):
    html = "<table></table>"
    soup = BeautifulSoup(html, "html.parser")
    assert utils_mod.table_to_grid(soup.select_one("table")) == []


def test_table_to_grid_colspan(utils_mod):
    html = """
    <table>
      <tr><td colspan="2">Merged</td></tr>
      <tr><td>A</td><td>B</td></tr>
    </table>
    """
    soup = BeautifulSoup(html, "html.parser")
    grid = utils_mod.table_to_grid(soup.select_one("table"))
    assert grid[0] == ["Merged", "Merged"]
    assert grid[1] == ["A", "B"]


def test_table_to_grid_rowspan(utils_mod):
    html = """
    <table>
      <tr><td rowspan="2">X</td><td>1</td></tr>
      <tr><td>2</td></tr>
    </table>
    """
    soup = BeautifulSoup(html, "html.parser")
    grid = utils_mod.table_to_grid(soup.select_one("table"))
    assert grid[0] == ["X", "1"]
    assert grid[1] == ["X", "2"]


# --- get_first_name ---


def test_get_first_name_from_string(utils_mod):
    assert utils_mod.get_first_name("Darzalex Daratumumab") == "Darzalex"
    assert utils_mod.get_first_name("HERCEPTIN") == "HERCEPTIN"


def test_get_first_name_from_list(utils_mod):
    assert utils_mod.get_first_name(["Darzalex", "Herceptin"]) == "Darzalex"


def test_get_first_name_empty(utils_mod):
    assert utils_mod.get_first_name("") == ""
    assert utils_mod.get_first_name([]) == ""
    assert utils_mod.get_first_name(None) == ""


def test_get_first_name_removes_trademarks(utils_mod):
    assert utils_mod.get_first_name("Darzalex® Injection") == "Darzalex"


# --- normalize_drug_name ---


def test_normalize_drug_name_empty(utils_mod):
    assert utils_mod.normalize_drug_name("") == ""
    assert utils_mod.normalize_drug_name(None) == ""


def test_normalize_drug_name_removes_dosage(utils_mod):
    assert "500mg" not in utils_mod.normalize_drug_name("Darzalex 500mg")
    assert "10 ml" not in utils_mod.normalize_drug_name("Drug 10 ml")


def test_normalize_drug_name_removes_trademarks(utils_mod):
    assert "®" not in utils_mod.normalize_drug_name("Darzalex®")
    assert "™" not in utils_mod.normalize_drug_name("Drug™")


def test_normalize_drug_name_lowercase_and_clean(utils_mod):
    result = utils_mod.normalize_drug_name("HERCEPTIN 600mg/5ml")
    assert result == "herceptin"


# --- extract_drug_tokens ---


def test_extract_drug_tokens_empty(utils_mod):
    assert utils_mod.extract_drug_tokens("") == []
    assert utils_mod.extract_drug_tokens(None) == []


def test_extract_drug_tokens_removes_stopwords(utils_mod):
    tokens = utils_mod.extract_drug_tokens("tablets for injection and sodium")
    assert "tablets" not in tokens
    assert "for" not in tokens
    assert "injection" not in tokens
    assert "and" not in tokens
    assert "sodium" not in tokens


def test_extract_drug_tokens_removes_parentheses_keeps_content(utils_mod):
    tokens = utils_mod.extract_drug_tokens("Darzalex (Daratumumab)")
    assert "darzalex" in tokens
    assert "daratumumab" in tokens


def test_extract_drug_tokens_lowercase(utils_mod):
    tokens = utils_mod.extract_drug_tokens("HERCEPTIN Trastuzumab")
    assert "herceptin" in tokens
    assert "trastuzumab" in tokens


# --- read_json ---


def test_read_json_valid_file(utils_mod):
    path = Path(__file__).parent / "test_files" / "fda_result.json"
    data = utils_mod.read_json(str(path))
    assert isinstance(data, dict)
    assert "meta" in data
    assert "results" in data


def test_read_json_file_not_found(utils_mod):
    with pytest.raises(FileNotFoundError):
        utils_mod.read_json("/nonexistent/path.json")
