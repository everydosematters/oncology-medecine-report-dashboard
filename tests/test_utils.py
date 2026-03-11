"""Unit tests for src.scrapers.utils."""

from datetime import datetime
from pathlib import Path

import pytest

# --- absolutize ---


def test_absolutize_relative_path(utils_mod):
    assert (
        utils_mod.absolutize("https://example.com/page/", "sub") == "https://example.com/page/sub"
    )
    assert (
        utils_mod.absolutize("https://example.com/", "recalls/123")
        == "https://example.com/recalls/123"
    )


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


# --- extract_drug_token ---


def test_extract_drug_token_empty(utils_mod):
    assert utils_mod.extract_drug_token("") is None
    assert utils_mod.extract_drug_token(None) is None


def test_extract_drug_token_removes_parentheses_keeps_content(utils_mod):
    token = utils_mod.extract_drug_token("Darzalex (Daratumumab)")
    assert "darzalex" == token


def test_extract_drug_token_lowercase(utils_mod):
    token = utils_mod.extract_drug_token("HERCEPTIN Trastuzumab")
    assert "herceptin" == token


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
