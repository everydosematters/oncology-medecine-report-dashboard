"""Tests for FDA USA scraper (AJAX-based workflow)."""

import json
from pathlib import Path
from typing import Any, Dict

import pytest
from bs4 import BeautifulSoup

from src.scrapers.fdausa import FDAUSAScraper


def _fda_ajax_config() -> Dict[str, Any]:
    """Minimal FDA_US config with AJAX listing (sources.json style)."""
    return {
        "source_id": "FDA_US",
        "source_country": "United States",
        "source_org": "U.S. Food and Drug Administration (FDA)",
        "base_url": "https://www.fda.gov",
        "ajax_url": "https://www.fda.gov/datatables/views/ajax",
        "request": {
            "headers": {
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts",
            }
        },
        "listing": {
            "type": "ajax",
            "ajax_url": "https://www.fda.gov/datatables/views/ajax",
            "params": {
                "search_api_fulltext": "",
                "view_name": "recall_solr_index",
                "view_display_id": "recall_datatable_block_1",
            },
            "pagination": {"page_size": 25, "max_pages": 1},
            "columns": {
                "date_index": 0,
                "brand_link_index": 1,
                "description_index": 2,
                "product_type_index": 3,
                "reason_index": 4,
                "company_index": 5,
            },
        },
        "detail_page": {
            "title_selector": "h1.page-title",
            "body_selector": "div.node__content",
        },
        "filters": {
            "require_oncology": True,
            "oncology_keywords": ["oncology", "cancer", "chemotherapy"],
        },
        "defaults": {"alert_type": "Recall / Safety Alert"},
    }


def _as_iso_date(val: Any) -> str:
    """Normalize publish_date for comparison."""
    if val is None:
        return ""
    if hasattr(val, "date"):
        try:
            return str(val.date())
        except Exception:
            pass
    return str(val)


def test_parse_anchor() -> None:
    """Test extraction of brand text and detail URL from anchor HTML."""
    cfg = _fda_ajax_config()
    scraper = FDAUSAScraper(cfg)

    html = '<a href="/safety/recalls-market-withdrawals-safety-alerts/oncodrug-x-recall">OncoDrug X</a>'
    brand, url = scraper._parse_anchor(html)
    assert brand == "OncoDrug X"
    assert "oncodrug-x-recall" in url
    assert url.startswith("https://www.fda.gov/")

    brand2, url2 = scraper._parse_anchor("no anchor here")
    assert brand2 is None
    assert url2 is None


def test_parse_date_from_time_html() -> None:
    """Test extraction of datetime from time element."""
    cfg = _fda_ajax_config()
    scraper = FDAUSAScraper(cfg)

    html = '<time datetime="2026-01-15T05:00:00Z">01/15/2026</time>'
    dt = scraper._parse_date_from_time_html(html)
    assert dt is not None
    assert "2026-01-15" in _as_iso_date(dt)


def test_standardize_mocked(monkeypatch) -> None:
    """End-to-end test with mocked AJAX and detail page scraping."""
    cfg = _fda_ajax_config()
    cfg["filters"]["require_oncology"] = False  # avoid filtering for test
    scraper = FDAUSAScraper(cfg)

    # Mock AJAX response rows (col_0=date, col_1=brand+link, col_2=desc, col_5=company)
    sample_rows = [
        [
            '<time datetime="2026-01-10T05:00:00Z">01/10/2026</time>',
            '<a href="/safety/recalls-market-withdrawals-safety-alerts/test-recall-1">Test Brand</a>',
            "Test product description",
            "Drugs",
            "Recall reason",
            "Test Company Inc.",
            "",
            "",
        ],
    ]

    def fake_fetch_ajax(self):
        return sample_rows

    detail_html = """
    <html><body>
      <h1 class="page-title">Test Brand Recall</h1>
      <div class="node__content"><p>Product recalled due to quality issue.</p></div>
    </body></html>
    """

    def fake_scrape(url: str = None):
        from bs4 import BeautifulSoup
        return {
            "final_url": url or "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/test-recall-1",
            "status_code": 200,
            "html": BeautifulSoup(detail_html, "html.parser"),
            "text": "detail page",
            "retrieved_at": "2026-01-10T00:00:00Z",
        }

    monkeypatch.setattr(FDAUSAScraper, "_fetch_ajax_listing", fake_fetch_ajax)
    monkeypatch.setattr(scraper, "scrape", fake_scrape)

    records = scraper.standardize()

    assert len(records) == 1
    r = records[0]
    assert r.source_id == "FDA_US"
    assert r.source_org == "U.S. Food and Drug Administration (FDA)"
    assert r.alert_type == "Recall / Safety Alert"
    assert "2026-01-10" in _as_iso_date(r.publish_date)
    assert r.brand_name == "Test Brand"
    assert r.product_name == "Test Brand"
    assert "Test Company" in str(r.manufacturer or "")
    assert r.record_id
    assert len(r.record_id) > 10


def test_parse_fda_usa_table_rowspan() -> None:
    """Test _parse_fda_usa_table with rowspan structure (product spans multiple lots)."""
    cfg = _fda_ajax_config()
    scraper = FDAUSAScraper(cfg)

    html = """
    <tbody>
        <tr>
            <td class="text-align-center" rowspan="5">Javygtor™ (Sapropterin)<br>Dihydrochloride) Powder for Oral<br>Solution 100 mg</td>
            <td class="text-align-center">T2202812</td>
            <td class="text-align-center">07/2025</td>
            <td class="text-align-center">43598-097-30</td>
        </tr>
        <tr>
            <td class="text-align-center">T2204053</td>
            <td class="text-align-center">10/2025</td>
            <td class="text-align-center">43598-097-30</td>
        </tr>
        <tr>
            <td class="text-align-center">T2300975</td>
            <td class="text-align-center">02/2026</td>
            <td class="text-align-center">43598-097-30</td>
        </tr>
    </tbody>
    """
    soup = BeautifulSoup(html, "html.parser")
    tbody = soup.find("tbody")
    result = scraper._parse_fda_usa_table(tbody)

    assert "product_name" in result
    assert "Javygtor" in result["product_name"][0]
    assert result["batch_number"] == ["T2202812", "T2204053", "T2300975"]
    assert result["expiry_date"] == ["07/2025", "10/2025", "02/2026"]


def test_parse_fda_usa_table_flat() -> None:
    """Test _parse_fda_usa_table with flat rows (product, NDC, lot, expiry per row)."""
    cfg = _fda_ajax_config()
    scraper = FDAUSAScraper(cfg)

    html = """
    <tbody>
        <tr>
            <td><p class="text-align-center">PROGRAF® (tacrolimus)<br>0.5 mg capsules<br>100 capsules per bottle</p></td>
            <td><p class="text-align-center">0469-0607-73</p></td>
            <td><p class="text-align-center">0E3353D</p></td>
            <td><p class="text-align-center">03/2026</p></td>
        </tr>
        <tr>
            <td><p class="text-align-center">ASTAGRAF XL® (tacrolimus extended-release capsules)<br>0.5 mg capsules<br>30 capsules per bottle</p></td>
            <td><p class="text-align-center">0469-0647-73</p></td>
            <td><p class="text-align-center">0R3092A</p></td>
            <td><p class="text-align-center">03/2026</p></td>
        </tr>
    </tbody>
    """
    soup = BeautifulSoup(html, "html.parser")
    tbody = soup.find("tbody")
    result = scraper._parse_fda_usa_table(tbody)

    assert "product_name" in result
    assert "PROGRAF" in result["product_name"][0]
    assert "ASTAGRAF" in result["product_name"][1]
    assert result["batch_number"] == ["0E3353D", "0R3092A"]
    assert result["expiry_date"] == ["03/2026", "03/2026"]
    # We deliberately ignore NDC-like codes in this parser
    assert "ndc" not in result
