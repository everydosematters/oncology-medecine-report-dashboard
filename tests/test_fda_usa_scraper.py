import json
from pathlib import Path
from typing import Any, Dict

import pytest

from src.scrapers.fda_usa import FDAUSAScraper


@pytest.fixture
def sources_path(tmp_path: Path) -> str:
    """
    Minimal sources.json for FDA_US using TABLE-based listing selectors.
    """
    cfg = {
        "FDA_US": {
            "source_id": "FDA_US",
            "source_country": "United States",
            "source_org": "U.S. Food and Drug Administration (FDA)",
            "base_url": "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts",
            "request": {"headers": {"Accept-Language": "en-US,en;q=0.9"}},
            "listing": {
                # Key change: table rows
                "item_selector": "table#datatable tbody tr",
                # Link lives in the Brand Name column
                "link_selector": "td:nth-child(2) a",
                # Date column may include a <time> (often does)
                "date_selector": "td:nth-child(1) time",
                "pagination": {"type": "query_param", "param": "page", "start": 0, "max_pages": 5},
            },
            "filters": {
                "require_oncology": True,
                "oncology_keywords": ["oncology", "cancer", "tumor", "chemotherapy", "immunotherapy", "malignant"],
            },
            "defaults": {"therapeutic_category": "Oncology", "alert_type": "Recall"},
        }
    }

    p = tmp_path / "sources.json"
    p.write_text(json.dumps(cfg), encoding="utf-8")
    return str(p)


def _as_iso_date(val: Any) -> str:
    """
    Helper: normalize publish_date regardless of whether your DrugAlert model
    stores it as str, date, or datetime.
    """
    if val is None:
        return ""
    # datetime/date objects
    if hasattr(val, "date"):
        try:
            return str(val.date())
        except Exception:
            pass
    return str(val)


def test_fda_listing_urls_pagination(sources_path: str) -> None:
    scraper = FDAUSAScraper(sources_path)
    urls = scraper._listing_urls()

    assert len(urls) == 5
    assert urls[0].endswith("?page=0")
    assert urls[-1].endswith("?page=4")


def test_fda_standardize_from_table_listing(monkeypatch, sources_path: str) -> None:
    """
    End-to-end sanity check using a mocked TABLE listing.
    - Ensures selectors match the table DOM
    - Ensures oncology filtering passes
    - Ensures record fields are populated sensibly
    """
    scraper = FDAUSAScraper(sources_path)

    listing_html = """
    <html>
      <body>
        <table id="datatable">
          <thead>
            <tr>
              <th>Date</th><th>Brand Name</th><th>Product Description</th>
              <th>Product Type</th><th>Recall Reason</th><th>Company Name</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td><time datetime="2026-01-01">January 1, 2026</time></td>
              <td><a href="/recalls/recall-1">OncoDrug X</a></td>
              <td>Oncology medicine tablets, 50mg</td>
              <td>Drugs</td>
              <td>Potential contamination during chemotherapy supply chain</td>
              <td>ACME Pharma</td>
            </tr>
          </tbody>
        </table>
      </body>
    </html>
    """

    # If your current standardize() still scrapes the detail URL too,
    # this prevents it from failing. (We don’t rely on detail parsing here.)
    detail_html = """
    <html><body><h1>OncoDrug X</h1><div>Placeholder detail page</div></body></html>
    """

    def fake_scrape(url: str = None) -> Dict[str, Any]:
        target = url or scraper.url

        # Listing pages (with pagination)
        if "page=" in target or target == scraper.cfg["base_url"]:
            return {
                "final_url": target,
                "status_code": 200,
                "html": listing_html,
                "text": "listing page",
                "retrieved_at": "2026-01-01T00:00:00Z",
            }

        # Detail page (if called)
        return {
            "final_url": "https://www.fda.gov/recalls/recall-1",
            "status_code": 200,
            "html": detail_html,
            "text": "Placeholder detail page",
            "retrieved_at": "2026-01-01T00:00:01Z",
        }

    monkeypatch.setattr(scraper, "scrape", fake_scrape)

    records = scraper.standardize()

    assert len(records) == 1
    r = records[0]

    # Basic identity/defaults
    assert r.source_id == "FDA_US"
    assert r.therapeutic_category == "Oncology"
    assert r.alert_type == "Recall"

    # Publish date should be derived from the table date column
    assert "2026-01-01" in _as_iso_date(r.publish_date)

    # Title should come from Brand Name column in the table (OncoDrug X)
    assert r.title is not None
    assert "OncoDrug X" in str(r.title)

    # Company + reason should be populated from the table row
    assert r.manufacturer_stated is not None
    assert "ACME" in str(r.manufacturer_stated)

    assert r.reason is not None
    assert "contamination" in str(r.reason).lower()

    # Record id should be stable-ish
    assert isinstance(r.record_id, str)
    assert len(r.record_id) > 10
