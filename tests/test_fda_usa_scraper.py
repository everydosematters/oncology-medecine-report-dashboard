import json
from pathlib import Path
from typing import Dict, Any

import pytest

from src.scrapers.fda_usa import FDAUSAScraper


@pytest.fixture
def sources_path(tmp_path: Path) -> str:
    """
    Create a minimal sources.json for just FDA_US, matching your config shape.
    """
    cfg = {
        "FDA_US": {
            "source_id": "FDA_US",
            "source_country": "United States",
            "source_org": "U.S. Food and Drug Administration (FDA)",
            "base_url": "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts",
            "request": {"headers": {"Accept-Language": "en-US,en;q=0.9"}},
            "listing": {
                "item_selector": "div.view-content div.views-row",
                "link_selector": "a",
                "date_selector": "span.date-display-single",
                "pagination": {"type": "query_param", "param": "page", "start": 0, "max_pages": 5},
            },
            "detail_page": {
                "title_selector": "h1",
                "body_selector": "div.field--name-body",
                "publish_date_selector": "time",
                "fields": {
                    "manufacturer_stated": {
                        "strategy": "regex",
                        "pattern": r"(manufacturer|company)[:\s]+(.+)",
                    },
                    "reason": {
                        "strategy": "regex",
                        "pattern": r"(reason for recall|issue|problem)[:\s]+(.+)",
                    },
                },
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


def test_fda_listing_urls_pagination(sources_path: str) -> None:
    scraper = FDAUSAScraper(sources_path)
    urls = scraper._listing_urls()

    assert len(urls) == 5
    assert urls[0].endswith("?page=0")
    assert urls[-1].endswith("?page=4")


def test_fda_standardize_end_to_end_with_mocked_scrape(monkeypatch, sources_path: str) -> None:
    scraper = FDAUSAScraper(sources_path)

    listing_html = """
    <div class="view-content">
      <div class="views-row">
        <a href="/detail-1">Recall Notice A</a>
        <span class="date-display-single">2026-01-01</span>
      </div>
    </div>
    """

    detail_html = """
    <html>
      <body>
        <h1>Recall Notice A</h1>
        <time>2026-01-01</time>
        <div class="field--name-body">
          This is an oncology medicine recall.
          Company: ACME Pharma
          Reason for recall: contamination detected
        </div>
      </body>
    </html>
    """

    def fake_scrape(url: str = None) -> Dict[str, Any]:
        target = url or scraper.url
        if "page=" in target or target == scraper.cfg["base_url"]:
            return {
                "final_url": target,
                "status_code": 200,
                "html": listing_html,
                "text": "listing page",
                "retrieved_at": "2026-01-01T00:00:00Z",
            }
        # detail
        return {
            "final_url": "https://www.fda.gov/detail-1",
            "status_code": 200,
            "html": detail_html,
            "text": "This is an oncology medicine recall. Company: ACME Pharma Reason for recall: contamination detected",
            "retrieved_at": "2026-01-01T00:00:01Z",
        }

    monkeypatch.setattr(scraper, "scrape", fake_scrape)

    records = scraper.standardize()

    assert len(records) == 1
    r = records[0]

    # defaults from config
    assert r.source_id == "FDA_US"
    assert r.therapeutic_category == "Oncology"
    assert r.alert_type == "Recall"

    # selectors / fields
    assert r.title == "Recall Notice A"
    assert r.publish_date is not None
    assert str(r.publish_date.date()) == "2026-01-01"

    # regex extractions should exist (may capture more text depending on greedy regex)
    assert r.manufacturer_stated is not None
    assert "ACME" in r.manufacturer_stated

    assert r.reason is not None
    assert "contamination" in r.reason.lower()

    # stable id present
    assert isinstance(r.record_id, str)
    assert len(r.record_id) > 10
