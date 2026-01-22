import json
from pathlib import Path
from typing import Dict, Any

import pytest

from src.scrapers.nafdac import NafDacScraper


@pytest.fixture
def sources_path(tmp_path: Path) -> str:
    """
    Create a minimal sources.json for just NAFDAC_NG, matching your config shape.
    """
    cfg = {
        "NAFDAC_NG": {
            "source_id": "NAFDAC_NG",
            "source_country": "Nigeria",
            "source_org": "National Agency for Food and Drug Administration and Control (NAFDAC)",
            "base_url": "https://nafdac.gov.ng/category/recalls-and-alerts/",
            "request": {"headers": {"Accept-Language": "en-GB,en;q=0.9"}},
            "listing": {
                "item_selector": "article",
                "link_selector": "h2.entry-title a",
                "date_selector": "time.entry-date",
                "pagination": {"type": "path", "pattern": "page/{page}/", "start": 1, "max_pages": 5},
            },
            "detail_page": {
                "title_selector": "h1.entry-title",
                "body_selector": "div.entry-content",
                "publish_date_selector": "time.entry-date",
                "fields": {
                    "manufacturer_stated": {
                        "strategy": "regex",
                        "pattern": r"(manufacturer|manufactured by)[:\s]+(.+)",
                    },
                    "reason": {
                        "strategy": "regex",
                        "pattern": r"(reason|cause|issue)[:\s]+(.+)",
                    },
                },
            },
            "filters": {
                "require_oncology": True,
                "oncology_keywords": ["oncology", "cancer", "tumour", "chemotherapy", "immunotherapy"],
            },
            "defaults": {"therapeutic_category": "Oncology", "alert_type": "Recall / Safety Alert"},
        }
    }

    p = tmp_path / "sources.json"
    p.write_text(json.dumps(cfg), encoding="utf-8")
    return str(p)


def test_nafdac_listing_urls_pagination(sources_path: str) -> None:
    scraper = NafDacScraper(sources_path)
    urls = scraper._listing_urls()

    assert len(urls) == 5
    assert urls[0].endswith("/page/1/")
    assert urls[-1].endswith("/page/5/")


def test_nafdac_standardize_end_to_end_with_mocked_scrape(monkeypatch, sources_path: str) -> None:
    scraper = NafDacScraper(sources_path)

    listing_html = """
    <article>
      <h2 class="entry-title"><a href="https://nafdac.gov.ng/alert-1/">Public Alert 1</a></h2>
      <time class="entry-date">2026-01-02</time>
    </article>
    """

    detail_html = """
    <html>
      <body>
        <h1 class="entry-title">Public Alert 1</h1>
        <time class="entry-date">2026-01-02</time>
        <div class="entry-content">
          This is a cancer-related safety alert.
          Manufactured by: XYZ Ltd
          Reason: falsified product labeling
        </div>
      </body>
    </html>
    """

    def fake_scrape(url: str = None) -> Dict[str, Any]:
        target = url or scraper.url
        if "/page/" in target or target == scraper.cfg["base_url"]:
            return {
                "final_url": target,
                "status_code": 200,
                "html": listing_html,
                "text": "listing page",
                "retrieved_at": "2026-01-02T00:00:00Z",
            }
        # detail
        return {
            "final_url": "https://nafdac.gov.ng/alert-1/",
            "status_code": 200,
            "html": detail_html,
            "text": "This is a cancer-related safety alert. Manufactured by: XYZ Ltd Reason: falsified product labeling",
            "retrieved_at": "2026-01-02T00:00:01Z",
        }

    monkeypatch.setattr(scraper, "scrape", fake_scrape)

    records = scraper.standardize()

    assert len(records) == 1
    r = records[0]

    # defaults from config
    assert r.source_id == "NAFDAC_NG"
    assert r.therapeutic_category == "Oncology"
    assert r.alert_type == "Recall / Safety Alert"

    # selectors / fields
    assert r.title == "Public Alert 1"
    assert r.publish_date is not None
    assert str(r.publish_date.date()) == "2026-01-02"

    # regex extractions should exist
    assert r.manufacturer_stated is not None
    assert "XYZ" in r.manufacturer_stated

    assert r.reason is not None
    assert "falsified" in r.reason.lower()

    # stable id present
    assert isinstance(r.record_id, str)
    assert len(r.record_id) > 10
