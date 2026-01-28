import json
from pathlib import Path
from typing import Dict, Any

import pytest

from src.scrapers.nafdac import NafDacScraper


@pytest.fixture
def sources_path(tmp_path: Path) -> str:
    """
    Create a minimal sources.json for just NAFDAC_NG, matching the TABLE-based listing.
    """
    cfg = {
        "NAFDAC_NG": {
            "source_id": "NAFDAC_NG",
            "source_country": "Nigeria",
            "source_org": "National Agency for Food and Drug Administration and Control (NAFDAC)",
            "base_url": "https://nafdac.gov.ng/category/recalls-and-alerts/",
            "request": {"headers": {"Accept-Language": "en-GB,en;q=0.9"}},
            "listing": {
                # ✅ table-based listing
                "item_selector": "table tbody tr",
                "link_selector": "td:nth-child(2) a.ninja_table_permalink",
                "date_selector": "td:nth-child(1)",
                # optional additional columns (if your scraper reads them)
                "fields": {
                    "alert_type": "td:nth-child(3)",
                    "category": "td:nth-child(4)",
                    "company": "td:nth-child(5)",
                },
                "pagination": {"type": "path", "pattern": "page/{page}/", "start": 1, "max_pages": 1},
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

    assert len(urls) == 1

def test_nafdac_standardize_end_to_end_with_mocked_scrape(monkeypatch, sources_path: str) -> None:
    """
    End-to-end sanity check:
    - listing page is table/tbody/tr
    - scraper follows title URL (detail_url)
    - oncology filter uses DETAIL page content
    """
    scraper = NafDacScraper(sources_path)

    listing_html = """
    <html><body>
      <table>
        <tbody>
          <tr class="ninja_table_row_0 nt_row_id_0" data-row_id="0">
            <td>09-Jan-26</td>
            <td>
              <a class="ninja_table_permalink"
                 href="https://nafdac.gov.ng/public-alert-no-03-2026-alert-on-the-circulation-of-an-unauthorized-and-unregistered-risperdal-2-mg-tablets-brand-formulation-in-nigeria/"
                 title="Public Alert No. 03/2026–Alert on the Circulation of an Unauthorized and Unregistered Risperdal 2 mg Tablets Brand Formulation in Nigeria">
                 Public Alert No. 03/2026–Alert on the Circulation of an Unauthorized and Unregistered Risperdal 2 mg Tablets Brand Formulation in Nigeria
              </a>
            </td>
            <td>Safety Alert</td>
            <td>Drugs</td>
            <td>Johnson &amp; Johnson</td>
          </tr>
        </tbody>
      </table>
    </body></html>
    """

    # Detail page content includes "cancer" so oncology filter passes
    detail_html = """
    <html>
      <body>
        <h1 class="entry-title">Public Alert No. 03/2026</h1>
        <time class="entry-date">2026-01-09</time>
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

        # listing pages
        if "/page/" in target or target == scraper.cfg["base_url"]:
            return {
                "final_url": target,
                "status_code": 200,
                "html": listing_html,
                "text": "listing page",
                "retrieved_at": "2026-01-09T00:00:00Z",
            }

        # detail page
        return {
            "final_url": target,
            "status_code": 200,
            "html": detail_html,
            "text": "This is a cancer-related safety alert. Manufactured by: XYZ Ltd Reason: falsified product labeling",
            "retrieved_at": "2026-01-09T00:00:01Z",
        }

    monkeypatch.setattr(scraper, "scrape", fake_scrape)

    records = scraper.standardize()

    assert len(records) == 1
    r = records[0]

    # defaults from config
    assert r.source_id == "NAFDAC_NG"
    assert r.therapeutic_category == "Oncology"

    # alert_type: could come from listing column or defaults depending on your implementation
    assert r.alert_type in {"Safety Alert", "Recall / Safety Alert"}

    # required-by-model field must exist
    assert r.product_name is not None
    assert len(str(r.product_name)) > 0

    # title should exist (from detail page or listing)
    assert r.title is not None

    # date: your model uses datetime; ensure it parsed
    assert r.publish_date is not None
    # If your scraper uses listing date "09-Jan-26" instead, this may differ.
    # Detail page here provides 2026-01-09, so we'll expect that.
    assert str(r.publish_date.date()) == "2026-01-09"

    # regex extractions should exist
    assert r.manufacturer_stated is not None
    assert "XYZ" in str(r.manufacturer_stated)

    assert r.reason is not None
    assert "falsified" in str(r.reason).lower()

    # stable id present
    assert isinstance(r.record_id, str)
    assert len(r.record_id) > 10
