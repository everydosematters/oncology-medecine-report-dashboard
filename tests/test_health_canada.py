"""Tests for Health Canada scraper (JSON feed workflow)."""

from datetime import datetime

from src.scrapers.healthcanada import HealthCanadaScraper


def test_init(healthcanada_scraper) -> None:
    """Scraper initializes with config."""
    scraper = healthcanada_scraper
    assert scraper.source_id == "HEALTH_CANADA"
    assert scraper.source_org == "Health Canada"
    assert scraper.cfg["source_country"] == "Canada"
    assert "HCRSAMOpenData.json" in scraper.cfg["api_endpoint"]


def test_init_accepts_start_date(healthcanada_scraper) -> None:
    """Scraper accepts optional start_date for filtering."""
    start = datetime(2024, 1, 1)
    scraper = HealthCanadaScraper(start_date=start)
    assert scraper.start_date is not None
    assert scraper.start_date.tzinfo is not None


def test_norm(healthcanada_scraper) -> None:
    """_norm lowercases and strips input."""
    assert healthcanada_scraper._norm("  Drug  ") == "drug"
    assert healthcanada_scraper._norm(None) == ""
    assert healthcanada_scraper._norm("") == ""


def test_is_health_product_recall(healthcanada_scraper) -> None:
    """_is_health_product_recall filters by category keywords."""
    scraper = healthcanada_scraper

    assert scraper._is_health_product_recall({"Category": "Health Products"}) is True
    assert scraper._is_health_product_recall({"category": "drug"}) is True
    assert scraper._is_health_product_recall({"Category": "Biologics"}) is True
    assert scraper._is_health_product_recall({"Category": "vaccine"}) is True
    assert scraper._is_health_product_recall({"Category": "Food"}) is False
    assert scraper._is_health_product_recall({"Category": "Cosmetics"}) is False


def test_extract_dates(healthcanada_scraper) -> None:
    """_extract_dates parses Last updated and returns iso + datetime."""
    scraper = healthcanada_scraper

    iso, dt = scraper._extract_dates({"Last updated": "2025-11-05"})
    assert "2025-11-05" in (iso or "")
    assert dt is not None
    assert dt.tzinfo is not None

    iso2, dt2 = scraper._extract_dates({"Last updated": "10-2024"})
    assert iso2 is not None
    assert dt2 is not None

    iso3, dt3 = scraper._extract_dates({})
    assert iso3 is None
    assert dt3 is None


def test_standardize_mocked(healthcanada_scraper, monkeypatch) -> None:
    """End-to-end test with mocked feed."""
    scraper = healthcanada_scraper
    monkeypatch.setattr(scraper, "oncology_drugs", ["herceptin", "trastuzumab"])

    sample_feed = [
        {
            "Category": "Health Products",
            "Last updated": "2025-11-05",
            "Title": "Herceptin recall",
            "Product": "Herceptin 600mg",
            "URL": "https://recalls.canada.ca/example",
            "Issue": "Quality concern",
            "NID": "12345",
        },
    ]

    def fake_fetch(self):
        return sample_feed

    monkeypatch.setattr(HealthCanadaScraper, "_fetch_feed", fake_fetch)

    records = scraper.standardize()

    assert len(records) == 1
    r = records[0]
    assert r.source_id == "HEALTH_CANADA"
    assert r.source_org == "Health Canada"
    assert r.source_country == "Canada"
    assert "2025-11-05" in (r.publish_date or "")
    assert r.product_name == "Herceptin"
    assert "Quality concern" in (r.reason or "")
    assert r.record_id
    assert "https://recalls.canada.ca/example" in (r.source_url or "")


def test_standardize_skips_non_health_categories(
    healthcanada_scraper, monkeypatch
) -> None:
    """Records with non-health categories are skipped."""
    scraper = healthcanada_scraper
    monkeypatch.setattr(scraper, "oncology_drugs", ["herceptin"])

    sample_feed = [
        {
            "Category": "Food",
            "Last updated": "2025-11-05",
            "Title": "Food recall",
            "Product": "Some Food",
            "URL": "https://example.com",
            "NID": "999",
        },
    ]

    def fake_fetch(self):
        return sample_feed

    monkeypatch.setattr(HealthCanadaScraper, "_fetch_feed", fake_fetch)

    records = scraper.standardize()
    assert len(records) == 0


def test_standardize_skips_non_oncology_drugs(
    healthcanada_scraper, monkeypatch
) -> None:
    """Records for drugs not in oncology list are skipped."""
    scraper = healthcanada_scraper
    monkeypatch.setattr(scraper, "oncology_drugs", ["herceptin"])  # only Herceptin

    sample_feed = [
        {
            "Category": "Health Products",
            "Last updated": "2025-11-05",
            "Title": "Random Drug XYZ recall",
            "Product": "Random Drug XYZ",
            "URL": "https://example.com",
            "NID": "999",
        },
    ]

    def fake_fetch(self):
        return sample_feed

    monkeypatch.setattr(HealthCanadaScraper, "_fetch_feed", fake_fetch)

    records = scraper.standardize()
    assert len(records) == 0
