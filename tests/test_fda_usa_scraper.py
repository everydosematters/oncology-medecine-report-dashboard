"""Tests for FDA USA scraper (openFDA API workflow)."""

from datetime import datetime, timezone

from src.scrapers.fdausa import FDAUSAScraper


def test_init_accepts_start_date(fdausa_scraper) -> None:
    """Scraper initializes with start_date."""
    scraper = fdausa_scraper
    assert scraper.source_id == "FDA_US"
    assert scraper.source_org == "U.S. Food and Drug Administration (FDA)"
    assert scraper.cfg["api_endpoint"] == "https://api.fda.gov/drug/enforcement.json"


def test_init_accepts_start_date_datetime(fdausa_scraper) -> None:
    """Scraper accepts optional start_date for filtering (adds tzinfo if missing)."""
    start = datetime(2024, 1, 1)
    scraper = FDAUSAScraper(start_date=start)
    assert scraper.start_date is not None
    assert scraper.start_date.tzinfo is not None


def test_openfda_date_range(fdausa_scraper) -> None:
    """_openfda_date_range returns correct query syntax."""
    scraper = fdausa_scraper
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = datetime(2024, 12, 31, tzinfo=timezone.utc)
    result = scraper._openfda_date_range(start, end)
    assert result == "[20240101 TO 20241231]"


def test_get_manufacturer(fdausa_scraper) -> None:
    """_get_manufacturer extracts manufacturer from product description."""
    scraper = fdausa_scraper

    desc = "Herceptin, 10 mg/4 mL, Mfd by: ProRx, 619 Jeffers Cir, Exton, PA"
    assert scraper._get_manufacturer(desc) == "ProRx"

    desc2 = "Some Drug, Manufactured by: Acme Pharma Inc."
    assert scraper._get_manufacturer(desc2) == "Acme Pharma Inc."

    assert scraper._get_manufacturer("No manufacturer here") is None


def test_get_distributor(fdausa_scraper) -> None:
    """_get_distributor extracts distributor from product description."""
    scraper = fdausa_scraper

    desc = "Drug X, Distributed by: ABC Distributors LLC"
    assert scraper._get_distributor(desc) == "ABC Distributors LLC"

    desc2 = "Drug Y, Distributor: XYZ Corp"
    assert scraper._get_distributor(desc2) == "XYZ Corp"

    assert scraper._get_distributor("Acme Drug 100mg, Rx only") is None


def test_standardize_mocked(fdausa_scraper, monkeypatch) -> None:
    """End-to-end test with mocked openFDA API response."""
    scraper = fdausa_scraper

    # Mock oncology drugs so Herceptin is recognized
    monkeypatch.setattr(scraper, "oncology_drugs", ["herceptin", "trastuzumab"])

    sample_records = [
        {
            "recall_number": "D-0115-2026",
            "country": "United States",
            "product_description": "Herceptin, 10 mg/4 mL, Mfd by: ProRx, Exton, PA. NDC: 84139-225-04",
            "reason_for_recall": "Lack of Assurance of Sterility",
            "report_date": "2025-11-05",
            "code_info": "Lot PRORX050925-1",
        },
    ]

    def fake_fetch(self, endpoint, params, *, page_size=1000, pause_s=0.1):
        return sample_records

    monkeypatch.setattr(FDAUSAScraper, "_fetch_all_openfda", fake_fetch)

    records = scraper.standardize()

    assert len(records) == 1
    r = records[0]
    assert r.source_id == "FDA_US"
    assert r.source_org == "U.S. Food and Drug Administration (FDA)"
    assert r.source_country == "United States"
    assert "2025-11-05" in (r.publish_date or "")
    assert r.product_name == "Herceptin"
    assert r.manufacturer == "ProRx"
    assert "Lack of Assurance of Sterility" in (r.reason or "")
    assert r.record_id
    assert len(r.record_id) > 10
    assert "D-0115-2026" in (r.source_url or "")


def test_standardize_skips_non_oncology_drugs(fdausa_scraper, monkeypatch) -> None:
    """Records for drugs not in oncology list are skipped."""
    scraper = fdausa_scraper
    monkeypatch.setattr(scraper, "oncology_drugs", ["herceptin"])  # only Herceptin

    sample_records = [
        {
            "recall_number": "D-9999-2026",
            "country": "United States",
            "product_description": "Random Drug XYZ, Mfd by: SomeCo",
            "reason_for_recall": "Other",
            "report_date": "2025-11-05",
            "code_info": "",
        },
    ]

    def fake_fetch(self, endpoint, params, *, page_size=1000, pause_s=0.1):
        return sample_records

    monkeypatch.setattr(FDAUSAScraper, "_fetch_all_openfda", fake_fetch)

    records = scraper.standardize()
    assert len(records) == 0
