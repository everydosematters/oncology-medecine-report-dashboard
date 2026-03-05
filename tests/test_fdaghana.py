"""Tests for FDA Ghana scraper (wpDataTables AJAX workflow)."""

from datetime import datetime

from src.scrapers import fdaghana


def test_init(fdaghana_scraper) -> None:
    """Scraper initializes with config."""
    scraper = fdaghana_scraper
    assert scraper.source_id == "FDA_GH"
    assert scraper.source_org == "Food and Drugs Authority (Ghana)"
    assert scraper.source_country == "Ghana"
    assert scraper.cfg["table_id"] == "47"
    assert "admin-ajax.php" in scraper.cfg["ajax_url"]


def test_init_accepts_start_date(fdaghana_scraper) -> None:
    """Scraper accepts optional start_date for filtering."""
    start = datetime(2024, 1, 1)
    scraper = fdaghana.FDAGhanaScraper(start_date=start)
    assert scraper.start_date is not None
    assert scraper.start_date.tzinfo is not None


def test_safe_json_loads() -> None:
    """_safe_json_loads parses JSON and strips BOM."""
    assert fdaghana._safe_json_loads('{"a": 1}') == {"a": 1}
    assert fdaghana._safe_json_loads('\ufeff{"x": "y"}') == {"x": "y"}
    assert fdaghana._safe_json_loads('  {"n": 0}  ') == {"n": 0}


def test_detect_column_count_from_html() -> None:
    """_detect_column_count_from_html finds table with expected headers."""
    html = """
    <table>
      <thead><tr>
        <th>Date Recall Was Issued</th>
        <th>Product Name</th>
        <th>Col3</th>
      </tr></thead>
    </table>
    """
    assert fdaghana._detect_column_count_from_html(html) == 3

    assert fdaghana._detect_column_count_from_html("<html><body>no table</body></html>") == 13


def test_extract_link_and_text() -> None:
    """_extract_link_and_text extracts href and text from cell HTML."""
    cell = '<a href="https://example.com/recall">Herceptin 600mg</a>'
    href, text = fdaghana._extract_link_and_text(cell)
    assert href == "https://example.com/recall"
    assert "Herceptin" in (text or "")

    href2, text2 = fdaghana._extract_link_and_text("Plain text only")
    assert href2 is None
    assert text2 == "Plain text only"

    assert fdaghana._extract_link_and_text(None) == (None, None)


def test_is_pdf_url() -> None:
    """_is_pdf_url detects PDF URLs."""
    assert fdaghana._is_pdf_url("https://example.com/notice.pdf") is True
    assert fdaghana._is_pdf_url("https://example.com/notice.PDF?q=1") is True
    assert fdaghana._is_pdf_url("https://example.com/page.html") is False
    assert fdaghana._is_pdf_url(None) is False


def test_row_to_alert(fdaghana_scraper, monkeypatch) -> None:
    """_row_to_alert converts wpDataTables row to DrugAlert."""
    scraper = fdaghana_scraper
    monkeypatch.setattr(scraper, "oncology_drugs", ["herceptin"])

    row = [
        "row_1",
        "",
        "2024-01-01",
        "",
        "2024-01-02",
        "15/01/2024",  # recall date col 5
        '<a href="https://fdaghana.gov.gh/recall/1">Herceptin 600mg</a>',  # product col 6
        "Drug",  # product_type col 7
        "Roche",  # manufacturer col 8
        "Distributor Inc",  # recalling_firm col 9
        "LOT123",  # batches col 10
        "01/2024",  # mfg_date col 11
        "12/2026",  # exp_date col 12
    ]

    alert = scraper._row_to_alert(row)
    assert alert is not None
    assert alert.source_id == "FDA_GH"
    assert alert.product_name == "Herceptin"
    assert alert.manufacturer == "Roche"
    assert alert.distributor == "Distributor Inc"
    assert "2024-01-15" in (alert.publish_date or "")
    assert "LOT123" in (alert.more_info or "")
    assert alert.record_id


def test_row_to_alert_skips_non_drug(fdaghana_scraper, monkeypatch) -> None:
    """_row_to_alert returns None for non-Drug product type."""
    scraper = fdaghana_scraper
    monkeypatch.setattr(scraper, "oncology_drugs", ["herceptin"])

    row = [
        "row_1",
        "",
        "",
        "",
        "",
        "15/01/2024",
        '<a href="https://example.com">Herceptin</a>',
        "Medical Device",  # not Drug
        "Roche",
        "Dist",
        "LOT",
        "01/2024",
        "12/2026",
    ]

    assert scraper._row_to_alert(row) is None


def test_row_to_alert_skips_non_oncology(fdaghana_scraper, monkeypatch) -> None:
    """_row_to_alert returns None when product not in oncology list."""
    scraper = fdaghana_scraper
    monkeypatch.setattr(scraper, "oncology_drugs", ["herceptin"])  # only Herceptin

    row = [
        "row_1",
        "",
        "",
        "",
        "",
        "15/01/2024",
        '<a href="https://example.com">Random Drug XYZ</a>',
        "Drug",
        "Roche",
        "Dist",
        "LOT",
        "01/2024",
        "12/2026",
    ]

    assert scraper._row_to_alert(row) is None


def test_row_to_alert_returns_none_for_short_row(fdaghana_scraper) -> None:
    """_row_to_alert returns None for rows with fewer than 13 columns."""
    assert fdaghana_scraper._row_to_alert([1, 2, 3]) is None
    assert fdaghana_scraper._row_to_alert("not a list") is None


def test_standardize_mocked(fdaghana_scraper, monkeypatch) -> None:
    """End-to-end test with mocked fetch_all_rows."""
    scraper = fdaghana_scraper
    monkeypatch.setattr(scraper, "oncology_drugs", ["herceptin"])

    sample_rows = [
        [
            "row_1",
            "",
            "",
            "",
            "",
            "15/01/2024",
            '<a href="https://fdaghana.gov.gh/recall/1">Herceptin 600mg</a>',
            "Drug",
            "Roche",
            "Dist Inc",
            "LOT123",
            "01/2024",
            "12/2026",
        ],
    ]

    def fake_fetch_all(self, page_size=200):
        return sample_rows

    monkeypatch.setattr(
        fdaghana.GhanaWpDataTablesClient,
        "fetch_all_rows",
        fake_fetch_all,
    )

    # Need to mock prime/discover so client doesn't make real requests
    def fake_prime(self):
        self._listing_html = "<html></html>"
        self._ncols = 13

    def fake_discover(self):
        self._wdt_nonce = "fake_nonce"

    monkeypatch.setattr(fdaghana.GhanaWpDataTablesClient, "prime", fake_prime)
    monkeypatch.setattr(fdaghana.GhanaWpDataTablesClient, "discover_wdt_nonce", fake_discover)

    records = scraper.standardize()

    assert len(records) == 1
    r = records[0]
    assert r.source_id == "FDA_GH"
    assert r.product_name == "Herceptin"
    assert r.manufacturer == "Roche"
