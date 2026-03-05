"""Conftest"""

import dataclasses
import importlib.util
import sys
import types
from pathlib import Path

import pytest

# Add the project root and src to Python path (scrapers.config lives under src)
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))


def _load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module {module_name} from {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="session")
def utils_mod():
    """
    Load src.scrapers.utils from file so tests can import even if your local
    package layout differs (as long as file exists at src/scrapers/utils.py).
    """
    path = Path("src/scrapers/utils.py")
    if not path.exists():
        # fallback for your uploaded filenames / alternate layout
        path = Path("utils.py")
    return _load_module("src.scrapers.utils", path)


@pytest.fixture(scope="session")
def nafdac_mod(utils_mod):
    """
    Load src.scrapers.nafdac with lightweight stubs for src.models and BaseScraper
    so we can test parsing methods without needing the full app wiring.
    """
    # Stub src as a package (needed for src.database, src.models)
    if "src" not in sys.modules:
        src = types.ModuleType("src")
        src.__path__ = [str(project_root / "src")]
        sys.modules["src"] = src

    models_mod = types.ModuleType("src.models")

    @dataclasses.dataclass
    class DrugAlert:
        title: str = ""

    models_mod.DrugAlert = DrugAlert
    sys.modules["src.models"] = models_mod

    # Stub src.database (nafdac imports upsert_df)
    db_mod = types.ModuleType("src.database")
    db_mod.upsert_df = lambda conn, results: None
    sys.modules["src.database"] = db_mod

    # Ensure src.scrapers package exists
    scrapers_pkg = types.ModuleType("src.scrapers")
    scrapers_pkg.__path__ = []
    sys.modules["src.scrapers"] = scrapers_pkg

    # Stub BaseScraper (only needed for class inheritance)
    base_mod = types.ModuleType("src.scrapers.base")

    class BaseScraper:
        pass

    base_mod.BaseScraper = BaseScraper
    sys.modules["src.scrapers.base"] = base_mod

    path = Path("src/scrapers/nafdac.py")
    if not path.exists():
        path = Path("nafdac.py")

    return _load_module("src.scrapers.nafdac", path)


def _make_read_json_mock(monkeypatch, drugs: list):
    """Mock read_json for oncology drugs; used by scrapers that need BaseScraper."""
    from src.scrapers.utils import read_json as _real_read_json

    def _mock(path: str):
        if "nci_oncology" in path:
            return drugs
        return _real_read_json(path)

    import src.scrapers.base as base_mod

    monkeypatch.setattr(base_mod, "read_json", _mock)


@pytest.fixture
def fdausa_scraper(monkeypatch):
    """Create FDAUSAScraper with mocked oncology drugs."""
    from datetime import datetime, timezone

    _make_read_json_mock(monkeypatch, ["herceptin", "trastuzumab", "tacrolimus"])
    from src.scrapers.fdausa import FDAUSAScraper

    return FDAUSAScraper(start_date=datetime(2024, 1, 1, tzinfo=timezone.utc))


@pytest.fixture
def healthcanada_scraper(monkeypatch):
    """Create HealthCanadaScraper with mocked oncology drugs."""
    from datetime import datetime, timezone

    _make_read_json_mock(monkeypatch, ["herceptin", "trastuzumab", "tacrolimus"])
    from src.scrapers.healthcanada import HealthCanadaScraper

    return HealthCanadaScraper(start_date=datetime(2024, 1, 1, tzinfo=timezone.utc))


@pytest.fixture
def fdaghana_scraper(monkeypatch):
    """Create FDAGhanaScraper with mocked oncology drugs."""
    from datetime import datetime, timezone

    _make_read_json_mock(monkeypatch, ["herceptin", "trastuzumab", "tacrolimus"])
    from src.scrapers.fdaghana import FDAGhanaScraper

    return FDAGhanaScraper(start_date=datetime(2024, 1, 1, tzinfo=timezone.utc))


@pytest.fixture()
def nafdac_scraper(nafdac_mod):
    """
    Create a scraper instance without calling BaseScraper.__init__ (if any).
    We only need cfg + parsing methods.
    """
    Scraper = nafdac_mod.NafDacScraper
    scraper = Scraper.__new__(Scraper)

    scraper.cfg = {
        "detail_page": {
            "title_selector": "h1.entry-title",
            "body_selector": "div.entry-content",
            "publish_date_selector": "time.entry-date",
        },
        "filters": {
            "oncology_keywords": [
                "oncology",
                "oncology",
                "tumour",
                "chemotherapy",
                "immunotherapy",
            ]
        },
    }
    return scraper
