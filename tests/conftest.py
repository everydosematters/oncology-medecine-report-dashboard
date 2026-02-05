"""Conftest"""

import sys
from pathlib import Path

import dataclasses
import importlib.util
import types

import pytest

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))



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
    # Stub src + src.models.DrugAlert
    if "src" not in sys.modules:
        sys.modules["src"] = types.ModuleType("src")

    models_mod = types.ModuleType("src.models")

    @dataclasses.dataclass
    class DrugAlert:
        title: str = ""

    models_mod.DrugAlert = DrugAlert
    sys.modules["src.models"] = models_mod

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
            "fields": {},  # keep empty for unit tests
        }
    }
    return scraper

