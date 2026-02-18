"""Base scraper utilities shared across site-specific scrapers."""

import hashlib
from abc import ABC, abstractmethod
from datetime import date, datetime
from typing import Any, Dict, List, Optional, final


import requests
from bs4 import BeautifulSoup

from src.models import DrugAlert
from .utils import normalize_drug_name, extract_drug_tokens, read_json
import json


class BaseScraper(ABC):
    """Abstract base class for all site-specific scrapers."""

    def __init__(
        self,
        url: str,
        *,
        args: Optional[Dict[str, Any]] = None,
        timeout: int = 30,
        start_date: Optional[datetime] = None,
        oncology_drugs_path: str = "data/nci_oncology_drugs.json",
    ) -> None:
        """Initialize the scraper with a base URL and optional request args."""

        self.url = url
        self.args = args or {}
        self.timeout = timeout
        self.start_date = start_date
        self.search_url = "https://webapis.oncology.gov/drugdictionary/v1/Drugs/search"
        self.nci_url = (
            "https://www.oncology.gov/about-oncology/treatment/drugs/oncology-drugs"
        )

        # Safe default UA
        self.args.setdefault("headers", {})
        self.args["headers"].setdefault(
            "User-Agent",
            "Mozilla/5.0 (compatible; EDM-Dashboard/1.0; +https://everydosematters.org)",
        )
        self.oncology_drugs_path = oncology_drugs_path
        self.oncology_drugs = read_json(self.oncology_drugs_path)

    def scrape(self, url: Optional[str] = None) -> Dict[str, Any]:
        """Fetch a URL and return minimal response metadata plus parsed HTML."""

        target = url or self.url
        resp = requests.get(target, timeout=self.timeout, **self.args)
        resp.raise_for_status()

        html = resp.text
        soup = BeautifulSoup(html, "html.parser")

        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()

        return {
            "final_url": resp.url,
            "status_code": resp.status_code,
            "html": soup,
        }

    def get_json(self, url: str, params: dict) -> Dict:
        """Fetch a json instead."""

        r = requests.get(url, params=params)
        data = r.json()
        return data

    @abstractmethod
    def standardize(self) -> List[DrugAlert]:
        """Standardize the scraper's data into a list of DrugAlert objects."""

        raise NotImplementedError

    @final
    def make_record_id(self, source_id: str, drug_name: str, publish_date: str) -> str:
        """Build a stable record identifier from heterogeneous parts."""

        raw = "||".join([source_id, drug_name, publish_date])
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    @final
    def get_nci_name(self, drug_name: str, approved_drugs: list = []) -> Optional[str]:
        """Look up the NCI dictionary for the drug name."""

        if not drug_name:
            return None
        if not approved_drugs:
            approved_drugs = self.oncology_drugs
        if not approved_drugs:
            approved_drugs = self.fetch_oncology_drug_names()

        normalized = normalize_drug_name(drug_name)
        if normalized in approved_drugs:
            return normalized.capitalize()
        else:
            return None

    def fetch_oncology_drug_names(self) -> list[str]:
        """Get the approved cancer drugs."""

        response = requests.get(self.nci_url, headers=self.args["headers"], timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        drug_names = []

        # The page lists drugs as links inside the main content area.
        # We grab all anchor tags that link to individual drug dictionary pages.
        uls = soup.select("ul.no-bullets.no-description")
        for ul in uls:
            for li in ul.select("li"):
                name = li.get_text(strip=True)
                drug_names = drug_names + extract_drug_tokens(name)

        # Remove duplicates while preserving order
        drug_names = list(dict.fromkeys(drug_names))
        self.oncology_drugs = drug_names
        with open("data/nci_oncology_drugs.json", "w") as f:
            json.dump(drug_names, f, indent=2)
        return drug_names
