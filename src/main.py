"""Entry point for running scrapers and exporting CSV output."""

from __future__ import annotations

from datetime import datetime

import json
import pandas as pd
import sqlite3

from src.scrapers.nafdac import NafDacScraper  # noqa: E402
from src.scrapers.fdausa import FDAUSAScraper
from database import create_table


def main():
    """Run scrapers and export results as CSVs."""

    create_table()

    start_date = datetime(2024, 1, 1)
    # Example: run FDA USA scraper from 2024-01-01 onwards
    fdausa = FDAUSAScraper(start_date)
    fdausa.standardize(upload_to_db=True)

    # # Example: run NAFDAC scraper (commented out by default)
    nafdac = NafDacScraper(start_date)
    nafdac.standardize(upload_to_db=True)


if __name__ == "__main__":
    main()
