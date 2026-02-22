"""Entry point for running scrapers and exporting CSV output."""

from __future__ import annotations

from datetime import datetime

from src.scrapers.nafdac import NafDacScraper  # noqa: E402
from src.scrapers.fdausa import FDAUSAScraper
from src.scrapers.healthcanada import HealthCanadaScraper

from database import create_table


def main():
    """Run scrapers and export results as CSVs."""

    create_table()

    start_date = datetime(2024, 1, 1)

    healthcanada = HealthCanadaScraper(start_date)
    records = healthcanada.standardize()
    print(records)
   
    fdausa = FDAUSAScraper(start_date)
    fdausa.standardize(upload_to_db=True)

   
    nafdac = NafDacScraper(start_date)
    nafdac.standardize(upload_to_db=True)


if __name__ == "__main__":
    main()
