"""Entry point for running scrapers and exporting CSV output."""

from __future__ import annotations

from datetime import datetime

from src.scrapers.nafdac import NafDacScraper  # noqa: E402
from src.scrapers.fdausa import FDAUSAScraper
from src.scrapers.healthcanada import HealthCanadaScraper
from src.scrapers.fdaghana import FDAGhanaScraper

from database import create_table, create_csv
import argparse

import sys
print("DEBUG argv:", sys.argv)

def main(start_date: datetime):
    """Run scrapers and export results as CSVs."""
    
    print(f"Starting scrapers from {start_date.strftime('%Y-%m-%d')}...")
    
    create_table()

    fdaghana = FDAGhanaScraper(start_date)
    fdaghana.standardize()

    healthcanada = HealthCanadaScraper(start_date)
    healthcanada.standardize(upload_to_db=True)

    fdausa = FDAUSAScraper(start_date)
    fdausa.standardize(upload_to_db=True)

    nafdac = NafDacScraper(start_date)
    nafdac.standardize(upload_to_db=True)

    create_csv()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run regulatory scrapers and export CSV output."
    )

    parser.add_argument(
        "--start-date",
        type=str,
        default="2020-01-01",
        help="Start date in YYYY-MM-DD format (default: 2020-01-01)",
    )

    args = parser.parse_args()
    print(args.start_date)

    try:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    except ValueError:
        raise ValueError("Invalid date format. Use YYYY-MM-DD.")

    main(start_date)
