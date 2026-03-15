"""Entry point for running scrapers and exporting CSV output."""

from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime
from typing import Optional

from database import create_csv, create_table
from src.scrapers.base import BaseScraper
from src.scrapers.fdaghana import FDAGhanaScraper
from src.scrapers.fdausa import FDAUSAScraper
from src.scrapers.healthcanada import HealthCanadaScraper
from src.scrapers.nafdac import NafDacScraper  # noqa: E402

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def run_scraper(scraper: BaseScraper, start_date: datetime):
    try:
        logger.info(f"Running {scraper.__name__}...")
        scraper(start_date).standardize(upload_to_db=True)
        logger.info(f"Completed {scraper.__name__}...")
    except Exception as e:
        logger.error(f"Error on {scraper.__name__}: {e}")


def main(start_date: datetime):
    """Run scrapers and export results as CSVs."""

    logger.info(f"Starting scrapers from {start_date.strftime('%Y-%m-%d')}...")

    create_table()

    for scraper in [FDAGhanaScraper, FDAUSAScraper, NafDacScraper, HealthCanadaScraper]:
        run_scraper(scraper, start_date)

    create_csv()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run regulatory scrapers and export CSV output.")

    parser.add_argument(
        "--start-date",
        type=str,
        default="2020-01-01",
        help="Start date in YYYY-MM-DD format (default: 2020-01-01)",
    )

    parser.add_argument(
        "--update-drug-database",
        action="store_true",
        help="Update oncology drug list from NCI.",
    )

    args = parser.parse_args()

    if args.update_drug_database:
        logger.info("Fetching the latest oncology drugs from NCI.")
        base = BaseScraper()
        base.fetch_oncology_drug_names()

    else:
        try:
            start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
        except ValueError:
            logger.error("Invalid date format. Use YYYY-MM-DD.")
            exit(1)

        main(start_date)
