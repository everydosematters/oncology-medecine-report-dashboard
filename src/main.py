"""Entry point for running scrapers and exporting CSV output."""

from __future__ import annotations

from datetime import datetime

import json
import pandas as pd
import sqlite3

from src.scrapers.nafdac import NafDacScraper  # noqa: E402
from src.scrapers.fdausa import FDAUSAScraper
from database import create_table, upsert_df


def main():
    """Run scrapers and export results as CSVs."""

    # Resolve sources.json relative to this file so it works regardless of CWD
    # FIXME move this inside the class instatiaiton
    # create_table()

    with open("scrapers/sources.json", "r") as f:
        sources = json.load(f)

    
    # Example: run FDA USA scraper from 2024-01-01 onwards
    fdausa = FDAUSAScraper(sources["FDA_US"], datetime(2024, 1, 1))
    # fdausa.fetch_oncology_drug_names()
    fda_records = fdausa.standardize()

    df = pd.DataFrame([record.model_dump() for record in fda_records])
    
    # # Example: run NAFDAC scraper (commented out by default)
    nafdac = NafDacScraper(sources["NAFDAC_NG"], datetime(2024, 1, 1))
    nafdac_records = nafdac.standardize()

    df = pd.concat([df, pd.DataFrame([record.model_dump() for record in nafdac_records])])
    df.sort_values("publish_date", inplace=True)
    
    with sqlite3.connect("recalls.db") as conn:
        upsert_df(conn, df)

if __name__ == "__main__":
    main()
