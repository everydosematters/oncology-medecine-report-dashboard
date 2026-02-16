"""Entry point for running scrapers and exporting CSV output."""

from __future__ import annotations

from datetime import datetime

import json
import pandas as pd

from src.scrapers.nafdac import NafDacScraper  # noqa: E402
from src.scrapers.fdausa import FDAUSAScraper


def main():
    """Run scrapers and export results as CSVs."""

    # Resolve sources.json relative to this file so it works regardless of CWD
    with open("scrapers/sources.json", "r") as f:
        sources = json.load(f)

    # Example: run FDA USA scraper from 2024-01-01 onwards
    fdausa = FDAUSAScraper(sources["FDA_US"], datetime(2025, 1, 1))
    fdausa.fetch_cancer_drug_names()
    fda_records = fdausa.standardize()

    df = pd.DataFrame([record.model_dump() for record in fda_records])
    df = df.applymap(lambda x: ",".join(x) if isinstance(x, list) else x)
    df.to_csv("fdarecords.csv", index=False)

    # # Example: run NAFDAC scraper (commented out by default)
    # nafdac = NafDacScraper(sources["NAFDAC_NG"], datetime(2024, 1, 1))
    # nafdac_records = nafdac.standardize()

    # df = pd.DataFrame([record.model_dump() for record in fda_records + nafdac_records])
    # df = df.applymap(lambda x: ",".join(x) if isinstance(x, list) else x)
    # df.to_csv("records.csv", index=False)
    # print(f"Records: {len(df)} records")


if __name__ == "__main__":
    main()
