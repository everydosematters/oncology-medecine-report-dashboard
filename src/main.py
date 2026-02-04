"""Main module for the project."""

from __future__ import annotations

from src.scrapers.nafdac import NafDacScraper  # noqa: E402
from datetime import datetime
import pandas as pd
import json


def main():
    # load the sources.json file
    with open("scrapers/sources.json", "r") as f:
        sources = json.load(f)

    nafdac = NafDacScraper(sources["NAFDAC_NG"], datetime(2024, 1, 1))
    nafdac_records = nafdac.standardize()
    df = pd.DataFrame([record.model_dump() for record in nafdac_records])
    # df = df.applymap(
    #     lambda x: ",".join(x) if isinstance(x, list) else x
    # )
    # df.to_csv("nafdac.csv")
    print(f"NAFDAC: {len(df)} records")


if __name__ == "__main__":
    main()
