# run_scrapers.py (example runner)
from scrapers.base import BaseScraper
from scrapers.fda_usa import FDAScraper
from scrapers.nafdac import NafDacScraper

DB_PATH = "recalls.db"

def main():
    BaseScraper.init_db(DB_PATH)

    fda = FDAScraper("config/sources.json")
    fda_records = fda.standardize()
    fda.upload_to_sqlite(DB_PATH, fda_records)
    print(f"FDA: {len(fda_records)} records")

    nafdac = NafDacScraper("config/sources.json")
    nafdac_records = nafdac.standardize()
    nafdac.upload_to_sqlite(DB_PATH, nafdac_records)
    print(f"NAFDAC: {len(nafdac_records)} records")

if __name__ == "__main__":
    main()
