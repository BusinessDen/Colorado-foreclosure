#!/usr/bin/env python3
"""Colorado Foreclosure Tracker - Main Scraper."""
import sys, json, logging
from pathlib import Path
from scrapers.base import load_data, save_data, merge_records

ADAPTERS = {
    "denver_custom": "scrapers.denver_adapter:DenverScraper",
}
DATA_FILE = "foreclosure-data.json"
CONFIG_FILE = "config/counties.json"
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("scraper")

def load_adapter(path):
    mod_path, cls_name = path.split(":")
    module = __import__(mod_path, fromlist=[cls_name])
    return getattr(module, cls_name)

def main():
    root = Path(__file__).parent
    with open(root / CONFIG_FILE) as f:
        counties = json.load(f)
    filt = sys.argv[1] if len(sys.argv) > 1 else None
    data_path = root / DATA_FILE
    existing = load_data(str(data_path))
    logger.info(f"Loaded {len(existing)} existing records")
    all_new = []
    for cid, cfg in counties.items():
        if not cfg.get("enabled", True): continue
        if filt and cid != filt: continue
        platform = cfg.get("platform", "")
        ap = ADAPTERS.get(platform)
        if not ap:
            logger.warning(f"No adapter for {platform} ({cid})")
            continue
        name = cfg.get("county_name", cfg.get("display_name", cid))
        logger.info(f"Scraping {name}...")
        try:
            Cls = load_adapter(ap)
            recs = Cls(cfg).scrape()
            logger.info(f"  -> {len(recs)} records")
            all_new.extend(recs)
        except Exception as e:
            logger.error(f"  -> FAILED: {e}", exc_info=True)
    if all_new:
        merged = merge_records(existing, all_new)
        save_data(str(data_path), merged)
        logger.info(f"Final total: {len(merged)} records")
    else:
        logger.info("No new records")

if __name__ == "__main__":
    main()
