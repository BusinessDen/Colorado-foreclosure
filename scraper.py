#!/usr/bin/env python3
"""Colorado Foreclosure Tracker - Main Scraper."""
import sys, json, logging, time, re
from pathlib import Path
from scrapers.base import load_data, save_data, merge_records
import requests

ADAPTERS = {
    "denver_custom": "scrapers.denver_adapter:DenverScraper",
}
DATA_FILE = "foreclosure-data.json"
CONFIG_FILE = "config/counties.json"
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("scraper")

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_HEADERS = {
    "User-Agent": "ColoradoForeclosureTracker/1.0 (github.com/BusinessDen/Colorado-foreclosure)"
}

def load_adapter(path):
    mod_path, cls_name = path.split(":")
    module = __import__(mod_path, fromlist=[cls_name])
    return getattr(module, cls_name)


def clean_address_for_geocoding(addr):
    """Strip lender names, apartment numbers, and normalize for Nominatim."""
    if not addr:
        return None
    clean = addr

    # PATTERN 1: Bidgrid addresses have lender after 2+ spaces
    # e.g. "9444 E GIRARD AVE, APARTMENT 12   AMERIHOME MORTGAGE..."
    # Split on 2+ spaces and keep only the first part (the street address)
    parts = re.split(r'\s{2,}', clean)
    clean = parts[0].strip()

    # PATTERN 2: Continuance addresses start with "YYYY" or "YYYY Reason"
    # e.g. "2026 Bankruptcy 344 S HOLLY ST DENVER, CO 80246"
    # e.g. "2026 4400 S QUEBEC ST APARTMENT E105 DENVER, CO 80237"
    clean = re.sub(r'^\d{4}\s+(Bankruptcy|Court\s*Order|Cure|Redemption)\s+', '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'^\d{4}\s+(?=\d)', '', clean)  # "2026 4400..." -> "4400..."

    # Remove apartment/unit/condo numbers (confuses geocoder)
    clean = re.sub(r',?\s*(APARTMENT|APT|UNIT|STE|SUITE|CONDO|#)\s*\S+', '', clean, flags=re.IGNORECASE)

    # Remove periods from abbreviations (N. -> N, ST. -> ST)
    clean = re.sub(r'\.(?=\s|,|$)', '', clean)

    # Normalize whitespace
    clean = re.sub(r'\s+', ' ', clean).strip().rstrip(',')

    # Ensure Denver, CO is present
    upper = clean.upper()
    if 'DENVER' not in upper and 'CO ' not in upper and not re.search(r'\d{5}', clean):
        clean += ', Denver, CO'
    elif 'DENVER' in upper and ', CO' not in upper and 'COLORADO' not in upper:
        clean += ', CO'

    return clean


def geocode_records(records):
    """Geocode records that are missing lat/lng using Nominatim (free, 1 req/sec)."""
    needs_geocoding = [
        r for r in records
        if r.get('property_address')
        and (r.get('latitude') is None or r.get('longitude') is None)
    ]
    if not needs_geocoding:
        logger.info("All records already geocoded")
        return

    logger.info(f"Geocoding {len(needs_geocoding)} records via Nominatim...")
    success = 0
    for i, rec in enumerate(needs_geocoding):
        addr = clean_address_for_geocoding(rec['property_address'])
        if not addr:
            continue
        try:
            time.sleep(1.1)  # Respect Nominatim 1 req/sec policy
            resp = requests.get(NOMINATIM_URL, params={
                'q': addr,
                'format': 'json',
                'limit': 1,
                'countrycodes': 'us',
                'addressdetails': 0,
            }, headers=NOMINATIM_HEADERS, timeout=10)
            resp.raise_for_status()
            results = resp.json()
            if results:
                rec['latitude'] = float(results[0]['lat'])
                rec['longitude'] = float(results[0]['lon'])
                success += 1
                logger.info(f"  [{i+1}/{len(needs_geocoding)}] {addr[:50]} -> {rec['latitude']:.4f}, {rec['longitude']:.4f}")
            else:
                logger.warning(f"  [{i+1}/{len(needs_geocoding)}] No result for: {addr[:60]}")
        except Exception as e:
            logger.warning(f"  [{i+1}/{len(needs_geocoding)}] Geocode error: {e}")

    logger.info(f"Geocoded {success}/{len(needs_geocoding)} addresses")

def estimate_ned_dates(records):
    """Estimate NED recording date from sale date using CO law (110-125 days).
    Uses midpoint of 117 days. Only fills in if ned_recorded_date is empty."""
    from datetime import timedelta
    count = 0
    for rec in records:
        if rec.get('ned_recorded_date'):
            continue  # Already has actual NED date
        sale = rec.get('scheduled_sale_date')
        if not sale:
            continue
        try:
            sale_dt = datetime.strptime(sale, '%Y-%m-%d')
            est_ned = sale_dt - timedelta(days=117)  # midpoint of 110-125
            rec['ned_recorded_date'] = est_ned.strftime('%Y-%m-%d')
            rec['ned_estimated'] = True
            count += 1
        except (ValueError, TypeError):
            continue
    logger.info(f"Estimated NED dates for {count} records (sale date - 117 days)")


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
        estimate_ned_dates(merged)
        geocode_records(merged)
        save_data(str(data_path), merged)
        logger.info(f"Final total: {len(merged)} records")
    else:
        # Even if no new records, estimate NED and geocode any that are missing
        estimate_ned_dates(existing)
        if any(r.get('latitude') is None and r.get('property_address') for r in existing):
            geocode_records(existing)
            save_data(str(data_path), existing)
        else:
            # Save if NED dates were estimated
            if any(r.get('ned_estimated') for r in existing):
                save_data(str(data_path), existing)
        logger.info("No new records")

if __name__ == "__main__":
    main()
