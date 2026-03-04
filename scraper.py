#!/usr/bin/env python3
"""Colorado Foreclosure Tracker - Main Scraper."""
import sys, json, logging, time, re
from pathlib import Path
from datetime import datetime
from scrapers.base import load_data, save_data, merge_records
import requests

ADAPTERS = {
    "denver_custom": "scrapers.denver_adapter:DenverScraper",
    "gts": "scrapers.gts_adapter:GTSScraper",
}
DATA_FILE = "foreclosure-data.json"
CONFIG_FILE = "config/counties.json"
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("scraper")

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_HEADERS = {
    "User-Agent": "ColoradoForeclosureTracker/1.0 (github.com/BusinessDen/Colorado-foreclosure)"
}
MAX_GEOCODE_PER_RUN = 800

# Colorado bounding box (generous) for filtering bad geocodes
CO_LAT_MIN, CO_LAT_MAX = 36.99, 41.01
CO_LNG_MIN, CO_LNG_MAX = -109.06, -102.04

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
    parts = re.split(r'\s{2,}', clean)
    clean = parts[0].strip()

    # PATTERN 2: Continuance addresses start with "YYYY" or "YYYY Reason"
    clean = re.sub(r'^\d{4}\s+(Bankruptcy|Court\s*Order|Cure|Redemption)\s+', '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'^\d{4}\s+(?=\d)', '', clean)

    # Remove apartment/unit/condo numbers (confuses geocoder)
    clean = re.sub(r',?\s*(APARTMENT|APT|UNIT|STE|SUITE|CONDO|#)\s*\S+', '', clean, flags=re.IGNORECASE)

    # Remove periods from abbreviations
    clean = re.sub(r'\.(?=\s|,|$)', '', clean)

    # Strip "County, CO" suffix from GTS addresses (e.g. ", Arapahoe County, CO")
    clean = re.sub(r',\s*[\w\s]+County,\s*CO\s*$', '', clean, flags=re.IGNORECASE)

    # Strip zip+4 -> zip5
    clean = re.sub(r'(\d{5})-\d{4}', r'\1', clean)

    # Normalize whitespace
    clean = re.sub(r'\s+', ' ', clean).strip().rstrip(',')

    # Abbreviate common street terms for Nominatim compatibility
    _abbrevs = [
        (r'\bNORTH\b', 'N'), (r'\bSOUTH\b', 'S'), (r'\bEAST\b', 'E'), (r'\bWEST\b', 'W'),
        (r'\bSTREET\b', 'St'), (r'\bAVENUE\b', 'Ave'), (r'\bBOULEVARD\b', 'Blvd'),
        (r'\bDRIVE\b', 'Dr'), (r'\bCOURT\b', 'Ct'), (r'\bCIRCLE\b', 'Cir'),
        (r'\bPLACE\b', 'Pl'), (r'\bLANE\b', 'Ln'), (r'\bROAD\b', 'Rd'),
        (r'\bTERRACE\b', 'Ter'), (r'\bPARKWAY\b', 'Pkwy'), (r'\bTRAIL\b', 'Trl'),
    ]
    for pattern, repl in _abbrevs:
        clean = re.sub(pattern, repl, clean, flags=re.IGNORECASE)

    return clean


# Common Colorado cities for address parsing
_CO_CITIES = (
    'DENVER|AURORA|LAKEWOOD|LITTLETON|ENGLEWOOD|THORNTON|ARVADA|WESTMINSTER|'
    'BOULDER|LONGMONT|LOVELAND|FORT COLLINS|GREELEY|COLORADO SPRINGS|'
    'CASTLE ROCK|PARKER|HIGHLANDS RANCH|CENTENNIAL|BROOMFIELD|BRIGHTON|'
    'COMMERCE CITY|NORTHGLENN|FEDERAL HEIGHTS|GOLDEN|WHEAT RIDGE|EDGEWATER|'
    'SHERIDAN|GLENDALE|LONE TREE|CASTLE PINES|ERIE|LOUISVILLE|SUPERIOR|'
    'LAFAYETTE|FREDERICK|FIRESTONE|DACONO|MEAD|JOHNSTOWN|BERTHOUD|'
    'WINDSOR|TIMNATH|WELLINGTON|SEVERANCE|EVANS|GARDEN CITY|KERSEY|'
    'FOUNTAIN|MONUMENT|PALMER LAKE|WOODLAND PARK|MANITOU SPRINGS|'
    'SECURITY|WIDEFIELD|PUEBLO|CANON CITY'
)

def _parse_address_parts(cleaned_addr):
    """Split a cleaned address into (street, city, state, zip) for structured query."""
    if not cleaned_addr:
        return None, None, None, None

    # Extract zip from end
    zip_m = re.search(r'\b(\d{5})\s*$', cleaned_addr)
    zipcode = zip_m.group(1) if zip_m else None
    if zipcode:
        cleaned_addr = cleaned_addr[:zip_m.start()].strip().rstrip(',')

    # Strip trailing state
    cleaned_addr = re.sub(r',?\s*\bCO\b\s*$', '', cleaned_addr, flags=re.IGNORECASE).strip().rstrip(',')

    # Try comma-separated parts
    parts = [p.strip() for p in cleaned_addr.split(',') if p.strip()]

    # Remove any parts that are just a zip code
    non_zip_parts = []
    for p in parts:
        if re.match(r'^\d{5}$', p):
            zipcode = zipcode or p
        else:
            non_zip_parts.append(p)
    parts = non_zip_parts

    city = None
    if len(parts) >= 2:
        street = parts[0]
        city = parts[-1].strip()
    elif len(parts) == 1:
        street = parts[0]
    else:
        return None, None, None, None

    # If city is still None, try to extract a known city name from end of street
    if not city:
        city_m = re.search(
            r'\s+(' + _CO_CITIES + r')\s*$',
            street, re.IGNORECASE
        )
        if city_m:
            city = city_m.group(1)
            street = street[:city_m.start()].strip()

    return street, city or None, 'CO', zipcode


def geocode_records(records):
    """Geocode records missing lat/lng using Nominatim structured queries."""
    needs_geocoding = [
        r for r in records
        if r.get('property_address')
        and (r.get('latitude') is None or r.get('longitude') is None)
        and not r.get('geocode_failed')
    ]
    if not needs_geocoding:
        logger.info("All records already geocoded (or marked as unfindable)")
        return

    batch = needs_geocoding[:MAX_GEOCODE_PER_RUN]
    logger.info(f"Geocoding {len(batch)}/{len(needs_geocoding)} records via Nominatim...")
    if len(needs_geocoding) > MAX_GEOCODE_PER_RUN:
        logger.info(f"  (capped at {MAX_GEOCODE_PER_RUN}/run; {len(needs_geocoding)-MAX_GEOCODE_PER_RUN} deferred to next run)")
    success = 0
    for i, rec in enumerate(batch):
        addr = clean_address_for_geocoding(rec['property_address'])
        if not addr:
            rec['geocode_failed'] = True
            continue
        street, city, state, zipcode = _parse_address_parts(addr)
        if not street:
            rec['geocode_failed'] = True
            continue
        try:
            time.sleep(1.1)  # Respect Nominatim 1 req/sec policy
            # Structured query for better results
            params = {
                'street': street,
                'state': 'Colorado',
                'country': 'US',
                'format': 'json',
                'limit': 1,
            }
            if city:
                params['city'] = city
            if zipcode:
                params['postalcode'] = zipcode

            resp = requests.get(NOMINATIM_URL, params=params,
                                headers=NOMINATIM_HEADERS, timeout=10)
            resp.raise_for_status()
            results = resp.json()

            # If structured query fails, retry free-form
            if not results and city:
                time.sleep(1.1)
                freeform = f"{street}, {city}, CO"
                resp = requests.get(NOMINATIM_URL, params={
                    'q': freeform, 'format': 'json', 'limit': 1,
                    'countrycodes': 'us',
                }, headers=NOMINATIM_HEADERS, timeout=10)
                resp.raise_for_status()
                results = resp.json()

            if results:
                lat = float(results[0]['lat'])
                lng = float(results[0]['lon'])
                if CO_LAT_MIN <= lat <= CO_LAT_MAX and CO_LNG_MIN <= lng <= CO_LNG_MAX:
                    rec['latitude'] = lat
                    rec['longitude'] = lng
                    success += 1
                    if (i + 1) % 50 == 0 or i < 5:
                        logger.info(f"  [{i+1}/{len(batch)}] {street[:40]} -> {lat:.4f}, {lng:.4f}")
                else:
                    logger.warning(f"  [{i+1}/{len(batch)}] Outside CO ({lat:.2f},{lng:.2f}): {street[:40]}")
                    rec['geocode_failed'] = True
            else:
                logger.warning(f"  [{i+1}/{len(batch)}] No result: {street[:50]}")
                rec['geocode_failed'] = True
        except Exception as e:
            logger.warning(f"  [{i+1}/{len(batch)}] Geocode error: {e}")

    logger.info(f"Geocoded {success}/{len(batch)} addresses")


def scrub_bad_geocodes(records):
    """Remove lat/lng from records outside Colorado bounds so they get re-geocoded."""
    count = 0
    for rec in records:
        lat = rec.get('latitude')
        lng = rec.get('longitude')
        if lat is not None and lng is not None:
            if not (CO_LAT_MIN <= lat <= CO_LAT_MAX and CO_LNG_MIN <= lng <= CO_LNG_MAX):
                logger.info(f"  Scrubbed bad geocode ({lat:.2f},{lng:.2f}): {rec.get('property_address','?')[:50]}")
                rec['latitude'] = None
                rec['longitude'] = None
                count += 1
    if count:
        logger.info(f"Scrubbed {count} out-of-Colorado geocodes")


def estimate_ned_dates(records):
    """Estimate NED recording date from sale date using CO law (110-125 days).
    Uses midpoint of 117 days. Only fills in if ned_recorded_date is empty."""
    from datetime import timedelta
    count = 0
    for rec in records:
        if rec.get('ned_recorded_date'):
            continue
        sale = rec.get('scheduled_sale_date')
        if not sale:
            continue
        try:
            sale_dt = datetime.strptime(sale, '%Y-%m-%d')
            est_ned = sale_dt - timedelta(days=117)
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
        scrub_bad_geocodes(merged)
        geocode_records(merged)
        save_data(str(data_path), merged)
        logger.info(f"Final total: {len(merged)} records")
    else:
        estimate_ned_dates(existing)
        scrub_bad_geocodes(existing)
        if any(r.get('latitude') is None and r.get('property_address') for r in existing):
            geocode_records(existing)
            save_data(str(data_path), existing)
        else:
            if any(r.get('ned_estimated') for r in existing):
                save_data(str(data_path), existing)
        logger.info("No new records")

if __name__ == "__main__":
    main()
