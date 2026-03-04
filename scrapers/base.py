"""
Base classes for the Colorado Foreclosure Tracker scraper system.

The adapter pattern: one scraper class per platform, county config is just a dict.
Every county adapter inherits from CountyScraper and produces ForeclosureRecord objects
that conform to the unified data model.

Dependencies: requests (only stdlib otherwise)
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from typing import Optional
from datetime import datetime, timezone
import json
import logging
import time
import requests

logger = logging.getLogger(__name__)


@dataclass
class ForeclosureRecord:
    """Unified data model for a single foreclosure filing.

    Every county adapter produces records in this format. Not every field
    will be populated from every source -- missing fields are None.
    The id field is the dedup key: {county}-{foreclosure_number}.
    """

    # Required identifiers
    id: str
    county: str
    foreclosure_number: str

    # Parties
    borrower_name: Optional[str] = None
    original_lender: Optional[str] = None
    foreclosing_entity: Optional[str] = None
    attorney: Optional[str] = None
    attorney_file_number: Optional[str] = None

    # Property
    property_address: Optional[str] = None
    legal_description: Optional[str] = None

    # Key dates
    ned_recorded_date: Optional[str] = None
    scheduled_sale_date: Optional[str] = None
    sale_time: Optional[str] = None
    first_publication_date: Optional[str] = None
    last_publication_date: Optional[str] = None

    # Financial
    loan_type: Optional[str] = None
    original_loan_amount: Optional[float] = None
    principal_balance: Optional[float] = None
    total_due: Optional[float] = None
    deficiency: Optional[float] = None
    interest_rate: Optional[float] = None
    winning_bid: Optional[float] = None
    winning_bidder: Optional[str] = None

    # Status
    status: Optional[str] = None
    reception_number: Optional[str] = None

    # Geolocation (populated by geocoder)
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    # Computed fields
    ned_estimated: Optional[bool] = None

    # Metadata
    scraped_at: Optional[str] = None
    source_url: Optional[str] = None
    source: Optional[str] = None
    history: list = field(default_factory=list)

    def to_dict(self) -> dict:
        """Serialize to dict, keeping None values for schema consistency."""
        return asdict(self)

    @staticmethod
    def from_dict(data: dict) -> "ForeclosureRecord":
        """Deserialize from dict, handling missing keys gracefully."""
        known = {f.name for f in ForeclosureRecord.__dataclass_fields__.values()}
        filtered = {k: v for k, v in data.items() if k in known}
        return ForeclosureRecord(**filtered)

    @staticmethod
    def now_iso() -> str:
        """Current UTC timestamp in ISO format."""
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class CountyScraper(ABC):
    """Abstract base class all county scrapers inherit from."""

    REQUEST_DELAY = 2.0

    def __init__(self, county_config: dict):
        self.county = county_config["county_id"]
        self.config = county_config
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })

    @abstractmethod
    def scrape(self) -> list:
        """Return list of ForeclosureRecord for this county."""
        pass

    def make_id(self, foreclosure_number: str) -> str:
        return f"{self.county}-{foreclosure_number}"

    def now_iso(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    MAX_RETRIES = 3

    def throttled_get(self, url: str, **kwargs):
        kwargs.setdefault("timeout", 30)
        for attempt in range(1, self.MAX_RETRIES + 1):
            time.sleep(self.REQUEST_DELAY)
            logger.debug(f"GET {url} (attempt {attempt})")
            try:
                resp = self.session.get(url, **kwargs)
                resp.raise_for_status()
                return resp
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as e:
                if attempt == self.MAX_RETRIES:
                    raise
                wait = 5 * attempt
                logger.warning(f"  GET {url} failed (attempt {attempt}): {e}, retrying in {wait}s")
                time.sleep(wait)

    def throttled_post(self, url: str, **kwargs):
        kwargs.setdefault("timeout", 30)
        for attempt in range(1, self.MAX_RETRIES + 1):
            time.sleep(self.REQUEST_DELAY)
            logger.debug(f"POST {url} (attempt {attempt})")
            try:
                resp = self.session.post(url, **kwargs)
                resp.raise_for_status()
                return resp
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as e:
                if attempt == self.MAX_RETRIES:
                    raise
                wait = 5 * attempt
                logger.warning(f"  POST {url} failed (attempt {attempt}): {e}, retrying in {wait}s")
                time.sleep(wait)


def merge_records(existing, new_records):
    """Merge new scraped records into the existing data file.
    Deduplicates by record id. Updates fields and tracks history on changes."""
    by_id = {r["id"]: r for r in existing}
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for new in new_records:
        new_dict = new.to_dict() if hasattr(new, "to_dict") else new
        rid = new_dict["id"]

        if rid in by_id:
            old = by_id[rid]
            status_changed = (
                new_dict.get("status") and
                new_dict["status"] != old.get("status")
            )
            sale_changed = (
                new_dict.get("scheduled_sale_date") and
                new_dict["scheduled_sale_date"] != old.get("scheduled_sale_date")
            )
            # Update non-null fields
            for key, val in new_dict.items():
                if val is not None and key != "history":
                    old[key] = val
            # Track history on status/sale changes
            if status_changed or sale_changed:
                history = old.get("history", [])
                history.append({
                    "date": today,
                    "status": old.get("status"),
                    "sale_date": old.get("scheduled_sale_date"),
                })
                old["history"] = history
        else:
            if not new_dict.get("history"):
                new_dict["history"] = [{
                    "date": today,
                    "status": new_dict.get("status"),
                    "sale_date": new_dict.get("scheduled_sale_date"),
                }]
            by_id[rid] = new_dict

    return list(by_id.values())


def load_data(path):
    try:
        with open(path, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def save_data(path, records):
    with open(path, "w") as f:
        json.dump(records, f, indent=2, default=str)
    logger.info(f"Saved {len(records)} records to {path}")
