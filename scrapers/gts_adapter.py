"""
GTS (Gov-Soft) Adapter for Colorado Public Trustee Foreclosure Search.

Handles 7+ counties that use the GTS/Gov-Soft ASP.NET WebForms platform.
Auto-detects field prefix, column count, and FC# format per county.

Validated counties:
  Arapahoe  - 8 cols, MainContent prefix, terms page, 934 records
  Douglas   - 8 cols, MainContent prefix, terms page, 342 records
  Weld      - 8 cols, MainContent prefix, direct search, 603 records
  Larimer   - 7 cols, ContentPlaceHolder1 prefix, direct search, 259 records
  Boulder   - 7 cols, ContentPlaceHolder1 prefix, session URL, 181 records
  El Paso   - 6 cols, MainContent prefix, direct search, 1160 records
  Broomfield- 7 cols, ContentPlaceHolder1 prefix, direct search, 40 records
  Jefferson - currently 503 (down)
"""

import re
import logging
from datetime import datetime, timedelta, timezone
from html import unescape

from scrapers.base import CountyScraper, ForeclosureRecord

logger = logging.getLogger(__name__)

# Two known field prefixes in GTS deployments
PREFIXES = [
    "ctl00$ctl00$MainContent$CustomContentPlaceHolder$",
    "ctl00$ContentPlaceHolder1$",
]


class GTSScraper(CountyScraper):
    """Scrapes GTS/Gov-Soft powered county foreclosure search portals."""

    ROWS_PER_PAGE = 25
    MAX_PAGES = 80
    REQUEST_DELAY = 2.5  # Slightly longer than default to be respectful

    def __init__(self, county_config: dict):
        super().__init__(county_config)
        self.base_url = county_config["search_url"]
        self.field_map = county_config.get("field_map", {})
        self.prefix = None  # Auto-detected
        self.actual_url = None  # May differ from base_url (session IDs etc)

    def _detect_prefix(self, html: str) -> str:
        """Auto-detect the ASP.NET field name prefix from the page HTML."""
        for p in PREFIXES:
            if p in html:
                return p
        # Try to extract from any btnSearch field
        m = re.search(r'name="([^"]*btnSearch)"', html)
        if m:
            prefix = m.group(1).rsplit("btnSearch", 1)[0]
            logger.info(f"  [{self.county}] Detected custom prefix: {prefix}")
            return prefix
        logger.warning(f"  [{self.county}] Could not detect prefix, using default")
        return PREFIXES[0]

    def _field(self, short_name: str) -> str:
        """Resolve a short field name to the full ASP.NET control name."""
        if short_name in self.field_map:
            return self.field_map[short_name]
        return self.prefix + short_name

    def _parse_form_fields(self, html: str) -> dict:
        """Extract all hidden input fields from an ASP.NET WebForms page."""
        fields = {}
        for m in re.finditer(
            r'<input[^>]*name="([^"]*)"[^>]*value="([^"]*)"', html
        ):
            fields[m.group(1)] = unescape(m.group(2))
        return fields

    def _accept_terms(self) -> str:
        """Navigate to the search page, accept terms if present."""
        logger.info(f"  [{self.county}] Fetching {self.base_url}")
        r1 = self.throttled_get(self.base_url, timeout=(15, 60))
        html = r1.text
        self.actual_url = r1.url  # May have redirected (session ID, etc)

        # Auto-detect prefix from whatever page we're on
        self.prefix = self._detect_prefix(html)

        # Check if there's a terms page
        if "btnAcceptTerms" not in html:
            logger.info(f"  [{self.county}] No terms page, already on search")
            return html

        logger.info(f"  [{self.county}] Accepting terms...")
        fields = self._parse_form_fields(html)
        # Find the actual accept button name
        btn = None
        for key in fields:
            if "btnAcceptTerms" in key:
                btn = key
                break
        if not btn:
            btn = self._field("btnAcceptTerms")
        fields[btn] = "Accept Terms"

        r2 = self.throttled_post(self.actual_url, data=fields, allow_redirects=True)
        self.actual_url = r2.url
        # Re-detect prefix on search page (may differ from terms page)
        self.prefix = self._detect_prefix(r2.text)
        return r2.text

    def _do_search(self, search_html: str) -> str:
        """Submit a search for recent NED recordings."""
        fields = self._parse_form_fields(search_html)

        # Set NED date range: last 14 months
        ned_start = (datetime.now() - timedelta(days=425)).strftime("%m/%d/%Y")
        ned_end = datetime.now().strftime("%m/%d/%Y")

        fields[self._field("txtNedDate1")] = ned_start
        fields[self._field("txtNedDate2")] = ned_end
        fields[self._field("btnSearch")] = "Search"

        # Remove competing buttons
        for key in list(fields.keys()):
            if "btnReset" in key or "btnShowAll" in key:
                del fields[key]

        logger.info(f"  [{self.county}] Searching NEDs {ned_start} to {ned_end}")
        r = self.throttled_post(self.actual_url, data=fields, allow_redirects=True)
        self.actual_url = r.url

        if "Runtime Error" in r.text or r.status_code >= 400:
            logger.error(f"  [{self.county}] Search error (status {r.status_code})")
            return ""

        return r.text

    def _detect_pager_style(self, html: str) -> str:
        """Detect which paging mechanism this GTS instance uses.
        
        Returns:
          'nav'  - TopPager/BottomPager nav links (Arapahoe, Douglas, Weld, El Paso)
          'grid' - Grid control Page$N postback (Larimer, Boulder, Broomfield)
          'none' - No paging detected
        """
        if "TopPager" in html or "BottomPager" in html:
            return "nav"
        # Check for Page$N pattern in grid postbacks
        if re.search(r"Page\$\d+", html):
            return "grid"
        return "none"

    def _get_page(self, current_html: str, page_num: int, pager_style: str = "nav") -> str:
        """Navigate to a specific page of results using __doPostBack."""
        fields = self._parse_form_fields(current_html)

        if pager_style == "grid":
            # Grid-based paging: __EVENTTARGET = grid control, __EVENTARGUMENT = Page$N
            grid_name = self._field("gvSearchResults")
            fields["__EVENTTARGET"] = grid_name
            fields["__EVENTARGUMENT"] = f"Page${page_num}"
        else:
            # Nav-based paging: __EVENTTARGET = TopPager$ctlNN$Page
            pager_targets = re.findall(
                r"__doPostBack\(&#39;([^&]*(?:Top|Bottom)Pager[^&]*)&#39;\s*,\s*&#39;&#39;\)",
                current_html
            )
            # Also check unescaped form
            if not pager_targets:
                pager_targets = re.findall(
                    r"__doPostBack\('([^']*(?:Top|Bottom)Pager[^']*)'\s*,\s*''\)",
                    current_html
                )

            event_target = None
            target_ctl = f"ctl{page_num - 1:02d}"
            for t in pager_targets:
                if target_ctl in t:
                    event_target = t
                    break

            if not event_target:
                event_target = self._field(f"TopPager${target_ctl}$Page")
                if "TopPager" not in current_html and "BottomPager" in current_html:
                    event_target = event_target.replace("TopPager", "BottomPager")

            fields["__EVENTTARGET"] = event_target
            fields["__EVENTARGUMENT"] = ""

        # Remove button fields
        for key in list(fields.keys()):
            if any(btn in key for btn in ("btnSearch", "btnReset", "btnShowAll",
                                           "btnAcceptTerms")):
                del fields[key]

        r = self.throttled_post(self.actual_url, data=fields, allow_redirects=True)
        return r.text

    def _detect_columns(self, html: str) -> int:
        """Detect the number of data columns per row.
        
        Looks for rows where the first cell matches a foreclosure number pattern
        (digits, possibly with dashes or letter prefixes like EPC).
        Skips pager rows, header rows, and other non-data content.
        """
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL)
        for row in rows:
            tds = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
            if len(tds) < 6:
                continue
            first_cell = unescape(re.sub(r'<[^>]+>', '', tds[0]).strip())
            first_cell = first_cell.replace('\xa0', '').strip()
            if not first_cell or len(first_cell) < 4 or len(first_cell) > 20:
                continue
            # Must look like a FC number: digits with optional dashes/letters
            # e.g. "0184-2026", "260045", "26-28707", "EPC202600170", "2026-003"
            if re.match(r'^[A-Z]{0,4}\d[\d\-]+$', first_cell, re.I):
                return len(tds)
        return 8

    def _parse_results(self, html: str) -> list:
        """Parse the GTS results table into ForeclosureRecord objects.

        Handles variable column layouts:
          8 cols: FC#, Grantor, Street, Zip, Subdivision, Balance, Sale Date, Status
          7 cols: FC#, Grantor, Street, Zip, Subdivision, Balance, Status
          6 cols: FC#, Grantor, Street, Zip, Subdivision, Balance
        """
        records = []
        now = ForeclosureRecord.now_iso()
        num_cols = self._detect_columns(html)
        county_name = self.config.get("county_name", self.county).title()

        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL)
        for row in rows:
            tds = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
            if len(tds) != num_cols:
                continue

            cells = []
            for td in tds:
                clean = re.sub(r'<[^>]+>', '', td).strip()
                clean = unescape(clean).replace('\xa0', '').strip()
                cells.append(clean)

            fc_num = cells[0]
            if not fc_num:
                continue
            # Must look like a FC number: digits with optional dashes/letters
            # e.g. "0184-2026", "260045", "26-28707", "EPC202600170", "2026-003"
            if not re.match(r'^[A-Z]{0,4}\d[\d\-]+$', fc_num, re.I):
                continue
            if len(fc_num) < 4 or len(fc_num) > 20:
                continue

            borrower = cells[1] if len(cells) > 1 else None
            street = cells[2] if len(cells) > 2 else None
            zipcode = cells[3] if len(cells) > 3 else None
            subdivision = cells[4] if len(cells) > 4 else None

            # Balance is always column 5
            balance_raw = cells[5] if len(cells) > 5 else None
            total_due = None
            if balance_raw:
                cleaned = balance_raw.replace('$', '').replace(',', '')
                try:
                    total_due = float(cleaned)
                except ValueError:
                    pass

            # Sale date (only in 8-col layouts)
            sale_date = None
            if num_cols >= 8:
                sale_date_raw = cells[6]
                if sale_date_raw:
                    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
                        try:
                            dt = datetime.strptime(sale_date_raw, fmt)
                            sale_date = dt.strftime("%Y-%m-%d")
                            break
                        except ValueError:
                            continue

            # Status
            status_raw = None
            if num_cols >= 8:
                status_raw = cells[7]
            elif num_cols == 7:
                status_raw = cells[6]

            status = self._normalize_status(status_raw) if status_raw else "scheduled"

            # Build address
            addr_parts = [street] if street else []
            if zipcode:
                addr_parts.append(zipcode)
            address = ", ".join(p for p in addr_parts if p)
            if address and county_name:
                address += f", {county_name} County, CO"

            rec = ForeclosureRecord(
                id=self.make_id(fc_num),
                county=self.county,
                foreclosure_number=fc_num,
                borrower_name=borrower or None,
                property_address=address or None,
                legal_description=subdivision or None,
                total_due=total_due,
                scheduled_sale_date=sale_date,
                status=status,
                scraped_at=now,
                source_url=self.base_url,
                source="gts",
            )
            records.append(rec)

        return records

    def _normalize_status(self, raw: str) -> str:
        """Map GTS status values to unified status values."""
        if not raw:
            return "scheduled"
        r = raw.lower().strip()

        # Active / pre-sale
        if r in ("ned recorded", "new foreclosure", "publication partial",
                 "publication complete"):
            return "scheduled"
        if r in ("intent to cure filed",):
            return "intent_to_cure"
        if "continued" in r or "pending continuance" in r:
            return "continued"
        if r in ("deferred",):
            return "continued"

        # Bankruptcy
        if "bankruptcy" in r:
            return "bankruptcy"

        # Sale / post-sale
        if r in ("sold", "original sale", "sold - loss mitigation"):
            return "sold"
        if "redeemed" in r or "redemption" in r:
            return "redeemed"
        if r in ("deeded",):
            return "deeded"

        # Closed / withdrawn
        if "withdrawn" in r or "cured" in r or "rescinded" in r:
            return "withdrawn"

        # Court actions
        if "restraining" in r or "set aside" in r:
            return "court_stayed"
        if "resumed" in r or "restarted" in r:
            return "scheduled"

        return raw.lower().replace(" ", "_")

    def _count_pages(self, html: str) -> int:
        """Count how many page links exist in the pager."""
        # Style 1: Nav-based pager (Arapahoe, Douglas, etc.)
        pages = re.findall(r'aria-label="Goto page (\d+)"', html)
        if pages:
            max_visible = max(int(p) for p in pages)
            if "LastPageButton" in html or "..." in html:
                return max_visible + 5
            return max_visible

        pager_links = re.findall(r'(?:Top|Bottom)Pager\$ctl(\d+)\$Page', html)
        if pager_links:
            return max(int(p) for p in pager_links) + 1

        # Style 2: Grid-based pager (Larimer, Boulder, Broomfield)
        # Look for Page$N patterns in __doPostBack calls
        grid_pages = re.findall(r'Page\$(\d+)', html)
        if grid_pages:
            max_page = max(int(p) for p in grid_pages)
            # If there's a Page$Last, there are more pages beyond what's visible
            if 'Page$Last' in html:
                return max_page + 5
            return max_page

        return 1

    def scrape(self) -> list:
        """Main entry point: accept terms, search, paginate, return all records."""
        all_records = []

        try:
            search_html = self._accept_terms()
            if "btnSearch" not in search_html:
                logger.error(f"  [{self.county}] Failed to reach search page")
                return []

            results_html = self._do_search(search_html)
            if not results_html:
                return []

            if "No Records Found" in results_html:
                logger.info(f"  [{self.county}] No records found")
                return []
            total_check = re.search(r'Returned\s+(\d+)\s+Record', results_html)
            if total_check and total_check.group(1) == '0':
                logger.info(f"  [{self.county}] No records found")
                return []

            page_records = self._parse_results(results_html)
            all_records.extend(page_records)

            total_m = re.search(r'(\d+)\s*(?:[Rr]ecord|[Rr]esult)', results_html)
            total_str = total_m.group(1) if total_m else "?"
            logger.info(f"  [{self.county}] Page 1: {len(page_records)} records (total: {total_str})")

            total_pages = self._count_pages(results_html)
            pager_style = self._detect_pager_style(results_html)
            logger.info(f"  [{self.county}] Pager: {pager_style}, est. {total_pages} pages")
            current_html = results_html
            for page in range(2, min(total_pages + 1, self.MAX_PAGES + 1)):
                try:
                    page_html = self._get_page(current_html, page, pager_style)
                    page_recs = self._parse_results(page_html)
                    if not page_recs:
                        logger.info(f"  [{self.county}] Page {page}: empty, stopping")
                        break
                    all_records.extend(page_recs)
                    logger.info(f"  [{self.county}] Page {page}: {len(page_recs)} records")
                    current_html = page_html
                    # For grid-style paging, re-check page count as new pages may appear
                    if pager_style == "grid":
                        new_total = self._count_pages(page_html)
                        if new_total > total_pages:
                            total_pages = new_total
                except Exception as e:
                    logger.warning(f"  [{self.county}] Page {page} failed: {e}")
                    break

            logger.info(f"  [{self.county}] Total scraped: {len(all_records)} records")

        except Exception as e:
            logger.error(f"  [{self.county}] Scrape failed: {e}", exc_info=True)

        return all_records
