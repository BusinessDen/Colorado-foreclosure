"""
Denver County Foreclosure Scraper

Data sources (all free, public, no API keys needed):
1. Bid Grid PDF - upcoming auction cases with financial details
2. Sales Results PDF - completed auction results with winning bids
3. Continuances PDF - cases continued (postponed)
4. PublicSearch API - Notice of Election and Demand filings from recorder

Cost: $0. All sources are public government data.
"""

import re
import io
import time
import logging
from datetime import datetime, timedelta
from typing import Optional, List

import requests

from .base import CountyScraper, ForeclosureRecord

logger = logging.getLogger(__name__)

BIDGRID_URL = "https://www.denvergov.org/media/denvergov/clerkandrecorder/BidGrid/Current_BidGrid.pdf"
SALES_RESULTS_URL = "https://www.denvergov.org/media/denvergov/clerkandrecorder/AuctionResults/Current_Sales_Results.pdf"
CONTINUANCES_URL = "https://www.denvergov.org/media/denvergov/clerkandrecorder/Continuances/Current_Continuances.pdf"
PUBLICSEARCH_BASE = "https://denver.co.publicsearch.us"
DELAY = 2.0


class DenverScraper(CountyScraper):
    """Scrapes Denver County foreclosure data from multiple free public sources."""

    def __init__(self, county_config=None):
        if county_config is None:
            county_config = {"county_id": "denver", "platform": "denver_custom", "display_name": "Denver"}
        super().__init__(county_config)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Accept": "application/pdf,text/html,application/json,*/*",
        })

    def scrape(self) -> List[ForeclosureRecord]:
        records_by_case = {}

        # Source 1: Bid Grid
        try:
            for r in self._scrape_bidgrid():
                records_by_case[r.foreclosure_number] = r
            logger.info(f"Bid Grid: {len(records_by_case)} records")
        except Exception as e:
            logger.warning(f"Bid Grid failed: {e}")
        time.sleep(DELAY)

        # Source 2: Continuances
        try:
            ct = 0
            for r in self._scrape_continuances():
                if r.foreclosure_number in records_by_case:
                    ex = records_by_case[r.foreclosure_number]
                    ex.status = "continued"
                    if r.scheduled_sale_date:
                        ex.scheduled_sale_date = r.scheduled_sale_date
                    if r.borrower_name and not ex.borrower_name:
                        ex.borrower_name = r.borrower_name
                    if r.property_address and not ex.property_address:
                        ex.property_address = r.property_address
                else:
                    records_by_case[r.foreclosure_number] = r
                ct += 1
            logger.info(f"Continuances: {ct} records")
        except Exception as e:
            logger.warning(f"Continuances failed: {e}")
        time.sleep(DELAY)

        # Source 3: Sales Results
        try:
            sc = 0
            for r in self._scrape_sales_results():
                if r.foreclosure_number in records_by_case:
                    ex = records_by_case[r.foreclosure_number]
                    ex.status = "sold"
                    if r.winning_bid:
                        ex.winning_bid = r.winning_bid
                    if r.attorney and not ex.attorney:
                        ex.attorney = r.attorney
                else:
                    records_by_case[r.foreclosure_number] = r
                sc += 1
            logger.info(f"Sales Results: {sc} records")
        except Exception as e:
            logger.warning(f"Sales Results failed: {e}")
        time.sleep(DELAY)

        # Source 4: PublicSearch NEDs
        try:
            nc = 0
            for r in self._scrape_publicsearch_neds():
                if r.foreclosure_number not in records_by_case:
                    records_by_case[r.foreclosure_number] = r
                    nc += 1
            logger.info(f"PublicSearch NEDs: {nc} new records")
        except Exception as e:
            logger.warning(f"PublicSearch NEDs failed: {e}")

        result = list(records_by_case.values())
        logger.info(f"Denver total: {len(result)} unique records")
        return result

    def _download_pdf(self, url):
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        if b"%PDF" not in resp.content[:10]:
            raise ValueError(f"Not a PDF: {url}")
        return resp.content

    def _get_pdfplumber(self):
        try:
            import pdfplumber
            return pdfplumber
        except ImportError:
            logger.warning("pdfplumber not installed")
            return None

    # --- Bid Grid ---
    def _scrape_bidgrid(self):
        pdfplumber = self._get_pdfplumber()
        if not pdfplumber:
            return []
        pdf_bytes = self._download_pdf(BIDGRID_URL)
        records = []
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    records.extend(self._parse_case_blocks(text, self._parse_bidgrid_case))
        return records

    def _parse_bidgrid_case(self, case_num, text):
        amounts = re.findall(r"\$[\d,]+\.\d{2}", text)
        written_bid = self._money(amounts[-3]) if len(amounts) >= 3 else None
        deficiency = self._money(amounts[-2]) if len(amounts) >= 2 else None
        total_due = self._money(amounts[-1]) if len(amounts) >= 1 else None
        dates = re.findall(r"\d{2}/\d{2}/\d{4}", text)
        first_pub = self._to_iso_date(dates[0]) if len(dates) >= 1 else None
        last_pub = self._to_iso_date(dates[1]) if len(dates) >= 2 else None
        clean = re.sub(r"\$[\d,]+\.\d{2}", "", text)
        clean = re.sub(r"\d{2}/\d{2}/\d{4}", "", clean)
        addr, grantor, bene = self._extract_addr_parties(clean)
        return ForeclosureRecord(
            id=self.make_id(case_num), county="denver", foreclosure_number=case_num,
            borrower_name=grantor, property_address=addr, foreclosing_entity=bene,
            first_publication_date=first_pub, last_publication_date=last_pub,
            original_loan_amount=written_bid, total_due=total_due, deficiency=deficiency,
            status="active", scraped_at=ForeclosureRecord.now_iso(),
            source_url=BIDGRID_URL, source="bidgrid")

    # --- Continuances ---
    def _scrape_continuances(self):
        pdfplumber = self._get_pdfplumber()
        if not pdfplumber:
            return []
        pdf_bytes = self._download_pdf(CONTINUANCES_URL)
        records = []
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    records.extend(self._parse_case_blocks(text, self._parse_cont_case))
        return records

    def _parse_cont_case(self, case_num, text):
        dates = re.findall(r"\d{2}/\d{2}/\d{4}", text)
        cont_date = self._to_iso_date(dates[-1]) if dates else None
        sm = re.search(r"(Bankruptcy|Court Order|Cure|Redemption)\s*$", text, re.IGNORECASE)
        note = sm.group(1).lower() if sm else None
        am = re.search(r"(\d+\s+[\w\s.,#]+(?:DENVER|Denver),?\s*(?:CO|Colorado|COLORADO)\s*\d{5}(?:-\d{4})?)", text)
        addr = am.group(1).strip() if am else None
        grantor = None
        if am:
            before = re.sub(r"\d{2}/\d{2}/\d{4}", "", text[:am.start()]).strip()
            grantor = self._clean_name(before)
        status = f"continued ({note})" if note else "continued"
        return ForeclosureRecord(
            id=self.make_id(case_num), county="denver", foreclosure_number=case_num,
            borrower_name=grantor, property_address=addr, scheduled_sale_date=cont_date,
            status=status, scraped_at=ForeclosureRecord.now_iso(),
            source_url=CONTINUANCES_URL, source="continuances")

    # --- Sales Results ---
    def _scrape_sales_results(self):
        pdfplumber = self._get_pdfplumber()
        if not pdfplumber:
            return []
        pdf_bytes = self._download_pdf(SALES_RESULTS_URL)
        records = []
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    records.extend(self._parse_case_blocks(text, self._parse_sales_case))
        return records

    def _parse_sales_case(self, case_num, text):
        amounts = re.findall(r"\$[\d,]+\.\d{2}", text)
        written_bid = self._money(amounts[0]) if len(amounts) >= 1 else None
        deficiency = self._money(amounts[1]) if len(amounts) >= 2 else None
        total_due = self._money(amounts[2]) if len(amounts) >= 3 else None
        winning_bid = self._money(amounts[-1]) if len(amounts) >= 4 else None
        dates = re.findall(r"\d{2}/\d{2}/\d{4}", text)
        first_pub = self._to_iso_date(dates[0]) if len(dates) >= 1 else None
        last_pub = self._to_iso_date(dates[1]) if len(dates) >= 2 else None
        am = re.search(r"(\d+\s+[\w\s.,#]+(?:DENVER|Denver),?\s*(?:CO|Colorado)\s*\d{5}(?:-\d{4})?)", text)
        addr = am.group(1).strip() if am else None
        atm = re.search(r"((?:MCCARTHY|JANEWAY|BARRETT|HALLIDAY|CASTLE|SNELL)[^$]*?(?:LLP|P\.C\.|LLC))", text, re.IGNORECASE)
        attorney = atm.group(1).strip() if atm else None
        fm = re.search(r"(CO-\d{2}-\d{7}-\w+|\d{8,})", text)
        atty_file = fm.group(1) if fm else None
        return ForeclosureRecord(
            id=self.make_id(case_num), county="denver", foreclosure_number=case_num,
            property_address=addr, attorney=attorney, attorney_file_number=atty_file,
            first_publication_date=first_pub, last_publication_date=last_pub,
            original_loan_amount=written_bid, total_due=total_due, deficiency=deficiency,
            winning_bid=winning_bid, status="sold", scraped_at=ForeclosureRecord.now_iso(),
            source_url=SALES_RESULTS_URL, source="sales_results")

    # --- PublicSearch NEDs ---
    def _scrape_publicsearch_neds(self):
        records = []
        end = datetime.now()
        start = end - timedelta(days=90)
        try:
            url = f"{PUBLICSEARCH_BASE}/results"
            params = {
                "department": "RP", "limit": "200", "offset": "0",
                "recordedDateRange": start.strftime("%m/%d/%Y") + "," + end.strftime("%m/%d/%Y"),
                "searchOcrText": "false", "searchType": "quickSearch", "docTypes": "NED",
            }
            self.session.headers["Accept"] = "application/json"
            resp = self.session.get(url, params=params, timeout=30)
            if resp.status_code == 200 and "json" in resp.headers.get("content-type", ""):
                records = self._parse_ps(resp.json())
            else:
                logger.info("PublicSearch: no JSON results; PDF sources are primary")
        except Exception as e:
            logger.warning(f"PublicSearch error: {e}")
        return records

    def _parse_ps(self, data):
        records = []
        for doc in data.get("results", data.get("documents", [])):
            try:
                dn = doc.get("docNumber", doc.get("instrumentNumber", ""))
                if not dn:
                    continue
                rd = doc.get("recordedDate", "")
                if rd:
                    for fmt in ["%m/%d/%Y", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"]:
                        try:
                            rd = datetime.strptime(rd[:10], fmt).strftime("%Y-%m-%d")
                            break
                        except ValueError:
                            continue
                grantors = [p.get("name", "") for p in doc.get("parties", []) if "grantor" in p.get("type", "").lower()]
                grantees = [p.get("name", "") for p in doc.get("parties", []) if "grantee" in p.get("type", "").lower()]
                pa = ""
                for a in doc.get("propertyAddresses", doc.get("addresses", [])):
                    if isinstance(a, dict):
                        pa = ", ".join(x for x in [a.get("address1",""), a.get("city",""), a.get("state",""), a.get("zip","")] if x)
                    elif isinstance(a, str):
                        pa = a
                    break
                records.append(ForeclosureRecord(
                    id=self.make_id(f"NED-{dn}"), county="denver", foreclosure_number=f"NED-{dn}",
                    borrower_name="; ".join(grantors) or None, property_address=pa or None,
                    foreclosing_entity="; ".join(grantees) or None, ned_recorded_date=rd or None,
                    reception_number=dn, status="active", scraped_at=ForeclosureRecord.now_iso(),
                    source_url=f"{PUBLICSEARCH_BASE}/doc/{dn}", source="publicsearch"))
            except Exception:
                continue
        return records

    # --- Helpers ---
    def _parse_case_blocks(self, text, parser_fn):
        records = []
        lines = text.split("\n")
        i = 0
        while i < len(lines):
            m = re.match(r"^(\d{4}-\d{6})\s+(.*)", lines[i])
            if m:
                case_num = m.group(1)
                full = m.group(2)
                while i + 1 < len(lines):
                    nxt = lines[i + 1]
                    if re.match(r"^\d{4}-\d{6}", nxt) or nxt.startswith("Report") or nxt.startswith("Printed") or nxt.startswith("Page"):
                        break
                    i += 1
                    if nxt.strip():
                        full += " " + nxt.strip()
                try:
                    rec = parser_fn(case_num, full)
                    if rec:
                        records.append(rec)
                except Exception as e:
                    logger.debug(f"Failed to parse case {case_num}: {e}")
            i += 1
        return records

    def _extract_addr_parties(self, text):
        m = re.search(r"(\d+\s+[\w\s.,#]+(?:DENVER|Denver),?\s*(?:CO|Colorado|COLORADO)\s*\d{5}(?:-\d{4})?)", text)
        if not m:
            return None, None, None
        return m.group(1).strip(), self._clean_name(text[:m.start()]), self._clean_name(text[m.end():])

    @staticmethod
    def _money(s):
        if not s:
            return None
        try:
            return float(s.replace("$", "").replace(",", ""))
        except (ValueError, AttributeError):
            return None

    @staticmethod
    def _to_iso_date(s):
        if not s:
            return None
        try:
            return datetime.strptime(s, "%m/%d/%Y").strftime("%Y-%m-%d")
        except ValueError:
            return None

    @staticmethod
    def _clean_name(s):
        if not s:
            return None
        s = re.sub(r"\s+", " ", s).strip(", ")
        return s if s else None
