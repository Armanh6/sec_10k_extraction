
#!/usr/bin/env python3
# SEC 10-K extractor (Submissions + Archives) with fallbacks + consolidated-context preference
#
#
# Usage:
#   python sec_archives_10k_extract_consolidated.py MSFT 5
#
# Output:
#   msft_10k_important_5y.csv
#
# Environment:
#   export SEC_USER_AGENT="Your Name your@email.com"

import os
import sys
import time
import re
import datetime as dt
from typing import Dict, List, Optional, Tuple, Iterable

import requests
import pandas as pd
import xml.etree.ElementTree as ET

DEFAULT_USER_AGENT = "Your Name your@email.com"
SEC_HEADERS = {
    "User-Agent": os.environ.get("SEC_USER_AGENT", DEFAULT_USER_AGENT),
    "Accept-Encoding": "gzip, deflate",
}

TICKER_CIK_URL = "https://www.sec.gov/files/company_tickers.json"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
ARCHIVES_INDEX_URL = "https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_no}/index.json"
ARCHIVES_FILE_URL = "https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_no}/{filename}"


def _sleep_briefly():
    time.sleep(0.2)


def get_json(url: str) -> dict:
    _sleep_briefly()
    r = requests.get(url, headers=SEC_HEADERS, timeout=30)
    if r.status_code == 429:
        time.sleep(0.6)
        r = requests.get(url, headers=SEC_HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


def get_bytes(url: str) -> bytes:
    _sleep_briefly()
    r = requests.get(url, headers=SEC_HEADERS, timeout=30)
    if r.status_code == 429:
        time.sleep(0.6)
        r = requests.get(url, headers=SEC_HEADERS, timeout=30)
    r.raise_for_status()
    return r.content


def ticker_to_cik10(ticker: str) -> str:
    data = get_json(TICKER_CIK_URL)
    t = ticker.upper().strip()
    for item in data.values():
        if item.get("ticker", "").upper() == t:
            return str(item["cik_str"]).zfill(10)
    raise ValueError(f"Ticker not found in SEC ticker list: {ticker}")


def list_10k_filings(cik10: str) -> List[dict]:
    sub = get_json(SUBMISSIONS_URL.format(cik=cik10))
    recent = sub.get("filings", {}).get("recent", {})

    forms = recent.get("form", [])
    accs = recent.get("accessionNumber", [])
    filing_dates = recent.get("filingDate", [])
    report_dates = recent.get("reportDate", [])
    primary_docs = recent.get("primaryDocument", [])

    out = []
    for i, form in enumerate(forms):
        if form == "10-K":
            out.append({
                "accessionNumber": accs[i],
                "filingDate": filing_dates[i] if i < len(filing_dates) else None,
                "reportDate": report_dates[i] if i < len(report_dates) else None,
                "primaryDocument": primary_docs[i] if i < len(primary_docs) else None,
            })

    out.sort(key=lambda x: (x.get("filingDate") or ""), reverse=True)
    return out


def _parse_date(s: Optional[str]) -> Optional[dt.date]:
    if not s:
        return None
    try:
        return dt.date.fromisoformat(s)
    except Exception:
        return None


def _localname(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag


def _is_dei_or_usgaap(tag: str) -> bool:
    return ("dei" in tag) or ("us-gaap" in tag)


def parse_context_info(root: ET.Element) -> Dict[str, Dict[str, object]]:
    """
    Map context id -> {
      start/end/instant: dates,
      has_segment: bool (dimensions/segment present),
      segment_text: str (flattened; for debugging)
    }
    We prefer facts in contexts WITHOUT segments because those are typically consolidated totals.
    """
    XBRLI = "http://www.xbrl.org/2003/instance"
    ctx_map: Dict[str, Dict[str, object]] = {}

    for ctx in root.findall(f".//{{{XBRLI}}}context"):
        ctx_id = ctx.attrib.get("id")
        if not ctx_id:
            continue

        period = ctx.find(f"{{{XBRLI}}}period")
        start = end = instant = None
        if period is not None:
            start_el = period.find(f"{{{XBRLI}}}startDate")
            end_el = period.find(f"{{{XBRLI}}}endDate")
            inst_el = period.find(f"{{{XBRLI}}}instant")

            start = _parse_date(start_el.text.strip()) if start_el is not None and start_el.text else None
            end = _parse_date(end_el.text.strip()) if end_el is not None and end_el.text else None
            instant = _parse_date(inst_el.text.strip()) if inst_el is not None and inst_el.text else None

        # segment/dimensions
        has_segment = False
        seg_text = ""
        seg_el = ctx.find(f".//{{{XBRLI}}}segment")
        if seg_el is not None:
            has_segment = True
            # Flatten segment for possible debugging; keep short
            seg_text = " ".join((seg_el.itertext() or [])).strip()

        ctx_map[ctx_id] = {
            "start": start,
            "end": end,
            "instant": instant,
            "has_segment": has_segment,
            "segment_text": seg_text,
        }

    return ctx_map


def collect_facts(root: ET.Element) -> Dict[str, List[dict]]:
    facts: Dict[str, List[dict]] = {}
    for el in root.iter():
        tag = el.tag
        if not isinstance(tag, str):
            continue
        if not _is_dei_or_usgaap(tag):
            continue

        name = _localname(tag)
        text = el.text.strip() if el.text else ""
        if not name or text == "":
            continue

        facts.setdefault(name, []).append({
            "value_raw": text,
            "contextRef": el.attrib.get("contextRef"),
            "unitRef": el.attrib.get("unitRef"),
        })
    return facts


def _to_number(s: Optional[str]) -> Optional[float]:
    if s is None:
        return None
    s = s.strip().replace(",", "")
    if re.fullmatch(r"-?\d+(\.\d+)?", s):
        try:
            return float(s)
        except Exception:
            return None
    return None


def pick_best_fact(
    facts: Dict[str, List[dict]],
    ctx_info: Dict[str, Dict[str, object]],
    concept: str,
    preferred_end: Optional[dt.date],
    kind: str,
) -> Tuple[Optional[float], Optional[str], int, bool]:
    """Returns (num, raw, score, used_segment_context)."""
    candidates = facts.get(concept, [])
    if not candidates:
        return None, None, -1, False

    scored = []
    for c in candidates:
        ctx_id = c.get("contextRef") or ""
        info = ctx_info.get(ctx_id, {})
        start = info.get("start")
        end = info.get("end")
        inst = info.get("instant")
        has_segment = bool(info.get("has_segment", False))

        if kind == "instant":
            date_key = inst
        elif kind == "duration":
            date_key = end
        else:
            date_key = end or inst

        score = 0

        # Prefer consolidated totals: penalize any segmented/dimensional context.
        if has_segment:
            score -= 200_000

        if preferred_end and date_key == preferred_end:
            score += 1_000_000
        if date_key:
            score += int(date_key.strftime("%Y%m%d"))

        if kind == "duration" and start and end:
            try:
                days = (end - start).days
                if 330 <= days <= 400:
                    score += 10_000
            except Exception:
                pass

        scored.append((score, c, has_segment))

    scored.sort(key=lambda x: x[0], reverse=True)
    best_score, best, used_segment = scored[0][0], scored[0][1], scored[0][2]
    raw = best.get("value_raw")
    num = _to_number(raw) if kind in ("duration", "instant") else None
    return num, raw, best_score, used_segment


def pick_best_from_concepts(
    facts: Dict[str, List[dict]],
    ctx_info: Dict[str, Dict[str, object]],
    concepts: Iterable[str],
    preferred_end: Optional[dt.date],
    kind: str,
) -> Tuple[Optional[float], Optional[str], Optional[str], bool]:
    best_num = None
    best_raw = None
    best_concept = None
    best_score = -10**18
    best_used_segment = False

    for concept in concepts:
        num, raw, score, used_segment = pick_best_fact(facts, ctx_info, concept, preferred_end, kind)
        if score > best_score:
            best_num, best_raw, best_concept, best_score, best_used_segment = num, raw, concept, score, used_segment

    return best_num, best_raw, best_concept, best_used_segment


def choose_xbrl_instance_filename(index_json: dict) -> Optional[str]:
    items = index_json.get("directory", {}).get("item", [])
    if not items:
        return None

    for it in items:
        name = it.get("name", "")
        if name.lower().endswith("_htm.xml"):
            return name

    bad_fragments = ("_cal", "_def", "_lab", "_pre")
    best = None
    best_size = -1
    for it in items:
        name = it.get("name", "")
        low = name.lower()
        if not low.endswith(".xml"):
            continue
        if low.endswith(".xsd"):
            continue
        if any(frag in low for frag in bad_fragments):
            continue
        try:
            size = int(it.get("size", 0))
        except Exception:
            size = 0
        if size > best_size:
            best_size = size
            best = name

    return best


# Concept fallbacks (ordered)
CONCEPTS = {
    "Revenue": {
        "kind": "duration",
        "concepts": [
            "Revenues",
            "SalesRevenueNet",
            "RevenueFromContractWithCustomerExcludingAssessedTax",
            "SalesRevenueGoodsNet",
            "RevenuesNetOfInterestExpense",
        ],
    },
    "Operating Income": {"kind": "duration", "concepts": ["OperatingIncomeLoss"]},
    "Net Income": {
        "kind": "duration",
        "concepts": ["NetIncomeLoss", "ProfitLoss", "NetIncomeLossAvailableToCommonStockholdersBasic"],
    },
    "Operating Cash Flow": {
        "kind": "duration",
        "concepts": [
            "NetCashProvidedByUsedInOperatingActivities",
            "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
        ],
    },
    "CapEx": {
        "kind": "duration",
        "concepts": [
            "PaymentsToAcquirePropertyPlantAndEquipment",
            "PaymentsToAcquireProductiveAssets",
            "PaymentsToAcquirePropertyPlantAndEquipmentGross",
            "CapitalExpenditures",
        ],
    },
    "Cash": {
        "kind": "instant",
        "concepts": [
            "CashAndCashEquivalentsAtCarryingValue",
            "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
            "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsIncludingDisposalGroupAndDiscontinuedOperations",
        ],
    },
    "Long-term Debt": {
        "kind": "instant",
        "concepts": [
            "LongTermDebt",
            "LongTermDebtNoncurrent",
            "LongTermDebtAndCapitalLeaseObligations",
        ],
    },
    "Operating Lease Liability Noncurrent": {
        "kind": "instant",
        "concepts": [
            "OperatingLeaseLiabilityNoncurrent",
            "OperatingLeaseLiability",
        ],
    },
    "Shares Outstanding": {
        "kind": "instant",
        "concepts": ["EntityCommonStockSharesOutstanding", "CommonStockSharesOutstanding"],
    },
    "Registrant Name": {"kind": "string", "concepts": ["EntityRegistrantName"]},
    "Fiscal Year Focus": {"kind": "string", "concepts": ["DocumentFiscalYearFocus"]},
    "Period End Date": {"kind": "string", "concepts": ["DocumentPeriodEndDate"]},
}


def extract_from_filing(cik10: str, filing: dict) -> dict:
    cik_int = str(int(cik10))
    acc = filing["accessionNumber"]
    acc_no = acc.replace("-", "")
    report_date = _parse_date(filing.get("reportDate"))
    filing_date = filing.get("filingDate")

    idx = get_json(ARCHIVES_INDEX_URL.format(cik_int=cik_int, acc_no=acc_no))
    inst_name = choose_xbrl_instance_filename(idx)
    if not inst_name:
        raise RuntimeError(f"Could not find XBRL instance XML for accession {acc}")

    xml_bytes = get_bytes(ARCHIVES_FILE_URL.format(cik_int=cik_int, acc_no=acc_no, filename=inst_name))
    root = ET.fromstring(xml_bytes)

    ctx_info = parse_context_info(root)
    facts = collect_facts(root)

    extracted: Dict[str, object] = {}
    chosen: Dict[str, Optional[str]] = {}
    used_segment: Dict[str, bool] = {}

    for label, cfg in CONCEPTS.items():
        kind = cfg["kind"]
        concepts = cfg["concepts"]
        num, raw, chosen_concept, seg = pick_best_from_concepts(
            facts=facts,
            ctx_info=ctx_info,
            concepts=concepts,
            preferred_end=report_date,
            kind=kind,
        )
        extracted[label] = num if kind in ("duration", "instant") else raw
        chosen[label] = chosen_concept
        used_segment[label] = bool(seg)

    fy = None
    fy_raw = extracted.get("Fiscal Year Focus")
    if isinstance(fy_raw, str) and re.fullmatch(r"\d{4}", fy_raw.strip()):
        fy = int(fy_raw.strip())
    elif report_date:
        fy = report_date.year

    ocf = extracted.get("Operating Cash Flow")
    capex = extracted.get("CapEx")
    fcf = (ocf - capex) if isinstance(ocf, (int, float)) and isinstance(capex, (int, float)) else None

    long_term_debt = extracted.get("Long-term Debt")
    operating_lease_liability = extracted.get("Operating Lease Liability Noncurrent")
    total_debt = None
    if isinstance(long_term_debt, (int, float)) and isinstance(operating_lease_liability, (int, float)):
        total_debt = long_term_debt + operating_lease_liability
    elif isinstance(long_term_debt, (int, float)):
        total_debt = long_term_debt
    elif isinstance(operating_lease_liability, (int, float)):
        total_debt = operating_lease_liability

    return {
        "CIK": cik10,
        "FY": fy,
        "Accession": acc,
        "Filing Date": filing_date,
        "Report Date": filing.get("reportDate"),
        "Registrant Name": extracted.get("Registrant Name"),
        "Period End Date (XBRL)": extracted.get("Period End Date"),
        "XBRL Instance": inst_name,

        "Revenue": extracted.get("Revenue"),
        "Operating Income": extracted.get("Operating Income"),
        "Net Income": extracted.get("Net Income"),
        "Operating Cash Flow": ocf,
        "CapEx": capex,
        "Free Cash Flow": fcf,
        "Cash": extracted.get("Cash"),
        "Total Debt": total_debt,
        "Total Debt(EV)": long_term_debt,
        "Operating Lease Liability Noncurrent": operating_lease_liability,
        "Shares Outstanding": extracted.get("Shares Outstanding"),
    }


def main():
    if len(sys.argv) != 3:
        print("Usage: python sec_archives_10k_extract_consolidated.py <TICKER> <YEARS>")
        sys.exit(1)

    ticker = sys.argv[1].upper().strip()
    years = int(sys.argv[2])

    cik10 = ticker_to_cik10(ticker)
    filings = list_10k_filings(cik10)[: max(years, 1)]
    rows = [extract_from_filing(cik10, f) for f in filings]

    df = pd.DataFrame(rows)
    df = df.sort_values(by=["FY", "Filing Date"], ascending=[False, False], na_position="last")

    # Format numeric columns to remove .0 (convert to int where possible)
    numeric_cols = df.select_dtypes(include=[float]).columns
    for col in numeric_cols:
        # Convert to int where values are whole numbers (handles NaN properly)
        df[col] = df[col].apply(lambda x: int(x) if pd.notna(x) and x == int(x) else x)

    out_name = f"{ticker.lower()}_10k_important_{years}y.csv"
    df.to_csv(out_name, index=False, float_format='%.0f')

    print(f"Saved: {out_name}")
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
