#!/usr/bin/env python3
# Enhanced SEC 10-K extractor (Submissions + Archives) with:
#   ✅ Excel output (multi-sheet)
#   ✅ Automatic DCF valuation output (simple, transparent)
#   ✅ Optional market cap + per-share metrics (SEC-only: user supplies price or market cap)
#   ✅ Batch mode: AAPL MSFT NVDA
#
# IMPORTANT:
# - Network calls are ONLY to SEC endpoints:
#   - https://www.sec.gov/files/company_tickers.json
#   - https://data.sec.gov/submissions/CIK##########.json
#   - https://www.sec.gov/Archives/edgar/data/...
#
# Usage examples:
#   python sec_10k_extraction_enhanced.py MSFT --years 5
#   python sec_10k_extraction_enhanced.py AAPL MSFT NVDA --years 5
#   python sec_10k_extraction_enhanced.py MSFT --years 5 --price 420
#   python sec_10k_extraction_enhanced.py MSFT --years 5 --market-cap 3.2e12
#
# Output:
#   sec_10k_output.xlsx  (default)
#
# Env:
#   export SEC_USER_AGENT="Your Name your@email.com"

import os
import re
import time
import datetime as dt
from typing import Dict, List, Optional, Tuple, Iterable, Any

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
        time.sleep(0.8)
        r = requests.get(url, headers=SEC_HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()

def get_bytes(url: str) -> bytes:
    _sleep_briefly()
    r = requests.get(url, headers=SEC_HEADERS, timeout=30)
    if r.status_code == 429:
        time.sleep(0.8)
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

def parse_context_info(root: ET.Element) -> Dict[str, Dict[str, Any]]:
    XBRLI = "http://www.xbrl.org/2003/instance"
    ctx_map: Dict[str, Dict[str, Any]] = {}

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

        has_segment = ctx.find(f".//{{{XBRLI}}}segment") is not None
        ctx_map[ctx_id] = {"start": start, "end": end, "instant": instant, "has_segment": has_segment}

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
        return float(s)
    return None

def pick_best_fact(
    facts: Dict[str, List[dict]],
    ctx_info: Dict[str, Dict[str, Any]],
    concept: str,
    preferred_end: Optional[dt.date],
    kind: str,
) -> Tuple[Optional[float], Optional[str], int]:
    candidates = facts.get(concept, [])
    if not candidates:
        return None, None, -10**18

    best_score = -10**18
    best_raw = None
    best_num = None

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
        if has_segment:
            score -= 200_000  # prefer consolidated totals

        if preferred_end and date_key == preferred_end:
            score += 1_000_000

        if date_key:
            score += int(date_key.strftime("%Y%m%d"))

        if kind == "duration" and start and end:
            days = (end - start).days
            if 330 <= days <= 400:
                score += 10_000

        if score > best_score:
            best_score = score
            best_raw = c.get("value_raw")
            best_num = _to_number(best_raw) if kind in ("duration", "instant") else None

    return best_num, best_raw, best_score

def pick_best_from_concepts(
    facts: Dict[str, List[dict]],
    ctx_info: Dict[str, Dict[str, Any]],
    concepts: Iterable[str],
    preferred_end: Optional[dt.date],
    kind: str,
) -> Tuple[Optional[float], Optional[str], Optional[str]]:
    best_num, best_raw, best_concept = None, None, None
    best_score = -10**18
    for concept in concepts:
        num, raw, score = pick_best_fact(facts, ctx_info, concept, preferred_end, kind)
        if score > best_score:
            best_num, best_raw, best_concept, best_score = num, raw, concept, score
    return best_num, best_raw, best_concept

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
        size = int(it.get("size", 0) or 0)
        if size > best_size:
            best_size = size
            best = name
    return best

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
    "Total Debt": {
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

    for label, cfg in CONCEPTS.items():
        kind = cfg["kind"]
        concepts = cfg["concepts"]
        num, raw, chosen_concept = pick_best_from_concepts(facts, ctx_info, concepts, report_date, kind)
        extracted[label] = num if kind in ("duration", "instant") else raw
        chosen[label] = chosen_concept

    fy = None
    fy_raw = extracted.get("Fiscal Year Focus")
    if isinstance(fy_raw, str) and re.fullmatch(r"\d{4}", fy_raw.strip()):
        fy = int(fy_raw.strip())
    elif report_date:
        fy = report_date.year

    ocf = extracted.get("Operating Cash Flow")
    capex = extracted.get("CapEx")
    fcf = (ocf - capex) if isinstance(ocf, (int, float)) and isinstance(capex, (int, float)) else None

    long_term_debt = extracted.get("Total Debt")
    op_lease = extracted.get("Operating Lease Liability Noncurrent")

    total_debt_incl_leases = None
    if isinstance(long_term_debt, (int, float)) and isinstance(op_lease, (int, float)):
        total_debt_incl_leases = long_term_debt + op_lease
    elif isinstance(long_term_debt, (int, float)):
        total_debt_incl_leases = long_term_debt
    elif isinstance(op_lease, (int, float)):
        total_debt_incl_leases = op_lease

    return {
        "CIK": cik10,
        "FY": fy,
        "Accession": acc,
        "Filing Date": filing.get("filingDate"),
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
        "Total Debt": long_term_debt,
        "Operating Lease Liability Noncurrent": op_lease,
        "Total Debt (incl op leases)": total_debt_incl_leases,
        "Shares Outstanding": extracted.get("Shares Outstanding"),
    }

def _cagr(old_to_new: List[float]) -> Optional[float]:
    if len(old_to_new) < 2:
        return None
    first, last = old_to_new[0], old_to_new[-1]
    if first is None or last is None or first <= 0 or last <= 0:
        return None
    n = len(old_to_new) - 1
    return (last / first) ** (1 / n) - 1

def run_simple_dcf(fin_df: pd.DataFrame, wacc: float, terminal_growth: float, projection_years: int, debt_for_ev: str) -> Dict[str, Any]:
    if fin_df.empty:
        return {"error": "No financial data"}
    df = fin_df.sort_values(by=["FY"], ascending=False).copy()

    fcf_series = df["Free Cash Flow"].dropna()
    if fcf_series.empty:
        return {"error": "No FCF available (Free Cash Flow is missing)"}
    base_fcf = float(fcf_series.iloc[0])

    hist_new_to_old = [float(x) for x in df["Free Cash Flow"].dropna().tolist()]
    hist_old_to_new = list(reversed(hist_new_to_old))
    g = _cagr(hist_old_to_new)
    if g is None:
        g = 0.05
    g = max(min(g, 0.30), -0.20)

    if wacc <= terminal_growth:
        return {"error": "WACC must be greater than terminal growth"}

    projected_rows = []
    pv_sum = 0.0
    for y in range(1, projection_years + 1):
        fcf_y = base_fcf * ((1.0 + g) ** y)
        pv_y = fcf_y / ((1.0 + wacc) ** y)
        pv_sum += pv_y
        projected_rows.append({"Year": y, "FCF": fcf_y, "PV": pv_y})

    terminal_fcf = base_fcf * ((1.0 + g) ** projection_years) * (1.0 + terminal_growth)
    terminal_value = terminal_fcf / (wacc - terminal_growth)
    pv_terminal = terminal_value / ((1.0 + wacc) ** projection_years)

    ev = pv_sum + pv_terminal

    cash_s = df["Cash"].dropna()
    cash = float(cash_s.iloc[0]) if not cash_s.empty else 0.0

    debt = 0.0
    if debt_for_ev in df.columns:
        debt_s = df[debt_for_ev].dropna()
        if not debt_s.empty:
            debt = float(debt_s.iloc[0])

    net_debt = debt - cash
    equity_value = ev - net_debt

    shares = None
    sh = df["Shares Outstanding"].dropna()
    if not sh.empty:
        shares = float(sh.iloc[0])

    implied_ps = (equity_value / shares) if shares and shares > 0 else None

    return {
        "base_fcf": base_fcf,
        "fcf_growth": g,
        "wacc": wacc,
        "terminal_growth": terminal_growth,
        "projection_years": projection_years,
        "pv_fcf": pv_sum,
        "pv_terminal": pv_terminal,
        "enterprise_value": ev,
        "cash": cash,
        "debt_basis": debt_for_ev,
        "debt": debt,
        "net_debt": net_debt,
        "equity_value": equity_value,
        "shares": shares,
        "implied_value_per_share": implied_ps,
        "projected_table": projected_rows,
    }

def compute_market_cap_metrics(shares_outstanding: Optional[float], price: Optional[float], market_cap: Optional[float]) -> Dict[str, Optional[float]]:
    out = {"price": None, "market_cap": None}
    if shares_outstanding is None or shares_outstanding <= 0:
        return out
    if price is not None:
        out["price"] = float(price)
        out["market_cap"] = float(price) * float(shares_outstanding)
        return out
    if market_cap is not None:
        out["market_cap"] = float(market_cap)
        out["price"] = float(market_cap) / float(shares_outstanding)
        return out
    return out

def _sanitize_sheet(name: str) -> str:
    name = re.sub(r"[\[\]\:\*\?\/\\]", "_", name)
    return name[:31]

def extract_company_years(ticker: str, years: int) -> pd.DataFrame:
    cik10 = ticker_to_cik10(ticker)
    filings = list_10k_filings(cik10)[: max(years, 1)]
    rows = [extract_from_filing(cik10, f) for f in filings]
    df = pd.DataFrame(rows)
    df = df.sort_values(by=["FY", "Filing Date"], ascending=[False, False], na_position="last")

    numeric_cols = df.select_dtypes(include=[float, int]).columns
    for col in numeric_cols:
        df[col] = df[col].apply(lambda x: int(x) if pd.notna(x) and isinstance(x, (int, float)) and float(x).is_integer() else x)

    return df

def main():
    import argparse

    parser = argparse.ArgumentParser(description="SEC 10-K extractor + DCF (SEC-only endpoints)")
    parser.add_argument("tickers", nargs="+", help="One or more tickers, e.g. AAPL MSFT NVDA")
    parser.add_argument("--years", type=int, default=5, help="Number of 10-K years to extract (default 5)")
    parser.add_argument("--output", type=str, default="sec_10k_output.xlsx", help="Excel output filename")
    parser.add_argument("--wacc", type=float, default=0.10, help="WACC for DCF (default 0.10)")
    parser.add_argument("--terminal-growth", type=float, default=0.03, help="Terminal growth rate (default 0.03)")
    parser.add_argument("--projection-years", type=int, default=5, help="DCF projection years (default 5)")
    parser.add_argument("--price", type=float, default=None, help="Optional price to compute market cap (no web calls)")
    parser.add_argument("--market-cap", type=float, default=None, help="Optional market cap to compute implied price (no web calls)")
    parser.add_argument("--debt-for-ev", type=str, default="Total Debt",
                        choices=["Total Debt", "Total Debt (incl op leases)"],
                        help="Debt measure used in DCF net debt (default: Total Debt)")

    args = parser.parse_args()

    seen = set()
    tickers = []
    for t in args.tickers:
        tt = t.upper().strip()
        if tt and tt not in seen:
            seen.add(tt)
            tickers.append(tt)

    summary_rows = []

    with pd.ExcelWriter(args.output, engine="openpyxl") as writer:
        for ticker in tickers:
            fin_df = extract_company_years(ticker, args.years)

            dcf = run_simple_dcf(fin_df, float(args.wacc), float(args.terminal_growth), int(args.projection_years), str(args.debt_for_ev))
            proj_df = pd.DataFrame(dcf.get("projected_table", [])) if isinstance(dcf, dict) else pd.DataFrame()

            shares_latest = None
            if not fin_df.empty:
                sh = fin_df["Shares Outstanding"].dropna()
                shares_latest = float(sh.iloc[0]) if not sh.empty else None

            mkt = compute_market_cap_metrics(shares_latest, args.price, args.market_cap)

            latest = fin_df.iloc[0].to_dict() if not fin_df.empty else {}
            summary_rows.append({
                "Ticker": ticker,
                "Latest FY": latest.get("FY"),
                "Shares (latest)": shares_latest,
                "Price (input/derived)": mkt.get("price"),
                "Market Cap (input/derived)": mkt.get("market_cap"),
                "Latest FCF": latest.get("Free Cash Flow"),
                "Cash (latest)": latest.get("Cash"),
                f"{args.debt_for_ev} (latest)": latest.get(args.debt_for_ev),
                "DCF EV": dcf.get("enterprise_value") if isinstance(dcf, dict) else None,
                "DCF Equity Value": dcf.get("equity_value") if isinstance(dcf, dict) else None,
                "DCF $/Share": dcf.get("implied_value_per_share") if isinstance(dcf, dict) else None,
                "DCF FCF growth": dcf.get("fcf_growth") if isinstance(dcf, dict) else None,
                "DCF Error": dcf.get("error") if isinstance(dcf, dict) else None,
            })

            fin_df.to_excel(writer, sheet_name=_sanitize_sheet(f"{ticker}_Financials"), index=False)

            dcf_items = {k: v for k, v in dcf.items() if k != "projected_table"} if isinstance(dcf, dict) else {"error": "DCF failed"}
            pd.DataFrame([dcf_items]).to_excel(writer, sheet_name=_sanitize_sheet(f"{ticker}_DCF"), index=False)

            if not proj_df.empty:
                proj_df.to_excel(writer, sheet_name=_sanitize_sheet(f"{ticker}_DCF_Proj"), index=False)

        pd.DataFrame(summary_rows).to_excel(writer, sheet_name="Summary", index=False)

    print(f"Saved Excel: {args.output}")

if __name__ == "__main__":
    main()
