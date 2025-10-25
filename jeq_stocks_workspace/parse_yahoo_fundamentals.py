# parse_yahoo_fundamentals.py
# -*- coding: utf-8 -*-
import re
import sys
import time
import json
import datetime as dt
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

import requests
from bs4 import BeautifulSoup

from sqlalchemy import (
    create_engine, Column, Integer, Float, String, Date, DateTime, BigInteger, UniqueConstraint
)
from sqlalchemy.orm import declarative_base, sessionmaker

# -------------------- DB model --------------------
Base = declarative_base()

class Fundamentals(Base):
    __tablename__ = "yahoo_fundamentals"

    id = Column(Integer, primary_key=True)
    ticker = Column(String(32), index=True, nullable=False)

    # numbers (normalized)
    market_cap_mil_jpy = Column(BigInteger)     # 百万円
    shares_outstanding = Column(BigInteger)     # 株
    dividend_yield_pct = Column(Float)          # %
    dividend_per_share_yen = Column(Float)      # 円
    per = Column(Float)                         # 倍
    pbr = Column(Float)                         # 倍
    eps_yen = Column(Float)                     # 円
    bps_yen = Column(Float)                     # 円
    roe_pct = Column(Float)                     # %
    equity_ratio_pct = Column(Float)            # %
    min_lot_price_yen = Column(Integer)         # 円
    trading_unit_shares = Column(Integer)       # 株
    ytd_high_yen = Column(Integer)
    ytd_high_date = Column(Date)
    ytd_low_yen = Column(Integer)
    ytd_low_date = Column(Date)

    # raw JSON dump for debugging
    raw_json = Column(String)

    # page “as of” dates (some fields carry their own dates)
    asof_date = Column(Date)                    # best-effort (e.g., 10/24)
    scraped_at = Column(DateTime, default=dt.datetime.utcnow, index=True)

    __table_args__ = (
        UniqueConstraint("ticker", "asof_date", name="uq_ticker_asof"),
    )

# -------------------- utilities --------------------
JP_MONTH_DAY = re.compile(r"\((\d{1,2})/(\d{1,2})\)")         # (10/24)
JP_YMD = re.compile(r"\((\d{4})/(\d{2})\)")                   # (2026/03)
JP_YTD = re.compile(r"\((\d{2})/(\d{2})/(\d{2})\)")           # (25/10/22) -> 20YY

def _clean_num(s: str) -> str:
    return s.replace(",", "").strip()

def _to_float(s: Optional[str]) -> Optional[float]:
    if not s: return None
    try:
        return float(_clean_num(s))
    except ValueError:
        return None

def _to_int(s: Optional[str]) -> Optional[int]:
    f = _to_float(s)
    return int(round(f)) if f is not None else None

def _parse_date_token(token: str) -> Optional[dt.date]:
    token = token.strip()
    # (10/24)
    m = JP_MONTH_DAY.search(token)
    if m:
        month, day = map(int, m.groups())
        year = dt.date.today().year
        return dt.date(year, month, day)
    # (2026/03) -> use month only, set day=1
    m = JP_YMD.search(token)
    if m:
        year, month = map(int, m.groups())
        return dt.date(year, month, 1)
    # (25/10/22) -> assume 20yy
    m = JP_YTD.search(token)
    if m:
        yy, mm, dd = map(int, m.groups())
        year = 2000 + yy
        return dt.date(year, mm, dd)
    return None

@dataclass
class ParsedItem:
    label: str
    value: str
    unit: Optional[str]
    date_token: Optional[str]

def parse_items(ul) -> Dict[str, ParsedItem]:
    out: Dict[str, ParsedItem] = {}
    for li in ul.select("li"):
        term = li.select_one("dt .DataListItem__name__3RQJ")
        if not term:
            continue
        label = term.get_text(strip=True)

        # dd contains the number & unit
        dd = li.select_one("dd")
        number_el = dd.select_one(".StyledNumber__value__3rXW")
        unit_el = dd.select_one(".StyledNumber__suffix__2SD5")
        date_el = dd.select_one(".DataListItem__date___6wH")

        value = number_el.get_text(strip=True) if number_el else ""
        unit = unit_el.get_text(strip=True) if unit_el else None
        date_token = date_el.get_text(strip=True) if date_el else None

        out[label] = ParsedItem(label, value, unit, date_token)
    return out

# -------------------- page fetch --------------------
def fetch_quote_html(ticker: str) -> str:
    # ticker format examples: "7203.T" (Toyota), "9984.T" (SoftBank G)
    url = f"https://finance.yahoo.co.jp/quote/{ticker}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    }
    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    return r.text

# -------------------- main parser for the block --------------------
def extract_fundamentals_from_html(html: str) -> Tuple[Dict[str, ParsedItem], Optional[dt.date]]:
    soup = BeautifulSoup(html, "lxml")
    # Target the exact UL class shown in your inspector
    ul = soup.select_one('ul.StocksReferenceIndex__items__3U1X')
    if not ul:
        raise RuntimeError("Could not locate fundamentals block <ul class='StocksReferenceIndex__items__3U1X'>")

    items = parse_items(ul)

    # Try to infer a general "as of" date from any item that has a (MM/DD)
    asof_date = None
    for p in items.values():
        if p.date_token:
            asof_date = _parse_date_token(p.date_token)
            if asof_date:
                break
    return items, asof_date

def normalize(items: Dict[str, ParsedItem]) -> Dict[str, Optional[float]]:
    """
    Map Japanese labels -> normalized numeric fields.
    """
    L = items
    get = lambda key: L.get(key)

    data = {
        # volumes/amounts
        "market_cap_mil_jpy": _to_int(get("時価総額").value) if get("時価総額") else None,
        "shares_outstanding": _to_int(get("発行済株式数").value) if get("発行済株式数") else None,

        # yields & multiples
        "dividend_yield_pct": _to_float(get("配当利回り").value) if get("配当利回り") else None,
        "dividend_per_share_yen": _to_float(get("1株配当").value) if get("1株配当") else None,
        "per": _to_float(get("PER").value) if get("PER") else None,
        "pbr": _to_float(get("PBR").value) if get("PBR") else None,

        # per-share metrics
        "eps_yen": _to_float(get("EPS").value) if get("EPS") else None,
        "bps_yen": _to_float(get("BPS").value) if get("BPS") else None,

        # profitability / balance-sheet
        "roe_pct": _to_float(get("ROE").value) if get("ROE") else None,
        "equity_ratio_pct": _to_float(get("自己資本比率").value) if get("自己資本比率") else None,

        # trading info
        "min_lot_price_yen": _to_int(get("最低購入代金").value) if get("最低購入代金") else None,
        "trading_unit_shares": _to_int(get("単元株数").value) if get("単元株数") else None,

        # ytd high/low (with their own dates)
        "ytd_high_yen": _to_int(get("年初来高値").value) if get("年初来高値") else None,
        "ytd_low_yen": _to_int(get("年初来安値").value) if get("年初来安値") else None,
    }

    # Parse YTD dates if present
    high = get("年初来高値")
    low = get("年初来安値")
    data["ytd_high_date"] = _parse_date_token(high.date_token) if high and high.date_token else None
    data["ytd_low_date"] = _parse_date_token(low.date_token) if low and low.date_token else None

    return data

# -------------------- persistence --------------------
def save_to_db(db_url: str, ticker: str, payload: Dict, asof_date: Optional[dt.date], raw_blob: Dict):
    engine = create_engine(db_url, future=True)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, future=True)
    with Session() as s:
        rec = Fundamentals(
            ticker=ticker,
            asof_date=asof_date,
            raw_json=json.dumps(raw_blob, ensure_ascii=False),

            market_cap_mil_jpy=payload["market_cap_mil_jpy"],
            shares_outstanding=payload["shares_outstanding"],
            dividend_yield_pct=payload["dividend_yield_pct"],
            dividend_per_share_yen=payload["dividend_per_share_yen"],
            per=payload["per"],
            pbr=payload["pbr"],
            eps_yen=payload["eps_yen"],
            bps_yen=payload["bps_yen"],
            roe_pct=payload["roe_pct"],
            equity_ratio_pct=payload["equity_ratio_pct"],
            min_lot_price_yen=payload["min_lot_price_yen"],
            trading_unit_shares=payload["trading_unit_shares"],
            ytd_high_yen=payload["ytd_high_yen"],
            ytd_high_date=payload["ytd_high_date"],
            ytd_low_yen=payload["ytd_low_yen"],
            ytd_low_date=payload["ytd_low_date"],
        )
        # Upsert-on-unique (ticker, asof_date)
        # Try delete existing, then insert (simple & portable)
        if asof_date:
            s.query(Fundamentals).filter(
                Fundamentals.ticker == ticker,
                Fundamentals.asof_date == asof_date
            ).delete()
        s.add(rec)
        s.commit()

# -------------------- CLI --------------------
def run_from_live(ticker: str, db_url: str):
    html = fetch_quote_html(ticker)
    items, asof_date = extract_fundamentals_from_html(html)
    payload = normalize(items)
    save_to_db(db_url, ticker, payload, asof_date, raw_blob={k: vars(v) for k, v in items.items()})
    print(f"Saved fundamentals for {ticker} (as of {asof_date})")

def run_from_snippet(ticker: str, db_url: str, snippet_path: str):
    with open(snippet_path, "r", encoding="utf-8") as f:
        html = f.read()
    items, asof_date = extract_fundamentals_from_html(html)
    payload = normalize(items)
    save_to_db(db_url, ticker, payload, asof_date, raw_blob={k: vars(v) for k, v in items.items()})
    print(f"Saved fundamentals for {ticker} (as of {asof_date}) from snippet")

if __name__ == "__main__":
    """
    Usage:
      python parse_yahoo_fundamentals.py 7203.T sqlite:///funds.db
      python parse_yahoo_fundamentals.py 7203.T sqlite:///funds.db --snippet ul_block.html
    """
    if len(sys.argv) < 3:
        print("Usage: python parse_yahoo_fundamentals.py <TICKER> <DB_URL> [--snippet path]")
        sys.exit(1)

    ticker = sys.argv[1]
    db_url = sys.argv[2]
    if len(sys.argv) >= 5 and sys.argv[3] == "--snippet":
        run_from_snippet(ticker, db_url, sys.argv[4])
    else:
        run_from_live(ticker, db_url)