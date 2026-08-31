#!/usr/bin/env python3
"""
Final Soroush AI Code 9

Binance USDT perpetual scanner focused on institutional order flow.
Active Scanners: F4 (CVD surge/acceleration), INFLOW24 (CoinGlass-style Netflow)

Reporting:
  - Outputs one JSON report plus per-scanner CSVs.
  - Generates an overlap report for symbols hitting multiple scanners simultaneously.
"""

from __future__ import annotations

import json
import logging
import math
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

# ----------------------
# Logging
# ----------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("openai-full-scanner-final-soroush-9")

# ----------------------
# Endpoints
# ----------------------
FAPI_BASE = os.environ.get("BINANCE_FAPI", "https://fapi.binance.com")
FAPI_EXCHANGE_INFO = FAPI_BASE + "/fapi/v1/exchangeInfo"
FAPI_TICKER_24H = FAPI_BASE + "/fapi/v1/ticker/24hr"
FAPI_KLINES = FAPI_BASE + "/fapi/v1/klines"

# ----------------------
# Runtime config
# ----------------------
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "10"))
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "10"))
RATE_LIMIT_SLEEP = float(os.environ.get("RATE_LIMIT_SLEEP", "1.5"))
RETRY_MAX = int(os.environ.get("RETRY_MAX", "3"))

BINANCE_TOP_CANDIDATES = int(os.environ.get("BINANCE_TOP_CANDIDATES", "400"))
MIN_24H_QUOTE_VOLUME = float(os.environ.get("MIN_24H_QUOTE_VOLUME", "5000000"))

# ----------------------
# Scanner configs
# ----------------------
# Scanner F4 (4H) - CVD Surge / Acceleration
F4_CVD_INCREASE_PCT = 1.10
F4_CVD_INCREASE_USD = 20_000_000.0

# Scanner INFLOW24 (1H) - 24h Net Taker Inflow (CoinGlass-style Netflow)
INFLOW24_LOOKBACK_HOURS = float(os.environ.get("INFLOW24_LOOKBACK_HOURS", "24"))
INFLOW24_MIN_USD = float(os.environ.get("INFLOW24_MIN_USD", "5000000"))

REPORT_JSON = os.environ.get("REPORT_JSON", "scanner_report_v11.json")
OVERLAP_OUTPUT_CSV = os.environ.get("OVERLAP_OUTPUT_CSV", "scanner_overlaps_v11.csv")

# ----------------------
# Session Configuration
# ----------------------
_session = requests.Session()
_session.headers.update({"User-Agent": "openai-full-scanner-final/11.0"})

# ----------------------
# Generic Helpers
# ----------------------
def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)

def safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v) if v is not None else default
    except Exception:
        return default

def safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v) if v is not None else default
    except Exception:
        return default

def iso_from_ms(ms: Optional[int]) -> str:
    if not ms: return ""
    try:
        return datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return str(ms)

def pct_fmt(v: Optional[float], digits: int = 2) -> str:
    if v is None: return "N/A"
    return f"{v:.{digits}f}%"

def usd_fmt(v: Optional[float], digits: int = 0) -> str:
    if v is None: return "N/A"
    return f"${v:,.{digits}f}" if digits > 0 else f"${v:,.0f}"

def safe_get(url: str, params: Optional[dict] = None, timeout: float = REQUEST_TIMEOUT, max_retries: int = RETRY_MAX) -> Optional[Any]:
    backoff = RATE_LIMIT_SLEEP
    for attempt in range(1, max_retries + 1):
        try:
            r = _session.get(url, params=params or {}, timeout=timeout)
            if r.status_code == 429:
                sleep_for = backoff + random.random() * 0.5 * backoff
                time.sleep(sleep_for)
                backoff = min(backoff * 2, 30.0)
                continue
            r.raise_for_status()
            return r.json()
        except requests.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            if status and 500 <= status < 600 and attempt < max_retries:
                time.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
                continue
            return None
        except requests.RequestException:
            if attempt < max_retries:
                time.sleep(backoff + random.random() * 0.2 * backoff)
                backoff = min(backoff * 2, 30.0)
                continue
            return None
    return None

def window_rows(rows: List[Dict[str, Any]], lookback_hours: float) -> List[Dict[str, Any]]:
    cutoff = now_ms() - int(lookback_hours * 3600 * 1000)
    return [r for r in rows if cutoff <= int(r.get("close_time") or 0) <= now_ms()]

def delta_for_row(r: Dict[str, Any]) -> float:
    qv = safe_float(r.get("quote_volume"))
    tbq = r.get("taker_buy_quote")
    if tbq is not None:
        buy = max(0.0, safe_float(tbq))
        sell = max(0.0, qv - buy)
        return buy - sell
    o = safe_float(r.get("open"))
    c = safe_float(r.get("close"))
    if c > o: return qv
    if c < o: return -qv
    return 0.0

def candles_needed_for_lookback(lookback_hours: float, interval_minutes: int, extra: int = 2) -> int:
    return max(3, int(math.ceil((lookback_hours * 60.0) / interval_minutes)) + extra)

# ----------------------
# Bulk Fetchers
# ----------------------
def fetch_candidates(limit: int = BINANCE_TOP_CANDIDATES) -> List[str]:
    info = safe_get(FAPI_EXCHANGE_INFO)
    tickers = safe_get(FAPI_TICKER_24H)
    if not isinstance(info, dict) or not isinstance(tickers, list):
        logger.error("Failed to fetch exchangeInfo or ticker/24h.")
        return []

    trading = {
        s.get("symbol")
        for s in info.get("symbols", [])
        if (s.get("quoteAsset") or "").upper() == "USDT" and s.get("status") == "TRADING"
    }

    vol_rows: List[Tuple[str, float]] = []
    for t in tickers:
        sym = t.get("symbol")
        if sym not in trading: continue
        qv = safe_float(t.get("quoteVolume") or t.get("quoteAssetVolume"))
        if qv >= MIN_24H_QUOTE_VOLUME:
            vol_rows.append((sym, qv))

    vol_rows.sort(key=lambda kv: kv[1], reverse=True)
    selected = [sym for sym, _ in vol_rows[:limit]]
    logger.info("Selected %d candidate symbols.", len(selected))
    return selected

def fetch_klines(symbol: str, interval: str = "1h", limit: int = 100) -> Optional[List[Dict[str, Any]]]:
    raw = safe_get(FAPI_KLINES, params={"symbol": symbol, "interval": interval, "limit": int(limit)})
    if not isinstance(raw, list) or not raw: return None

    rows: List[Dict[str, Any]] = []
    for k in raw:
        if not isinstance(k, list) or len(k) < 8: continue
        try:
            open_time, close_time = safe_int(k[0]), safe_int(k[6])
            open_p, high_p, low_p, close_p = safe_float(k[1]), safe_float(k[2]), safe_float(k[3]), safe_float(k[4])
            base_vol, quote_vol = safe_float(k[5]), safe_float(k[7])
            if not quote_vol and base_vol and close_p:
                quote_vol = base_vol * close_p
            taker_buy_quote = float(k[10]) if len(k) > 10 and k[10] is not None else None

            rows.append({
                "open_time": open_time, "close_time": close_time,
                "open": open_p, "high": high_p, "low": low_p, "close": close_p,
                "base_volume": base_vol, "quote_volume": quote_vol,
                "taker_buy_quote": taker_buy_quote,
            })
        except Exception:
            continue
    rows.sort(key=lambda r: r["open_time"])
    return rows or None

# ----------------------
# Analyzers
# ----------------------
def analyze_F4(rows_4h: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if len(rows_4h) < 2: return None
    cvd = 0.0
    cvd_list = []
    for r in rows_4h:
        cvd += delta_for_row(r)
        cvd_list.append(cvd)
    recent_idx = list(range(max(1, len(rows_4h)-6), len(rows_4h)))
    best = None
    for i in recent_idx:
        prev_cvd, curr_cvd = cvd_list[i-1], cvd_list[i]
        delta_usd = curr_cvd - prev_cvd
        cond_pct = (prev_cvd > 0 and curr_cvd >= prev_cvd * F4_CVD_INCREASE_PCT)
        cond_usd = (delta_usd >= F4_CVD_INCREASE_USD)
        if cond_pct or cond_usd:
            cand = {
                "match_time_ms": int(rows_4h[i]["close_time"]),
                "prev_cvd": prev_cvd,
                "curr_cvd": curr_cvd,
                "delta_usd": delta_usd,
                "increase_pct": ((curr_cvd - prev_cvd) / prev_cvd * 100.0) if prev_cvd > 0 else 0.0,
                "source": "F4_cvd_surge"
            }
            if not best or cand["match_time_ms"] > best["match_time_ms"]: best = cand
    return best

def analyze_INFLOW24(rows_1h: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    INFLOW24 - 24h Net Taker Inflow Scanner (CoinGlass-style Netflow proxy).
    Netflow = taker buy volume - taker sell volume, summed over the trailing
    24h window on Binance USDT-perp klines. Mirrors CoinGlass's own definition
    of futures net inflow/outflow (active buy volume minus active sell volume).
    Flags symbols with >= INFLOW24_MIN_USD net inflow over 24h.
    """
    filtered = window_rows(rows_1h, INFLOW24_LOOKBACK_HOURS)
    if not filtered:
        return None

    net_flow = 0.0
    buy_total = 0.0
    sell_total = 0.0
    for r in filtered:
        qv = safe_float(r.get("quote_volume"))
        tbq = r.get("taker_buy_quote")
        if tbq is not None:
            buy = max(0.0, safe_float(tbq))
            sell = max(0.0, qv - buy)
        else:
            d = delta_for_row(r)
            buy = qv if d >= 0 else 0.0
            sell = qv if d < 0 else 0.0
        buy_total += buy
        sell_total += sell
        net_flow += (buy - sell)

    if net_flow >= INFLOW24_MIN_USD:
        return {
            "net_flow_usd_24h": float(net_flow),
            "taker_buy_volume_usd_24h": float(buy_total),
            "taker_sell_volume_usd_24h": float(sell_total),
            "match_time_ms": int(filtered[-1]["close_time"]),
            "source": "INFLOW24_net_taker_inflow",
        }
    return None

# ----------------------
# Reporting Helpers
# ----------------------
SCANNER_ORDER = ["F4", "INFLOW24"]

SCANNER_DESCRIPTIONS = {
    "F4": "CVD surge/acceleration: up >= 10% or >= $20M within 24h (4H)",
    "INFLOW24": "24h Net Taker Inflow (CoinGlass-style Netflow) >= $5M (1H)"
}

SORT_HINTS: Dict[str, List[str]] = {
    "F4": ["delta_usd", "increase_pct"],
    "INFLOW24": ["net_flow_usd_24h"]
}

def save_matches(matches_list: List[Dict[str, Any]], out_csv: str, sort_by: Optional[List[str]] = None) -> pd.DataFrame:
    df = pd.DataFrame(matches_list)
    if df.empty: return df
    if "match_time_ms" in df.columns:
        df["match_time_iso"] = df["match_time_ms"].apply(iso_from_ms)
    if sort_by:
        cols = [c for c in sort_by if c in df.columns]
        if cols: df = df.sort_values(by=cols, ascending=False).reset_index(drop=True)
    df.to_csv(out_csv, index=False)
    return df

def pick_cols(df: pd.DataFrame, cols: List[str]) -> List[str]:
    return [c for c in cols if c in df.columns]

def print_section_header(title: str, subtitle: str = "") -> None:
    print("\n" + "=" * 92)
    print(title)
    if subtitle: print(subtitle)
    print("=" * 92)

def print_matches_table(label: str, df: pd.DataFrame, limit: int = 20) -> None:
    if df.empty:
        print("No matches.")
        return

    cols_map = {
        "F4": ["symbol", "prev_cvd", "curr_cvd", "delta_usd", "increase_pct", "match_time_iso"],
        "INFLOW24": ["symbol", "net_flow_usd_24h", "taker_buy_volume_usd_24h", "taker_sell_volume_usd_24h", "match_time_iso"]
    }

    cols = pick_cols(df, cols_map.get(label, list(df.columns)))
    display_df = df[cols].head(limit).copy()

    for col in display_df.columns:
        if col.endswith("_pct") or col.endswith("_pct_change"):
            display_df[col] = display_df[col].apply(lambda x: pct_fmt(x) if pd.notnull(x) else "N/A")
        elif "usd" in col or col in {"prev_cvd", "curr_cvd"}:
            display_df[col] = display_df[col].apply(lambda x: usd_fmt(x) if pd.notnull(x) else "N/A")

    print(display_df.to_string(index=False))

def save_overlap_report(symbol_map: Dict[str, List[Tuple[str, Dict[str, Any]]]]) -> pd.DataFrame:
    overlaps = {s: lst for s, lst in symbol_map.items() if len(lst) >= 2}
    rows: List[Dict[str, Any]] = []

    for sym, lst in overlaps.items():
        row: Dict[str, Any] = {"symbol": sym, "analyzers": ",".join(lab for lab, _ in lst)}
        latest = 0
        for lab, m in lst:
            row[f"{lab}_match_time_ms"] = m.get("match_time_ms")
            latest = max(latest, safe_int(m.get("match_time_ms")))

            if lab == "F4": row["F4_delta_usd"] = m.get("delta_usd")
            elif lab == "INFLOW24":
                row["INFLOW24_net_flow_usd_24h"] = m.get("net_flow_usd_24h")

        row["match_time_iso"] = iso_from_ms(latest)
        row["analyzer_count"] = len(lst)
        rows.append(row)

    df = pd.DataFrame(rows)
    if not df.empty:
        cols = ["symbol", "analyzers", "analyzer_count", "match_time_iso"]
        cols += [c for c in df.columns if c not in cols]
        df = df[cols].sort_values(by=["analyzer_count", "symbol"], ascending=[False, True]).reset_index(drop=True)
        df.to_csv(OVERLAP_OUTPUT_CSV, index=False)
    return df

# ----------------------
# Main Orchestration
# ----------------------
def main() -> None:
    print_section_header("OpenAI Full Institutional Order Flow Scanner (Final Soroush AI Code 9)", f"Scanners active: {', '.join(SCANNER_ORDER)}")

    candidates = fetch_candidates(limit=BINANCE_TOP_CANDIDATES)
    if not candidates:
        logger.error("No candidates found.")
        return

    scanner_matches: Dict[str, List[Dict[str, Any]]] = {k: [] for k in SCANNER_ORDER}

    def process_symbol(sym: str):
        # Fetching required timeframes
        rows_1h = fetch_klines(sym, interval="1h", limit=max(100, candles_needed_for_lookback(INFLOW24_LOOKBACK_HOURS, 60)))
        rows_4h = fetch_klines(sym, interval="4h", limit=100)

        # Apply 1H Scanners
        if rows_1h:
            inflow24 = analyze_INFLOW24(rows_1h)
            if inflow24: inflow24["symbol"] = sym; scanner_matches["INFLOW24"].append(inflow24)

        # Apply 4H Scanners
        if rows_4h:
            f4 = analyze_F4(rows_4h)
            if f4: f4["symbol"] = sym; scanner_matches["F4"].append(f4)

    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, max(1, len(candidates)))) as ex:
        futures = {ex.submit(process_symbol, sym): sym for sym in candidates}
        for fut in as_completed(futures):
            try: fut.result()
            except Exception: logger.exception(f"Failed processing {futures[fut]}")

    report: Dict[str, Any] = {
        "generated_at": iso_from_ms(now_ms()),
        "params": {
            "BINANCE_TOP_CANDIDATES": BINANCE_TOP_CANDIDATES,
            "scanners": {},
        },
        "scanners": {},
        "overlaps": {},
    }

    for label in SCANNER_ORDER:
        desc = SCANNER_DESCRIPTIONS[label]
        out_csv = f"scanner_{label}_final_v9.csv"
        df = save_matches(scanner_matches[label], out_csv, sort_by=SORT_HINTS.get(label))

        report["scanners"][label] = {
            "description": desc,
            "count": int(len(df)),
            "output_csv": out_csv,
        }

        print_section_header(f"Scanner {label} — {desc}", f"Matches: {len(df)}")
        print_matches_table(label, df, limit=20)

    symbol_map: Dict[str, List[Tuple[str, Dict[str, Any]]]] = {}
    for label in SCANNER_ORDER:
        for m in scanner_matches[label]:
            sym = m.get("symbol")
            if sym: symbol_map.setdefault(sym, []).append((label, m))

    overlap_df = save_overlap_report(symbol_map)
    report["overlaps"] = {
        "count": int(len(overlap_df)),
        "output_csv": OVERLAP_OUTPUT_CSV,
    }

    print_section_header("Overlapping Results", "Symbols matched by 2 or more scanners")
    if overlap_df.empty:
        print("No overlaps.")
    else:
        print(overlap_df[["symbol", "analyzers", "analyzer_count", "match_time_iso"]].head(50).to_string(index=False))

    for label in SCANNER_ORDER:
        report["params"]["scanners"][label] = {
            "description": SCANNER_DESCRIPTIONS[label],
            "sort_hint": SORT_HINTS.get(label, []),
        }

    with open(REPORT_JSON, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)

    print_section_header("Saved Outputs")
    print(f"JSON Master Report: {REPORT_JSON}")
    print(f"Overlaps Report: {OVERLAP_OUTPUT_CSV}")
    logger.info("Scan Complete. Match counts by scanner:")
    logger.info({k: len(v) for k, v in scanner_matches.items()})

if __name__ == "__main__":
    main()
