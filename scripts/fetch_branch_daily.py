#!/usr/bin/env python3
"""Fetch Taiwan stock branch (分點) chip data from FinMind.

Modes:
  auto     — if fewer than 5 daily parquet files exist, backfill ~1 year;
             otherwise fill recent missing trading days.
  daily    — fill missing trading days since last complete file (or last N days).
  backfill — fetch ~1 year of market-wide branch data with checkpoint resume.

Rate limit: FinMind Sponsor ~600 req/hour. Default --max-requests 500.
One stock × one date = one request. Do not use storage_objects (SponsorPro only).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data" / "daily"
STATE_DIR = ROOT / "state"
CHECKPOINT_PATH = STATE_DIR / "checkpoint.json"
LATEST_PATH = STATE_DIR / "latest.json"

TAIPEI = ZoneInfo("Asia/Taipei")
API_BASE = "https://api.finmindtrade.com/api/v4"
DATASET_REPORT = "TaiwanStockTradingDailyReport"
DATASET_INFO = "TaiwanStockInfo"
DATASET_TRADING_DATE = "TaiwanStockTradingDate"

OUTPUT_COLUMNS = [
    "date",
    "stock_id",
    "securities_trader_id",
    "securities_trader",
    "buy",
    "sell",
    "buy_amt",
    "sell_amt",
]

MIN_HISTORY_DAYS_FOR_DAILY = 5
BACKFILL_CALENDAR_DAYS = 365
DEFAULT_MAX_REQUESTS = 500
REQUEST_SLEEP_SEC = 0.05
HTTP_TIMEOUT = 60

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("fetch_branch_daily")


class RateLimitError(Exception):
    """HTTP 429 or explicit FinMind rate-limit payload."""


class ApiError(Exception):
    """Non-retryable or exhausted FinMind API error."""


@dataclass
class RunStats:
    mode: str
    requests_used: int = 0
    completed_dates: list[str] | None = None
    paused: bool = False
    pause_reason: str = ""
    in_progress_date: str | None = None
    in_progress_stock: str | None = None
    pending_dates: list[str] | None = None
    errors: list[str] | None = None

    def __post_init__(self) -> None:
        self.completed_dates = self.completed_dates or []
        self.pending_dates = self.pending_dates or []
        self.errors = self.errors or []


def today_taipei() -> date:
    return datetime.now(TAIPEI).date()


def data_asof_date() -> date:
    """FinMind branch prints publish around 21:00 Taipei; before that, exclude today."""
    now = datetime.now(TAIPEI)
    if now.hour < 21:
        return now.date() - timedelta(days=1)
    return now.date()


def require_token() -> str:
    token = os.environ.get("FINMIND_TOKEN", "").strip()
    if not token:
        raise SystemExit("FINMIND_TOKEN is not set")
    return token


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")
    tmp.replace(path)


def existing_daily_dates() -> list[str]:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    dates = []
    for p in DATA_DIR.glob("*.parquet"):
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", p.stem):
            dates.append(p.stem)
    return sorted(dates)


def empty_checkpoint() -> dict[str, Any]:
    return {
        "mode": None,
        "pending_dates": [],
        "in_progress_date": None,
        "completed_stocks": [],
        "stock_universe": [],
        "requests_used_session": 0,
        "updated_at": None,
        "note": None,
    }


def load_checkpoint() -> dict[str, Any]:
    cp = load_json(CHECKPOINT_PATH, empty_checkpoint())
    for key, default in empty_checkpoint().items():
        cp.setdefault(key, default)
    return cp


def save_checkpoint(cp: dict[str, Any]) -> None:
    cp["updated_at"] = datetime.now(timezone.utc).isoformat()
    save_json(CHECKPOINT_PATH, cp)


def update_latest(
    *,
    mode: str,
    last_completed_date: str | None,
    requests_used: int,
    paused: bool,
    message: str,
) -> None:
    payload = {
        "mode": mode,
        "last_completed_date": last_completed_date,
        "available_dates": existing_daily_dates(),
        "requests_used_last_run": requests_used,
        "paused": paused,
        "message": message,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "updated_at_taipei": datetime.now(TAIPEI).isoformat(),
    }
    save_json(LATEST_PATH, payload)


class FinMindClient:
    def __init__(self, token: str, stats: RunStats) -> None:
        self.token = token
        self.stats = stats
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {token}"})

    def _count_request(self) -> None:
        self.stats.requests_used += 1

    @retry(
        retry=retry_if_exception_type((RateLimitError, requests.RequestException)),
        wait=wait_exponential(multiplier=2, min=5, max=180),
        stop=stop_after_attempt(8),
        reraise=True,
    )
    def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        url = f"{API_BASE}/{path}"
        self._count_request()
        resp = self.session.get(url, params=params, timeout=HTTP_TIMEOUT)
        if resp.status_code == 429:
            log.warning("HTTP 429 rate limited; backing off")
            raise RateLimitError("HTTP 429")
        if resp.status_code >= 500:
            raise requests.RequestException(f"HTTP {resp.status_code}")
        try:
            payload = resp.json()
        except ValueError as exc:
            raise ApiError(f"Invalid JSON status={resp.status_code}") from exc

        status = payload.get("status", "")
        status_s = str(status).lower()
        msg = str(payload.get("msg", "") or "")
        lower_msg = msg.lower()
        if resp.status_code == 402 or ("sponsor" in lower_msg and "only" in lower_msg):
            raise ApiError(msg or "Sponsor-only dataset")
        if "rate limit" in lower_msg or "too many request" in lower_msg:
            log.warning("FinMind rate-limit message: %s", msg)
            raise RateLimitError(msg)
        if resp.status_code >= 400:
            raise ApiError(f"HTTP {resp.status_code}: {msg or payload}")
        ok = status in {200, "200"} or status_s in {"", "success", "ok"}
        if not ok and payload.get("data") is None:
            if "limit" in lower_msg:
                raise RateLimitError(msg)
            raise ApiError(msg or f"Unexpected status={status}")
        return payload

    def get_data(self, dataset: str, **params: Any) -> pd.DataFrame:
        payload = self._get("data", {"dataset": dataset, **params})
        rows = payload.get("data") or []
        return pd.DataFrame(rows)

    def get_trading_daily_report(self, stock_id: str, trade_date: str) -> pd.DataFrame:
        # Dedicated endpoint; still one request per stock/day.
        payload = self._get(
            "taiwan_stock_trading_daily_report",
            {"data_id": stock_id, "date": trade_date},
        )
        rows = payload.get("data") or []
        return pd.DataFrame(rows)


def fetch_stock_universe(client: FinMindClient) -> list[str]:
    log.info("Fetching TaiwanStockInfo for listed/OTC common stocks")
    info = client.get_data(DATASET_INFO)
    if info.empty:
        raise ApiError("TaiwanStockInfo returned empty")

    df = info.copy()
    if "date" in df.columns:
        # FinMind may include literal "None" date strings; keep the densest real snapshot.
        dates = df["date"].astype(str)
        valid = df[dates.str.fullmatch(r"\d{4}-\d{2}-\d{2}")].copy()
        if valid.empty:
            raise ApiError("TaiwanStockInfo has no valid dated rows")
        latest = valid["date"].astype(str).value_counts().index[0]
        df = valid[valid["date"].astype(str) == latest].copy()
        log.info("Using TaiwanStockInfo snapshot date=%s rows=%d", latest, len(df))

    type_col = "type" if "type" in df.columns else None
    industry_col = "industry_category" if "industry_category" in df.columns else None

    if type_col:
        df = df[df[type_col].astype(str).str.lower().isin(["twse", "tpex"])]

    # Ordinary shares: 4-digit numeric codes.
    df = df[df["stock_id"].astype(str).str.fullmatch(r"\d{4}")]

    if industry_col:
        pattern = r"ETF|ETN|權證|牛證|熊證|特別股|受益憑證|存託憑證"
        df = df[~df[industry_col].astype(str).str.contains(pattern, na=False)]

    if "stock_name" in df.columns:
        df = df[~df["stock_name"].astype(str).str.contains(r"特別股|ETF|ETN", na=False)]

    # TaiwanStockInfo may list the same stock under multiple industry rows.
    df = df.drop_duplicates(subset=["stock_id"], keep="first")

    stock_ids = sorted(df["stock_id"].astype(str).unique().tolist())
    log.info("Stock universe size: %d", len(stock_ids))
    if len(stock_ids) < 1000:
        log.warning("Universe unexpectedly small (%d); proceeding anyway", len(stock_ids))
    return stock_ids


def fetch_trading_dates(
    client: FinMindClient, start: date, end: date
) -> list[str]:
    log.info("Fetching trading dates %s → %s", start, end)
    df = client.get_data(
        DATASET_TRADING_DATE,
        start_date=start.isoformat(),
        end_date=end.isoformat(),
    )
    if df.empty:
        # Fallback: use 2330 daily prices as calendar proxy.
        log.warning("TaiwanStockTradingDate empty; fallback to 2330 price calendar")
        df = client.get_data(
            "TaiwanStockPrice",
            data_id="2330",
            start_date=start.isoformat(),
            end_date=end.isoformat(),
        )
        if df.empty:
            return []
        dates = sorted(df["date"].astype(str).unique().tolist())
        return dates

    date_col = "date" if "date" in df.columns else df.columns[0]
    dates = sorted(pd.to_datetime(df[date_col]).dt.strftime("%Y-%m-%d").unique().tolist())
    return [d for d in dates if start.isoformat() <= d <= end.isoformat()]


def aggregate_branch_rows(raw: pd.DataFrame) -> pd.DataFrame:
    if raw is None or raw.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    df = raw.copy()
    for col in ("buy", "sell", "price"):
        if col not in df.columns:
            df[col] = 0
    df["buy"] = pd.to_numeric(df["buy"], errors="coerce").fillna(0)
    df["sell"] = pd.to_numeric(df["sell"], errors="coerce").fillna(0)
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)
    df["buy_amt"] = df["buy"] * df["price"]
    df["sell_amt"] = df["sell"] * df["price"]

    grouped = (
        df.groupby(
            ["date", "stock_id", "securities_trader_id", "securities_trader"],
            dropna=False,
            as_index=False,
        )
        .agg(
            buy=("buy", "sum"),
            sell=("sell", "sum"),
            buy_amt=("buy_amt", "sum"),
            sell_amt=("sell_amt", "sum"),
        )
    )
    grouped["date"] = grouped["date"].astype(str)
    grouped["stock_id"] = grouped["stock_id"].astype(str)
    grouped["securities_trader_id"] = grouped["securities_trader_id"].astype(str)
    grouped["securities_trader"] = grouped["securities_trader"].astype(str)
    for col in ("buy", "sell"):
        grouped[col] = grouped[col].round().astype("int64")
    for col in ("buy_amt", "sell_amt"):
        grouped[col] = grouped[col].astype("float64")
    return grouped[OUTPUT_COLUMNS]


def write_daily_parquet(trade_date: str, frames: list[pd.DataFrame]) -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = DATA_DIR / f"{trade_date}.parquet"
    if frames:
        out = pd.concat(frames, ignore_index=True)
    else:
        out = pd.DataFrame(columns=OUTPUT_COLUMNS)
    if not out.empty:
        out = out.sort_values(
            ["stock_id", "securities_trader_id"]
        ).reset_index(drop=True)
    out.to_parquet(path, index=False)
    log.info("Wrote %s rows=%d", path.relative_to(ROOT), len(out))
    return path


def resolve_mode(requested: str) -> str:
    if requested != "auto":
        return requested
    n = len(existing_daily_dates())
    mode = "backfill" if n < MIN_HISTORY_DAYS_FOR_DAILY else "daily"
    log.info("auto → %s (existing daily files=%d)", mode, n)
    return mode


def build_target_dates(
    client: FinMindClient, mode: str, cp: dict[str, Any]
) -> list[str]:
    # Resume pending queue first if present and mode matches / unset.
    pending = [d for d in (cp.get("pending_dates") or []) if isinstance(d, str)]
    in_progress = cp.get("in_progress_date")
    if in_progress and in_progress not in pending:
        pending = [in_progress] + pending
    if pending and (cp.get("mode") in {None, mode}):
        # Drop dates already fully written (unless currently in progress).
        have = set(existing_daily_dates())
        resumed = []
        for d in pending:
            if d == in_progress or d not in have:
                resumed.append(d)
        # Unique preserve order
        seen: set[str] = set()
        out = []
        for d in resumed:
            if d not in seen:
                seen.add(d)
                out.append(d)
        if out:
            log.info("Resuming %d pending/in-progress dates", len(out))
            return out

    end = data_asof_date()
    # Evening job may run before same-day FinMind publish; allow up to as-of date.
    if mode == "backfill":
        start = end - timedelta(days=BACKFILL_CALENDAR_DAYS)
    else:
        have = existing_daily_dates()
        if have:
            start = date.fromisoformat(have[-1]) + timedelta(days=1)
        else:
            start = end - timedelta(days=14)
        # Also re-check a short lookback window for gaps.
        lookback = end - timedelta(days=21)
        if start > lookback:
            start = lookback

    trading = fetch_trading_dates(client, start, end)
    have = set(existing_daily_dates())
    targets = [d for d in trading if d not in have]
    if mode == "daily":
        # Prefer recent gaps only; keep chronological order.
        targets = targets[-10:]
    log.info("Target dates (%d): %s%s", len(targets), targets[:5], " ..." if len(targets) > 5 else "")
    return targets


def fetch_one_day(
    client: FinMindClient,
    trade_date: str,
    stock_ids: list[str],
    cp: dict[str, Any],
    stats: RunStats,
    max_requests: int,
) -> str:
    """Return 'completed' | 'paused' | 'error'."""
    completed = set(cp.get("completed_stocks") or [])
    remaining = [s for s in stock_ids if s not in completed]
    frames: list[pd.DataFrame] = []

    # Reload partial frames from temp if present.
    partial_path = STATE_DIR / f"partial_{trade_date}.parquet"
    if partial_path.exists():
        try:
            frames.append(pd.read_parquet(partial_path))
            log.info("Loaded partial %s rows=%d", partial_path.name, len(frames[-1]))
        except Exception as exc:  # noqa: BLE001
            log.warning("Failed reading partial file: %s", exc)

    cp["in_progress_date"] = trade_date
    save_checkpoint(cp)

    batch_frames: list[pd.DataFrame] = []
    for idx, stock_id in enumerate(remaining, start=1):
        if stats.requests_used >= max_requests:
            stats.paused = True
            stats.pause_reason = "max_requests reached"
            stats.in_progress_date = trade_date
            stats.in_progress_stock = stock_id
            if batch_frames:
                frames.extend(batch_frames)
                pd.concat(frames, ignore_index=True).to_parquet(partial_path, index=False)
            cp["completed_stocks"] = sorted(completed)
            save_checkpoint(cp)
            log.info(
                "Paused at %s stock=%s (%d/%d done) requests=%d",
                trade_date,
                stock_id,
                len(completed),
                len(stock_ids),
                stats.requests_used,
            )
            return "paused"

        stats.in_progress_date = trade_date
        stats.in_progress_stock = stock_id
        try:
            raw = client.get_trading_daily_report(stock_id, trade_date)
            agg = aggregate_branch_rows(raw)
            if not agg.empty:
                batch_frames.append(agg)
            completed.add(stock_id)
        except RateLimitError as exc:
            stats.paused = True
            stats.pause_reason = f"rate limited: {exc}"
            stats.in_progress_date = trade_date
            stats.in_progress_stock = stock_id
            stats.errors.append(f"{trade_date}/{stock_id}: rate limited")
            if batch_frames:
                frames.extend(batch_frames)
                pd.concat(frames, ignore_index=True).to_parquet(partial_path, index=False)
            cp["completed_stocks"] = sorted(completed)
            save_checkpoint(cp)
            return "paused"
        except Exception as exc:  # noqa: BLE001
            # Skip individual stock failure but keep going; empty means no branch prints.
            msg = f"{trade_date}/{stock_id}: {exc}"
            log.warning(msg)
            stats.errors.append(msg)
            completed.add(stock_id)

        if idx % 25 == 0 or idx == len(remaining):
            cp["completed_stocks"] = sorted(completed)
            cp["requests_used_session"] = stats.requests_used
            save_checkpoint(cp)
            if batch_frames:
                frames.extend(batch_frames)
                pd.concat(frames, ignore_index=True).to_parquet(partial_path, index=False)
                batch_frames = []
            log.info(
                "%s progress %d/%d requests=%d",
                trade_date,
                len(completed),
                len(stock_ids),
                stats.requests_used,
            )

        if REQUEST_SLEEP_SEC:
            time.sleep(REQUEST_SLEEP_SEC)

    if batch_frames:
        frames.extend(batch_frames)
    write_daily_parquet(trade_date, frames)
    if partial_path.exists():
        partial_path.unlink()

    cp["in_progress_date"] = None
    cp["completed_stocks"] = []
    pending = [d for d in (cp.get("pending_dates") or []) if d != trade_date]
    cp["pending_dates"] = pending
    save_checkpoint(cp)
    stats.completed_dates.append(trade_date)
    stats.in_progress_date = None
    stats.in_progress_stock = None
    return "completed"


def run(mode: str, max_requests: int) -> RunStats:
    token = require_token()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    resolved = resolve_mode(mode)
    stats = RunStats(mode=resolved)
    client = FinMindClient(token, stats)
    cp = load_checkpoint()

    # Stock universe: reuse checkpoint list when resuming same mode.
    stock_ids = [s for s in (cp.get("stock_universe") or []) if isinstance(s, str)]
    if not stock_ids or cp.get("mode") not in {None, resolved}:
        stock_ids = fetch_stock_universe(client)
    else:
        log.info("Reusing checkpoint stock universe (%d)", len(stock_ids))

    targets = build_target_dates(client, resolved, cp)
    cp["mode"] = resolved
    cp["stock_universe"] = stock_ids
    cp["pending_dates"] = targets
    if not cp.get("in_progress_date") and targets:
        # Keep completed_stocks only when continuing the same in-progress date.
        pass
    if cp.get("in_progress_date") and cp["in_progress_date"] not in targets:
        cp["completed_stocks"] = []
        cp["in_progress_date"] = None
    save_checkpoint(cp)

    if not targets:
        msg = "no missing trading dates"
        log.info(msg)
        update_latest(
            mode=resolved,
            last_completed_date=(existing_daily_dates() or [None])[-1],
            requests_used=stats.requests_used,
            paused=False,
            message=msg,
        )
        return stats

    for trade_date in list(targets):
        # Reset per-day completed stocks when starting a new date.
        if cp.get("in_progress_date") not in {None, trade_date}:
            cp["completed_stocks"] = []
        if cp.get("in_progress_date") != trade_date:
            # Fresh day
            if trade_date != cp.get("in_progress_date"):
                cp["completed_stocks"] = []
        status = fetch_one_day(client, trade_date, stock_ids, cp, stats, max_requests)
        if status == "paused":
            break
        # Refresh pending list after completion
        cp = load_checkpoint()
        targets = [d for d in (cp.get("pending_dates") or []) if d != trade_date]
        cp["pending_dates"] = targets
        save_checkpoint(cp)

    cp = load_checkpoint()
    stats.pending_dates = list(cp.get("pending_dates") or [])
    if cp.get("in_progress_date"):
        stats.in_progress_date = cp["in_progress_date"]
        stats.paused = True
        if not stats.pause_reason:
            stats.pause_reason = "in_progress remains"

    last_done = (stats.completed_dates or existing_daily_dates() or [None])[-1]
    if stats.paused:
        message = (
            f"paused ({stats.pause_reason}); "
            f"in_progress={stats.in_progress_date}/{stats.in_progress_stock}; "
            f"pending={len(stats.pending_dates)}"
        )
    else:
        message = f"completed dates={stats.completed_dates}"
    update_latest(
        mode=resolved,
        last_completed_date=last_done if isinstance(last_done, str) else None,
        requests_used=stats.requests_used,
        paused=stats.paused,
        message=message,
    )
    return stats


def print_summary(stats: RunStats) -> None:
    print("\n===== RUN SUMMARY =====")
    print(f"mode: {stats.mode}")
    print(f"completed_dates: {stats.completed_dates}")
    print(f"requests_used: {stats.requests_used}")
    print(f"paused: {stats.paused}")
    if stats.paused:
        print(f"pause_reason: {stats.pause_reason}")
        print(f"in_progress_date: {stats.in_progress_date}")
        print(f"in_progress_stock: {stats.in_progress_stock}")
    print(f"pending_dates_count: {len(stats.pending_dates or [])}")
    if stats.pending_dates:
        head = (stats.pending_dates or [])[:8]
        print(f"pending_dates_head: {head}")
    if stats.errors:
        print(f"errors_count: {len(stats.errors)}")
        for e in stats.errors[:10]:
            print(f"  - {e}")
    else:
        print("errors: none")
    print("=======================\n")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fetch FinMind branch chip data")
    p.add_argument(
        "--mode",
        choices=["auto", "daily", "backfill"],
        default="auto",
        help="auto|daily|backfill (default: auto)",
    )
    p.add_argument(
        "--max-requests",
        type=int,
        default=DEFAULT_MAX_REQUESTS,
        help=f"stop before exceeding FinMind hourly budget (default {DEFAULT_MAX_REQUESTS})",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.max_requests < 1:
        raise SystemExit("--max-requests must be >= 1")
    try:
        stats = run(args.mode, args.max_requests)
    except Exception as exc:  # noqa: BLE001
        log.exception("Fatal error: %s", exc)
        print(f"FATAL: {exc}", file=sys.stderr)
        return 1
    print_summary(stats)
    # Non-zero only on hard failure; pause-for-rate-limit is success with pending work.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
