#!/usr/bin/env python3
"""Radar: domestic branch "lock-chip" accumulation.

Finds (stock, branch) pairs that look like patient accumulation:
- cumulative net buy >= threshold (default 10 億)
- buy dominates sell most of the time
- sell/buy amount ratio stays low (chips not flipped)
- a majority of buy amount occurs on relatively low-price days
  (close <= stock's low-percentile within the scanned window)

Price data is fetched from FinMind TaiwanStockPrice (uses FINMIND_TOKEN).
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data" / "daily"
API = "https://api.finmindtrade.com/api/v4/data"

FOREIGN_IDS = {
    "1360",
    "1440",
    "1470",
    "1480",
    "1520",
    "1560",
    "1570",
    "1590",
    "1650",
    "8440",
    "8890",
    "8900",
    "8960",
}
NAME_EXCL_RE = re.compile(
    r"(?:自營|投信|摩根|瑞銀|花旗|美林|高盛|野村|麥格理|法銀|"
    r"港商|美商|新加坡商|大和國泰|上海匯豐|匯豐|渣打|巴克萊|"
    r"德意志|法國興業|瑞士信貸|星洲瑞銀)"
)
HQ_NAME_RE = re.compile(
    r"^(元大|富邦|凱基|永豐金|統一|國泰綜合|國票|兆豐|群益|台新|"
    r"玉山|宏遠|康和|福邦|第一金|合庫|華南永昌|致和|大昌|台灣匯立)$"
)


def load_daily(start: str | None, end: str | None) -> pd.DataFrame:
    files = sorted(DATA_DIR.glob("*.parquet"))
    if not files:
        raise SystemExit(f"No parquet files under {DATA_DIR}")
    frames = []
    for path in files:
        day = path.stem
        if start and day < start:
            continue
        if end and day > end:
            continue
        frames.append(pd.read_parquet(path))
    if not frames:
        raise SystemExit("No daily files in the requested date window")
    return pd.concat(frames, ignore_index=True), [p.stem for p in files if (not start or p.stem >= start) and (not end or p.stem <= end)]


def domestic_branch_mask(df: pd.DataFrame, exclude_hq: bool) -> pd.Series:
    tid = df["securities_trader_id"].astype(str)
    name = df["securities_trader"].astype(str).str.replace("－", "-", regex=False)
    name_compact = name.str.replace("-", "", regex=False)
    excl = tid.str.endswith("T") | tid.isin(FOREIGN_IDS) | name.str.contains(
        NAME_EXCL_RE, na=False
    )
    if exclude_hq:
        excl = excl | name_compact.str.fullmatch(HQ_NAME_RE.pattern)
    return ~excl


def fetch_prices(stock_ids: list[str], start: str, end: str, token: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for i, sid in enumerate(stock_ids):
        for attempt in range(6):
            r = requests.get(
                API,
                params={
                    "dataset": "TaiwanStockPrice",
                    "data_id": sid,
                    "start_date": start,
                    "end_date": end,
                    "token": token,
                },
                timeout=60,
            )
            if r.status_code == 429:
                time.sleep(min(2**attempt, 30))
                continue
            payload = r.json()
            if payload.get("msg") == "success" and payload.get("data"):
                frames.append(pd.DataFrame(payload["data"]))
            break
        if (i + 1) % 25 == 0:
            print(f"prices {i + 1}/{len(stock_ids)}", file=sys.stderr)
    if not frames:
        return pd.DataFrame(columns=["date", "stock_id", "close"])
    return pd.concat(frames, ignore_index=True)


def run(
    *,
    min_net_yi: float,
    start: str | None,
    end: str | None,
    exclude_hq: bool,
    min_buy_ratio: float,
    max_sell_to_buy: float,
    min_net_day_ratio: float,
    min_days: int,
    low_pct: float,
    min_low_buy_share: float,
    top: int,
) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    raw, days = load_daily(start, end)
    if not days:
        raise SystemExit("empty window")
    win_start, win_end = days[0], days[-1]

    keep = domestic_branch_mask(raw, exclude_hq=exclude_hq)
    df = raw.loc[keep].copy()
    for col in ("buy", "sell", "buy_amt", "sell_amt"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    df["net_amt"] = df["buy_amt"] - df["sell_amt"]
    df["active"] = (df["buy"] + df["sell"]) > 0
    df["net_pos"] = df["net_amt"] > 0

    g = df.groupby(
        ["stock_id", "securities_trader_id", "securities_trader"], as_index=False
    ).agg(
        days=("date", "nunique"),
        active_days=("active", "sum"),
        net_pos_days=("net_pos", "sum"),
        buy_shares=("buy", "sum"),
        sell_shares=("sell", "sum"),
        buy_amt=("buy_amt", "sum"),
        sell_amt=("sell_amt", "sum"),
        net_amt=("net_amt", "sum"),
        first_date=("date", "min"),
        last_date=("date", "max"),
    )
    g["淨買億"] = g["net_amt"] / 1e8
    g["買進億"] = g["buy_amt"] / 1e8
    g["賣出億"] = g["sell_amt"] / 1e8
    g["買占比"] = g["buy_amt"] / (g["buy_amt"] + g["sell_amt"]).replace(0, pd.NA)
    g["賣買比"] = g["sell_amt"] / g["buy_amt"].replace(0, pd.NA)
    g["淨買日占比"] = g["net_pos_days"] / g["active_days"].replace(0, pd.NA)
    g["持有跨度日"] = (
        pd.to_datetime(g["last_date"]) - pd.to_datetime(g["first_date"])
    ).dt.days + 1

    cand = g[
        (g["淨買億"] >= min_net_yi)
        & (g["days"] >= min_days)
        & (g["買占比"] >= min_buy_ratio)
        & (g["賣買比"] <= max_sell_to_buy)
        & (g["淨買日占比"] >= min_net_day_ratio)
    ].copy()

    token = os.environ.get("FINMIND_TOKEN", "").strip()
    if cand.empty:
        return cand, cand, days
    if not token:
        raise SystemExit("FINMIND_TOKEN required for TaiwanStockPrice low-range filter")

    px = fetch_prices(sorted(cand["stock_id"].unique()), win_start, win_end, token)
    px["close"] = pd.to_numeric(px["close"], errors="coerce")
    thr = px.groupby("stock_id")["close"].quantile(low_pct).rename("low_thr")
    px = px.merge(thr, on="stock_id", how="left")
    px["is_low"] = px["close"] <= px["low_thr"]

    day = df[
        [
            "date",
            "stock_id",
            "securities_trader_id",
            "securities_trader",
            "buy_amt",
            "sell_amt",
            "net_amt",
        ]
    ].merge(px[["date", "stock_id", "close", "is_low"]], on=["date", "stock_id"], how="left")
    day = day.merge(
        cand[["stock_id", "securities_trader_id"]].drop_duplicates(),
        on=["stock_id", "securities_trader_id"],
        how="inner",
    )
    day["buy_low"] = day["buy_amt"].where(day["is_low"].fillna(False), 0.0)
    low_agg = day.groupby(["stock_id", "securities_trader_id"], as_index=False).agg(
        buy_low=("buy_low", "sum"),
        buy_amt_chk=("buy_amt", "sum"),
    )
    low_agg["低檔買占比"] = low_agg["buy_low"] / low_agg["buy_amt_chk"].replace(0, pd.NA)
    out = cand.merge(
        low_agg[["stock_id", "securities_trader_id", "低檔買占比"]],
        on=["stock_id", "securities_trader_id"],
        how="left",
    )
    out["鎖倉分數"] = (
        out["買占比"].fillna(0) * 0.35
        + out["淨買日占比"].fillna(0) * 0.25
        + (1 - out["賣買比"].clip(0, 1).fillna(1)) * 0.20
        + out["低檔買占比"].fillna(0) * 0.20
    )
    hit = out[out["低檔買占比"] >= min_low_buy_share].sort_values(
        ["鎖倉分數", "淨買億"], ascending=False
    )
    near = out[out["低檔買占比"] < min_low_buy_share].sort_values("淨買億", ascending=False)
    if top > 0:
        hit = hit.head(top)
        near = near.head(top)
    return hit, near, days


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Domestic branch lock-chip radar")
    p.add_argument("--min-net-yi", type=float, default=10.0)
    p.add_argument("--start", default=None)
    p.add_argument("--end", default=None)
    p.add_argument("--include-hq", action="store_true")
    p.add_argument("--min-buy-ratio", type=float, default=0.65)
    p.add_argument("--max-sell-to-buy", type=float, default=0.40)
    p.add_argument("--min-net-day-ratio", type=float, default=0.60)
    p.add_argument("--min-days", type=int, default=5)
    p.add_argument("--low-pct", type=float, default=0.40, help="close quantile as low range")
    p.add_argument("--min-low-buy-share", type=float, default=0.50)
    p.add_argument("--top", type=int, default=50)
    p.add_argument("--csv", default=None)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    hit, near, days = run(
        min_net_yi=args.min_net_yi,
        start=args.start,
        end=args.end,
        exclude_hq=not args.include_hq,
        min_buy_ratio=args.min_buy_ratio,
        max_sell_to_buy=args.max_sell_to_buy,
        min_net_day_ratio=args.min_net_day_ratio,
        min_days=args.min_days,
        low_pct=args.low_pct,
        min_low_buy_share=args.min_low_buy_share,
        top=args.top,
    )
    cols = [
        "stock_id",
        "securities_trader",
        "securities_trader_id",
        "淨買億",
        "買進億",
        "賣出億",
        "買占比",
        "賣買比",
        "淨買日占比",
        "低檔買占比",
        "days",
        "持有跨度日",
        "first_date",
        "last_date",
        "鎖倉分數",
    ]
    pd.set_option("display.width", 220)
    pd.set_option("display.max_rows", 100)
    pd.set_option("display.float_format", lambda x: f"{x:,.2f}")

    print("===== LOCK-CHIP BRANCH RADAR =====")
    print(f"data_days: {len(days)} ({days[0] if days else '-'} → {days[-1] if days else '-'})")
    print(
        f"filters: 淨買>={args.min_net_yi}億, 買占比>={args.min_buy_ratio}, "
        f"賣/買<={args.max_sell_to_buy}, 淨買日占比>={args.min_net_day_ratio}, "
        f"低檔買占比>={args.min_low_buy_share} (close Q{args.low_pct:.0%})"
    )
    print(f"hits: {len(hit)}")
    if hit.empty:
        print("(no full hits in current coverage)")
    else:
        print(hit[cols].to_string(index=False))
    print(f"\nnear-miss (pass lock filters, low-buy share soft): {len(near)}")
    if not near.empty:
        print(near[cols].head(20).to_string(index=False))
    if args.csv:
        out = Path(args.csv)
        out.parent.mkdir(parents=True, exist_ok=True)
        hit.to_csv(out, index=False)
        print(f"wrote {out}")
    print("=================================")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
