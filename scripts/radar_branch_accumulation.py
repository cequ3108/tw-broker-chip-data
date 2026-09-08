#!/usr/bin/env python3
"""Radar: domestic broker-branch accumulation (淨買超建倉).

Scans local data/daily/*.parquet for (stock_id, branch) pairs whose cumulative
net buy amount (buy_amt - sell_amt) exceeds a threshold (default 50 億 NTD).

Design notes:
- FinMind branch prints are NOT labeled 自然人/投信/外資. This radar approximates
  "domestic branch channel" by excluding foreign desks and domestic 自營 (id*T).
- Sells at the same branch may come from other clients; we still use NET buy as
  the accumulation signal (same logic as 凱基三多 / 大立光).
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data" / "daily"

# Known foreign / institutional broker IDs commonly seen in branch prints.
FOREIGN_IDS = {
    "1360",  # 港商麥格理
    "1440",  # 美林
    "1470",  # 台灣摩根 / 摩根士丹利
    "1480",  # 美商高盛
    "1520",  # 瑞士信貸（若出現）
    "1560",  # 港商野村
    "1570",
    "1590",  # 花旗環球
    "1650",  # 瑞銀
    "8440",  # 摩根大通
    "8890",  # 大和國泰
    "8900",  # 法銀巴黎
    "8960",  # 上海匯豐
}

NAME_EXCL_RE = re.compile(
    r"(?:自營|投信|摩根|瑞銀|花旗|美林|高盛|野村|麥格理|法銀|"
    r"港商|美商|新加坡商|大和國泰|上海匯豐|匯豐|渣打|巴克萊|"
    r"德意志|法國興業|瑞士信貸|星洲瑞銀)"
)

# Head-office style short names without a branch locality marker.
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
    return pd.concat(frames, ignore_index=True)


def domestic_branch_mask(df: pd.DataFrame, exclude_hq: bool) -> pd.Series:
    tid = df["securities_trader_id"].astype(str)
    name = df["securities_trader"].astype(str).str.replace("－", "-", regex=False)
    # Normalize 凱基-三多 / 凱基三多 style for HQ detection only.
    name_compact = name.str.replace("-", "", regex=False)
    excl = tid.str.endswith("T") | tid.isin(FOREIGN_IDS) | name.str.contains(
        NAME_EXCL_RE, na=False
    )
    if exclude_hq:
        excl = excl | name_compact.str.fullmatch(HQ_NAME_RE.pattern)
    return ~excl


def run_radar(
    *,
    min_net_yi: float,
    start: str | None,
    end: str | None,
    exclude_hq: bool,
    top: int,
) -> pd.DataFrame:
    raw = load_daily(start, end)
    keep = domestic_branch_mask(raw, exclude_hq=exclude_hq)
    df = raw.loc[keep].copy()
    df["net_amt"] = pd.to_numeric(df["buy_amt"], errors="coerce").fillna(0) - pd.to_numeric(
        df["sell_amt"], errors="coerce"
    ).fillna(0)
    df["buy_amt"] = pd.to_numeric(df["buy_amt"], errors="coerce").fillna(0)
    df["sell_amt"] = pd.to_numeric(df["sell_amt"], errors="coerce").fillna(0)
    df["buy"] = pd.to_numeric(df["buy"], errors="coerce").fillna(0)
    df["sell"] = pd.to_numeric(df["sell"], errors="coerce").fillna(0)

    g = (
        df.groupby(
            ["stock_id", "securities_trader_id", "securities_trader"],
            as_index=False,
        )
        .agg(
            days=("date", "nunique"),
            buy_shares=("buy", "sum"),
            sell_shares=("sell", "sum"),
            buy_amt=("buy_amt", "sum"),
            sell_amt=("sell_amt", "sum"),
            net_amt=("net_amt", "sum"),
            first_date=("date", "min"),
            last_date=("date", "max"),
        )
    )
    g["淨買億"] = g["net_amt"] / 1e8
    g["買進億"] = g["buy_amt"] / 1e8
    g["賣出億"] = g["sell_amt"] / 1e8
    g["淨買股數"] = (g["buy_shares"] - g["sell_shares"]).astype("int64")
    hit = g[g["淨買億"] >= min_net_yi].sort_values("淨買億", ascending=False)
    if top > 0:
        hit = hit.head(top)
    return hit


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Domestic branch accumulation radar")
    p.add_argument(
        "--min-net-yi",
        type=float,
        default=50.0,
        help="minimum cumulative net buy in 億 NTD (default 50)",
    )
    p.add_argument("--start", default=None, help="YYYY-MM-DD inclusive")
    p.add_argument("--end", default=None, help="YYYY-MM-DD inclusive")
    p.add_argument(
        "--include-hq",
        action="store_true",
        help="also keep head-office style names (富邦/元大/凱基… without locality)",
    )
    p.add_argument("--top", type=int, default=50, help="max rows to print (0=all hits)")
    p.add_argument(
        "--csv",
        default=None,
        help="optional output CSV path",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    hit = run_radar(
        min_net_yi=args.min_net_yi,
        start=args.start,
        end=args.end,
        exclude_hq=not args.include_hq,
        top=args.top,
    )
    files = sorted(DATA_DIR.glob("*.parquet"))
    days = [p.stem for p in files]
    window = days
    if args.start:
        window = [d for d in window if d >= args.start]
    if args.end:
        window = [d for d in window if d <= args.end]

    print("===== BRANCH ACCUMULATION RADAR =====")
    print(f"data_days: {len(window)} ({window[0] if window else '-'} → {window[-1] if window else '-'})")
    print(f"threshold: 淨買超 >= {args.min_net_yi} 億")
    print(f"exclude_foreign_dealer: yes")
    print(f"exclude_hq_style: {not args.include_hq}")
    print(f"hits: {len(hit)}")
    if hit.empty:
        print("(no hits in current local coverage)")
    else:
        cols = [
            "stock_id",
            "securities_trader",
            "securities_trader_id",
            "淨買億",
            "買進億",
            "賣出億",
            "淨買股數",
            "days",
            "first_date",
            "last_date",
        ]
        pd.set_option("display.width", 200)
        pd.set_option("display.max_rows", 100)
        pd.set_option("display.float_format", lambda x: f"{x:,.2f}")
        print(hit[cols].to_string(index=False))
    if args.csv:
        out = Path(args.csv)
        out.parent.mkdir(parents=True, exist_ok=True)
        hit.to_csv(out, index=False)
        print(f"wrote {out}")
    print("====================================")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
