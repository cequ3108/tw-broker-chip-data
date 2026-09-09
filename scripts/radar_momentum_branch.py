#!/usr/bin/env python3
"""Radar: momentum broker branches that lift price without dumping chips.

Finds domestic (stock, branch) pairs where:
- large same-day net buys (>= impulse threshold) often coincide with big up days
  (TW red K: close>open and return >= big-red threshold)
- after those impulse buys, the branch does NOT sell hard within N sessions
- cumulative window still shows net accumulation (chips not scrambled)

Optionally pairs with lock-style accumulators on the same stock (dual force).
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from pathlib import Path

import numpy as np
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
HUI_LI_RE = re.compile(r"匯立")


def load_daily(start: str | None, end: str | None) -> tuple[pd.DataFrame, list[str]]:
    files = sorted(DATA_DIR.glob("*.parquet"))
    frames = []
    days = []
    for path in files:
        day = path.stem
        if start and day < start:
            continue
        if end and day > end:
            continue
        frames.append(pd.read_parquet(path))
        days.append(day)
    if not frames:
        raise SystemExit("No daily files in window")
    return pd.concat(frames, ignore_index=True), days


def domestic_mask(df: pd.DataFrame, exclude_hq: bool) -> pd.Series:
    tid = df["securities_trader_id"].astype(str)
    name = df["securities_trader"].astype(str).str.replace("－", "-", regex=False)
    name_c = name.str.replace("-", "", regex=False)
    excl = tid.str.endswith("T") | tid.isin(FOREIGN_IDS) | name.str.contains(
        NAME_EXCL_RE, na=False
    )
    if exclude_hq:
        excl = excl | name_c.str.fullmatch(HQ_NAME_RE.pattern)
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
        if (i + 1) % 40 == 0:
            print(f"prices {i + 1}/{len(stock_ids)}", file=sys.stderr)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Momentum branch radar + dual-force pairing")
    p.add_argument("--start", default=None)
    p.add_argument("--end", default=None)
    p.add_argument("--include-hq", action="store_true")
    p.add_argument("--exclude-huili", action="store_true", help="drop 匯立 electronic desks")
    p.add_argument("--impulse-yi", type=float, default=1.0, help="same-day net buy 億 for impulse")
    p.add_argument("--big-red-ret", type=float, default=0.03, help="min return for big red day")
    p.add_argument("--fwd-days", type=int, default=3, help="sell-check horizon after impulse")
    p.add_argument("--max-flip", type=float, default=0.35, help="fwd sell / impulse net")
    p.add_argument("--min-impulse-days", type=int, default=2)
    p.add_argument("--min-avg-ret", type=float, default=0.02)
    p.add_argument("--min-big-red-rate", type=float, default=0.40)
    p.add_argument("--min-sticky-rate", type=float, default=0.60)
    p.add_argument("--min-net-yi", type=float, default=5.0)
    p.add_argument("--max-sell-to-buy", type=float, default=0.55)
    p.add_argument("--lock-min-net-yi", type=float, default=10.0)
    p.add_argument("--top", type=int, default=25)
    p.add_argument("--csv", default=None)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    token = os.environ.get("FINMIND_TOKEN", "").strip()
    if not token:
        raise SystemExit("FINMIND_TOKEN required")

    raw, days = load_daily(args.start, args.end)
    win_start, win_end = days[0], days[-1]
    keep = domestic_mask(raw, exclude_hq=not args.include_hq)
    df = raw.loc[keep].copy()
    for col in ("buy", "sell", "buy_amt", "sell_amt"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    df["net_amt"] = df["buy_amt"] - df["sell_amt"]
    df["is_huili"] = df["securities_trader"].astype(str).str.contains(HUI_LI_RE, na=False)

    g = df.groupby(
        ["stock_id", "securities_trader_id", "securities_trader"], as_index=False
    ).agg(
        days=("date", "nunique"),
        buy_amt=("buy_amt", "sum"),
        sell_amt=("sell_amt", "sum"),
        net_amt=("net_amt", "sum"),
        is_huili=("is_huili", "max"),
    )
    g["淨買億"] = g["net_amt"] / 1e8
    g["買占比"] = g["buy_amt"] / (g["buy_amt"] + g["sell_amt"]).replace(0, np.nan)
    g["賣買比"] = g["sell_amt"] / g["buy_amt"].replace(0, np.nan)

    short = g[(g["淨買億"] >= args.min_net_yi) | ((g["buy_amt"] / 1e8) >= 8)].copy()
    stocks = sorted(short["stock_id"].unique())
    px = fetch_prices(stocks, win_start, win_end, token)
    if px.empty:
        raise SystemExit("no price data")
    for col in ("open", "close"):
        px[col] = pd.to_numeric(px[col], errors="coerce")
    px = px.sort_values(["stock_id", "date"])
    px["prev_close"] = px.groupby("stock_id")["close"].shift(1)
    px["ret"] = px["close"] / px["prev_close"] - 1
    px["is_big_red"] = (px["ret"] >= args.big_red_ret) & (px["close"] > px["open"])

    d = df.merge(
        px[["date", "stock_id", "open", "close", "ret", "is_big_red"]],
        on=["date", "stock_id"],
        how="inner",
    )
    d = d.merge(short[["stock_id", "securities_trader_id"]], on=["stock_id", "securities_trader_id"])
    impulse_amt = args.impulse_yi * 1e8
    imp = d[d["net_amt"] >= impulse_amt].copy()

    dates_by_stock = {sid: sorted(grp["date"].tolist()) for sid, grp in px.groupby("stock_id")}
    pair_day = {
        (r.stock_id, r.securities_trader_id, r.date): (r.buy_amt, r.sell_amt, r.net_amt)
        for r in d.itertuples(index=False)
    }

    rows = []
    for r in imp.itertuples(index=False):
        ds = dates_by_stock.get(r.stock_id, [])
        fwd = []
        if r.date in ds:
            i = ds.index(r.date)
            fwd = ds[i + 1 : i + 1 + args.fwd_days]
        fwd_sell = fwd_buy = fwd_net = 0.0
        for nd in fwd:
            key = (r.stock_id, r.securities_trader_id, nd)
            if key in pair_day:
                b, s, n = pair_day[key]
                fwd_buy += b
                fwd_sell += s
                fwd_net += n
        flip = (fwd_sell / r.net_amt) if r.net_amt > 0 else np.nan
        sticky = (flip <= args.max_flip) and (fwd_net >= -0.2 * r.net_amt)
        rows.append(
            {
                "stock_id": r.stock_id,
                "securities_trader_id": r.securities_trader_id,
                "securities_trader": r.securities_trader,
                "date": r.date,
                "net_amt": r.net_amt,
                "ret": r.ret,
                "is_big_red": bool(r.is_big_red),
                "is_huili": bool(r.is_huili),
                "flip_ratio": flip,
                "sticky": sticky,
            }
        )
    imp_df = pd.DataFrame(rows)
    if imp_df.empty:
        print("no impulse days")
        return 0

    agg = imp_df.groupby(
        ["stock_id", "securities_trader_id", "securities_trader"], as_index=False
    ).agg(
        impulse_days=("date", "nunique"),
        impulse_net=("net_amt", "sum"),
        avg_ret_on_impulse=("ret", "mean"),
        big_red_hits=("is_big_red", "sum"),
        sticky_hits=("sticky", "sum"),
        avg_flip=("flip_ratio", "mean"),
        is_huili=("is_huili", "max"),
    )
    agg["big_red_rate"] = agg["big_red_hits"] / agg["impulse_days"]
    agg["sticky_rate"] = agg["sticky_hits"] / agg["impulse_days"]
    agg["impulse_淨買億"] = agg["impulse_net"] / 1e8
    agg = agg.merge(
        g[["stock_id", "securities_trader_id", "淨買億", "買占比", "賣買比", "days"]],
        on=["stock_id", "securities_trader_id"],
        how="left",
    )
    agg["動能分數"] = (
        agg["avg_ret_on_impulse"].clip(-0.05, 0.12) / 0.12 * 0.35
        + agg["big_red_rate"].fillna(0) * 0.30
        + agg["sticky_rate"].fillna(0) * 0.25
        + (agg["impulse_淨買億"].clip(0, 50) / 50) * 0.10
    )

    mom = agg[
        (agg["impulse_days"] >= args.min_impulse_days)
        & (agg["avg_ret_on_impulse"] >= args.min_avg_ret)
        & (agg["big_red_rate"] >= args.min_big_red_rate)
        & (agg["sticky_rate"] >= args.min_sticky_rate)
        & (agg["淨買億"] >= args.min_net_yi)
        & (agg["賣買比"] <= args.max_sell_to_buy)
    ].sort_values(["動能分數", "impulse_淨買億"], ascending=False)
    if args.exclude_huili:
        mom = mom[~mom["is_huili"]]

    lock = g[
        (g["淨買億"] >= args.lock_min_net_yi)
        & (g["買占比"] >= 0.65)
        & (g["賣買比"] <= 0.40)
        & (g["days"] >= 5)
    ].copy()

    dual_rows = []
    for sid, subm in mom.groupby("stock_id"):
        locks = lock[lock["stock_id"] == sid]
        if locks.empty:
            continue
        for _, mrow in subm.iterrows():
            for _, lrow in locks.iterrows():
                dual_rows.append(
                    {
                        "stock_id": sid,
                        "role": (
                            "同點兼具"
                            if mrow["securities_trader_id"] == lrow["securities_trader_id"]
                            else "雙主力"
                        ),
                        "鎖倉分點": lrow["securities_trader"],
                        "鎖倉_淨買億": lrow["淨買億"],
                        "鎖倉_買占比": lrow["買占比"],
                        "動能分點": mrow["securities_trader"],
                        "動能_淨買億": mrow["淨買億"],
                        "動能_avg_ret": mrow["avg_ret_on_impulse"],
                        "動能_big_red_rate": mrow["big_red_rate"],
                        "動能_sticky": mrow["sticky_rate"],
                        "動能分數": mrow["動能分數"],
                    }
                )
    dual = pd.DataFrame(dual_rows)
    if not dual.empty:
        dual = dual.sort_values(
            ["role", "動能分數", "鎖倉_淨買億"],
            ascending=[True, False, False],
        )

    cols = [
        "stock_id",
        "securities_trader",
        "impulse_days",
        "big_red_rate",
        "avg_ret_on_impulse",
        "sticky_rate",
        "avg_flip",
        "impulse_淨買億",
        "淨買億",
        "買占比",
        "賣買比",
        "動能分數",
    ]
    pd.set_option("display.width", 240)
    pd.set_option("display.max_rows", 80)
    pd.set_option("display.float_format", lambda x: f"{x:,.3f}")

    print("===== MOMENTUM BRANCH RADAR =====")
    print(f"data_days: {len(days)} ({win_start} → {win_end})")
    print(
        f"impulse: 單日淨買>={args.impulse_yi}億; big_red: ret>={args.big_red_ret:.0%} & close>open; "
        f"sticky: {args.fwd_days}日內賣出/衝量 <= {args.max_flip}"
    )
    print(f"hits: {len(mom)}")
    show = mom.head(args.top) if args.top > 0 else mom
    if show.empty:
        print("(no hits)")
    else:
        print(show[cols].to_string(index=False))

    print("\n===== DUAL FORCE (lock + momentum on same stock) =====")
    if dual.empty:
        print("(no dual pairs)")
    else:
        best = dual.groupby("stock_id", as_index=False).head(1)
        if args.top > 0:
            best = best.head(args.top)
        print(best.to_string(index=False))

    if args.csv:
        out = Path(args.csv)
        out.parent.mkdir(parents=True, exist_ok=True)
        mom.to_csv(out, index=False)
        if not dual.empty:
            dual.to_csv(out.with_name(out.stem + "_dual.csv"), index=False)
        print(f"wrote {out}")
    print("=================================")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
