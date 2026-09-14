#!/usr/bin/env python3
"""Limit-up lock × domestic broker-branch associative backtest.

See docs/limitup_branch_backtest_spec.md. Requires FINMIND_TOKEN for prices
unless --dry-run. Do not hardcode tokens.

Event: close locked at official (or 10% approx) limit-up on a chip-sample day.
Features reuse the same foreign/proprietary/HQ filters as the radar scripts.
Entry baseline: T close. Not a causal claim and not a fillable strategy.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from collections import OrderedDict
from pathlib import Path

import numpy as np
import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data" / "daily"
CACHE_DIR = ROOT / "data" / "cache" / "price"
API = "https://api.finmindtrade.com/api/v4/data"
OUT_DEFAULT = ROOT / "output" / "limitup_branch_backtest"

# Identical to radar_lock_chip.py / radar_momentum_branch.py / radar_branch_accumulation.py
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
INDUSTRY_EXCL_RE = re.compile(r"ETF|ETN|權證|牛證|熊證|特別股|受益憑證|存託憑證")

CHIP_COLS = [
    "date",
    "stock_id",
    "securities_trader_id",
    "securities_trader",
    "buy",
    "sell",
    "buy_amt",
    "sell_amt",
]
RET_COLS = [
    "ret_t1_open",
    "ret_t1_close",
    "ret_t2_close",
    "ret_t3_close",
    "ret_t5_close",
    "mfe_t5",
    "mae_t5",
]


def chip_dates_on_disk() -> list[str]:
    return sorted(
        p.stem
        for p in DATA_DIR.glob("*.parquet")
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", p.stem)
    )


def filter_chip_dates(
    all_days: list[str], start: str | None, end: str | None
) -> list[str]:
    out = all_days
    if start:
        out = [d for d in out if d >= start]
    if end:
        out = [d for d in out if d <= end]
    return out


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


def require_token() -> str:
    token = os.environ.get("FINMIND_TOKEN", "").strip()
    if not token:
        raise SystemExit(
            "FINMIND_TOKEN is not set. Export a FinMind Sponsor token to fetch "
            "TaiwanStockPrice / TaiwanStockPriceLimit, or pass --dry-run to only "
            "print chip-date coverage."
        )
    return token


def tw_tick(price: float) -> float:
    if price < 10:
        return 0.01
    if price < 50:
        return 0.05
    if price < 100:
        return 0.10
    if price < 500:
        return 0.50
    if price < 1000:
        return 1.0
    return 5.0


def tick_round(price: float) -> float:
    if not np.isfinite(price) or price <= 0:
        return float("nan")
    tick = tw_tick(price)
    return round(round(price / tick) * tick, 6)


def approx_limit_up(prev_close: float) -> float:
    return tick_round(prev_close * 1.10)


def finmind_get(
    params: dict,
    token: str,
    *,
    sleep_sec: float,
    retries: int = 6,
) -> pd.DataFrame:
    last_err = "unknown"
    for attempt in range(retries):
        try:
            r = requests.get(
                API,
                params={**params, "token": token},
                timeout=90,
            )
        except requests.RequestException as exc:
            last_err = str(exc)
            time.sleep(min(2**attempt, 30))
            continue
        if r.status_code == 429:
            last_err = "HTTP 429"
            time.sleep(min(2**attempt, 60))
            continue
        try:
            payload = r.json()
        except ValueError:
            last_err = f"invalid JSON status={r.status_code}"
            time.sleep(min(2**attempt, 30))
            continue
        msg = str(payload.get("msg") or "")
        lower = msg.lower()
        if r.status_code == 429 or "rate limit" in lower or "too many request" in lower:
            last_err = msg or "rate limit"
            time.sleep(min(2**attempt, 60))
            continue
        if payload.get("msg") == "success" or str(payload.get("status")) in {"200", "success"}:
            rows = payload.get("data") or []
            if sleep_sec:
                time.sleep(sleep_sec)
            return pd.DataFrame(rows)
        last_err = msg or f"status={payload.get('status')} http={r.status_code}"
        break
    raise SystemExit(f"FinMind request failed ({params.get('dataset')}): {last_err}")


def _filter_four_digit(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "stock_id" not in df.columns:
        return df
    out = df.copy()
    out["stock_id"] = out["stock_id"].astype(str)
    out["date"] = out["date"].astype(str)
    return out[out["stock_id"].str.fullmatch(r"\d{4}")].copy()


def load_or_fetch_day(
    dataset: str,
    day: str,
    token: str,
    *,
    sleep_sec: float,
    use_cache: bool,
    request_counter: list[int],
) -> pd.DataFrame:
    cache_path = CACHE_DIR / dataset / f"{day}.parquet"
    if use_cache and cache_path.exists():
        return pd.read_parquet(cache_path)
    df = finmind_get(
        {"dataset": dataset, "start_date": day, "end_date": day},
        token,
        sleep_sec=sleep_sec,
    )
    request_counter[0] += 1
    df = _filter_four_digit(df)
    if use_cache:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(cache_path, index=False)
    return df


def fetch_stock_info(token: str, *, sleep_sec: float, use_cache: bool) -> pd.DataFrame:
    cache_path = CACHE_DIR / "TaiwanStockInfo.parquet"
    if use_cache and cache_path.exists():
        info = pd.read_parquet(cache_path)
    else:
        info = finmind_get({"dataset": "TaiwanStockInfo"}, token, sleep_sec=sleep_sec)
        if use_cache:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            info.to_parquet(cache_path, index=False)
    if info.empty:
        raise SystemExit("TaiwanStockInfo returned empty")
    df = info.copy()
    df["stock_id"] = df["stock_id"].astype(str)
    if "date" in df.columns:
        dates = df["date"].astype(str)
        valid = df[dates.str.fullmatch(r"\d{4}-\d{2}-\d{2}")].copy()
        if valid.empty:
            raise SystemExit("TaiwanStockInfo has no dated rows")
        latest = valid["date"].astype(str).value_counts().index[0]
        df = valid[valid["date"].astype(str) == latest].copy()
    type_col = "type" if "type" in df.columns else None
    if type_col:
        df = df[df[type_col].astype(str).str.lower().isin(["twse", "tpex"])]
    df = df[df["stock_id"].str.fullmatch(r"\d{4}")]
    if "industry_category" in df.columns:
        df = df[~df["industry_category"].astype(str).str.contains(INDUSTRY_EXCL_RE, na=False)]
    if "stock_name" in df.columns:
        df = df[~df["stock_name"].astype(str).str.contains(r"特別股|ETF|ETN", na=False)]
    df = df.drop_duplicates(subset=["stock_id"], keep="first")
    out = pd.DataFrame(
        {
            "stock_id": df["stock_id"].astype(str),
            "stock_name": df["stock_name"].astype(str) if "stock_name" in df.columns else "",
            "market": df["type"].astype(str).str.lower() if type_col else "",
        }
    )
    return out.reset_index(drop=True)


def fetch_trading_dates(
    token: str, start: str, end: str, *, sleep_sec: float
) -> list[str]:
    df = finmind_get(
        {
            "dataset": "TaiwanStockTradingDate",
            "start_date": start,
            "end_date": end,
        },
        token,
        sleep_sec=sleep_sec,
    )
    if df.empty:
        px = finmind_get(
            {
                "dataset": "TaiwanStockPrice",
                "data_id": "2330",
                "start_date": start,
                "end_date": end,
            },
            token,
            sleep_sec=sleep_sec,
        )
        if px.empty:
            return []
        return sorted(px["date"].astype(str).unique().tolist())
    col = "date" if "date" in df.columns else df.columns[0]
    days = sorted(pd.to_datetime(df[col]).dt.strftime("%Y-%m-%d").unique().tolist())
    return [d for d in days if start <= d <= end]


def load_price_panel(
    days: list[str],
    token: str,
    *,
    sleep_sec: float,
    use_cache: bool,
    request_counter: list[int],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    px_frames = []
    lim_frames = []
    n = len(days)
    for i, day in enumerate(days, start=1):
        px = load_or_fetch_day(
            "TaiwanStockPrice",
            day,
            token,
            sleep_sec=sleep_sec,
            use_cache=use_cache,
            request_counter=request_counter,
        )
        lim = load_or_fetch_day(
            "TaiwanStockPriceLimit",
            day,
            token,
            sleep_sec=sleep_sec,
            use_cache=use_cache,
            request_counter=request_counter,
        )
        if not px.empty:
            px_frames.append(px)
        if not lim.empty:
            lim_frames.append(lim)
        if i % 20 == 0 or i == n:
            print(f"prices+limits {i}/{n} (api_calls={request_counter[0]})", file=sys.stderr)
    if not px_frames:
        raise SystemExit("no TaiwanStockPrice rows in requested range")
    px = pd.concat(px_frames, ignore_index=True)
    for col in ("open", "max", "min", "close"):
        if col in px.columns:
            px[col] = pd.to_numeric(px[col], errors="coerce")
        else:
            px[col] = np.nan
    px["date"] = px["date"].astype(str)
    px["stock_id"] = px["stock_id"].astype(str)
    px = px.sort_values(["stock_id", "date"]).drop_duplicates(["stock_id", "date"], keep="last")
    if lim_frames:
        lim = pd.concat(lim_frames, ignore_index=True)
        for col in ("limit_up", "limit_down", "reference_price"):
            if col in lim.columns:
                lim[col] = pd.to_numeric(lim[col], errors="coerce")
        lim["date"] = lim["date"].astype(str)
        lim["stock_id"] = lim["stock_id"].astype(str)
        lim = lim.drop_duplicates(["stock_id", "date"], keep="last")
    else:
        lim = pd.DataFrame(columns=["date", "stock_id", "limit_up", "reference_price"])
    return px, lim


def build_event_frame(
    px: pd.DataFrame,
    lim: pd.DataFrame,
    info: pd.DataFrame,
    event_days: list[str],
    lock_tol: float,
    big_red_ret: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    universe = set(info["stock_id"].astype(str))
    bar = px[px["stock_id"].isin(universe)].copy()
    bar = bar.merge(
        lim[["date", "stock_id", "limit_up", "reference_price"]],
        on=["date", "stock_id"],
        how="left",
    )
    bar["prev_close"] = bar.groupby("stock_id")["close"].shift(1)
    official = pd.to_numeric(bar["limit_up"], errors="coerce")
    approx = bar["prev_close"].map(lambda x: approx_limit_up(float(x)) if pd.notna(x) else np.nan)
    bar["limit_src"] = np.where(official.fillna(0) > 0, "official", "approx_10pct")
    bar["limit_up_used"] = np.where(official.fillna(0) > 0, official, approx)
    bar["is_limit_up_lock"] = bar["close"] >= (bar["limit_up_used"] * lock_tol)
    bar["touched_limit_up"] = bar["max"] >= (bar["limit_up_used"] * lock_tol)
    bar["ret_vs_prev"] = bar["close"] / bar["prev_close"] - 1
    bar["is_big_red"] = (bar["ret_vs_prev"] >= big_red_ret) & (bar["close"] > bar["open"])

    # Consecutive lock count on the price calendar (includes T).
    lock_int = bar["is_limit_up_lock"].fillna(False).astype(int)
    not_lock = lock_int.eq(0)
    streak_id = not_lock.groupby(bar["stock_id"]).cumsum()
    bar["consec_limit_up"] = lock_int.groupby([bar["stock_id"], streak_id]).cumsum()
    bar.loc[~bar["is_limit_up_lock"].fillna(False), "consec_limit_up"] = 0

    ev = bar[bar["date"].isin(set(event_days)) & bar["is_limit_up_lock"].fillna(False)].copy()
    ev = ev.merge(info, on="stock_id", how="left")
    return ev, bar


class ChipStore:
    """LRU of daily parquet frames, filtered to event stocks + domestic mask."""

    def __init__(
        self,
        event_stocks: set[str],
        *,
        exclude_hq: bool,
        max_cached_days: int,
    ) -> None:
        self.event_stocks = event_stocks
        self.exclude_hq = exclude_hq
        self.max_cached_days = max_cached_days
        self.cache: OrderedDict[str, pd.DataFrame] = OrderedDict()

    def day(self, day: str) -> pd.DataFrame:
        if day in self.cache:
            self.cache.move_to_end(day)
            return self.cache[day]
        path = DATA_DIR / f"{day}.parquet"
        if not path.exists():
            empty = pd.DataFrame(columns=CHIP_COLS + ["net_amt", "is_huili"])
            self.cache[day] = empty
            return empty
        df = pd.read_parquet(path, columns=CHIP_COLS)
        df["stock_id"] = df["stock_id"].astype(str)
        df["date"] = df["date"].astype(str)
        if self.event_stocks:
            df = df[df["stock_id"].isin(self.event_stocks)]
        if df.empty:
            out = df.copy()
            out["net_amt"] = np.float64(0)
            out["is_huili"] = False
            self.cache[day] = out
            self._evict()
            return out
        keep = domestic_branch_mask(df, exclude_hq=self.exclude_hq)
        df = df.loc[keep].copy()
        df["is_huili"] = df["securities_trader"].astype(str).str.contains(HUI_LI_RE, na=False)
        for col in ("buy", "sell", "buy_amt", "sell_amt"):
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        df["net_amt"] = df["buy_amt"] - df["sell_amt"]
        self.cache[day] = df
        self._evict()
        return df

    def _evict(self) -> None:
        while len(self.cache) > self.max_cached_days:
            self.cache.popitem(last=False)

    def window(self, days: list[str]) -> pd.DataFrame:
        frames = [self.day(d) for d in days]
        frames = [f for f in frames if not f.empty]
        if not frames:
            return pd.DataFrame(columns=CHIP_COLS + ["net_amt", "is_huili"])
        return pd.concat(frames, ignore_index=True)


def lookback_days(chip_days: list[str], t: str, n: int) -> list[str]:
    prev = [d for d in chip_days if d <= t]
    return prev[-n:]


def day_leaders(
    day_df: pd.DataFrame,
    stocks: list[str],
    top1_lo: float,
    top1_hi: float,
) -> pd.DataFrame:
    if day_df.empty:
        return pd.DataFrame(
            {
                "stock_id": stocks,
                "chip_present": False,
                "top1_trader": pd.NA,
                "top1_trader_id": pd.NA,
                "top1_net_yi": 0.0,
                "top3_net_yi": 0.0,
                "top1_ge_lo": False,
                "top1_ge_hi": False,
                "top1_is_huili": False,
            }
        )
    sub = day_df[day_df["stock_id"].isin(stocks)]
    present = set(sub["stock_id"].unique())
    if sub.empty:
        ranked = pd.DataFrame()
    else:
        ranked = sub.sort_values(["stock_id", "net_amt"], ascending=[True, False])
        ranked["rk"] = ranked.groupby("stock_id").cumcount() + 1
    rows = []
    top1_map = {}
    top3_map = {}
    if not ranked.empty:
        t1 = ranked[ranked["rk"] == 1]
        top1_map = {r.stock_id: r for r in t1.itertuples(index=False)}
        top3_map = (
            ranked[ranked["rk"] <= 3]
            .groupby("stock_id")["net_amt"]
            .sum()
            .to_dict()
        )
    for sid in stocks:
        r = top1_map.get(sid)
        top1_yi = (float(r.net_amt) / 1e8) if r is not None else 0.0
        name = getattr(r, "securities_trader", None) if r is not None else None
        rows.append(
            {
                "stock_id": sid,
                "chip_present": sid in present,
                "top1_trader": name,
                "top1_trader_id": getattr(r, "securities_trader_id", None) if r is not None else None,
                "top1_net_yi": top1_yi,
                "top3_net_yi": float(top3_map.get(sid, 0.0)) / 1e8,
                "top1_ge_lo": top1_yi >= top1_lo,
                "top1_ge_hi": top1_yi >= top1_hi,
                "top1_is_huili": bool(
                    name and HUI_LI_RE.search(str(name))
                ),
            }
        )
    return pd.DataFrame(rows)


def lock_chip_flags(
    win: pd.DataFrame,
    stocks: list[str],
    px_win: pd.DataFrame,
    args: argparse.Namespace,
) -> pd.DataFrame:
    empty = pd.DataFrame(
        {
            "stock_id": stocks,
            "has_lock_chip_branch": False,
            "lock_branch": pd.NA,
            "lock_branch_id": pd.NA,
            "lock_net_yi": np.nan,
            "lock_buy_ratio": np.nan,
            "lock_sell_to_buy": np.nan,
            "lock_net_day_ratio": np.nan,
            "lock_low_buy_share": np.nan,
            "lock_score": np.nan,
        }
    )
    if win.empty:
        return empty
    df = win[win["stock_id"].isin(stocks)].copy()
    if df.empty:
        return empty
    df["active"] = (df["buy"] + df["sell"]) > 0
    df["net_pos"] = df["net_amt"] > 0
    g = df.groupby(
        ["stock_id", "securities_trader_id", "securities_trader"], as_index=False
    ).agg(
        days=("date", "nunique"),
        active_days=("active", "sum"),
        net_pos_days=("net_pos", "sum"),
        buy_amt=("buy_amt", "sum"),
        sell_amt=("sell_amt", "sum"),
        net_amt=("net_amt", "sum"),
    )
    g["買占比"] = g["buy_amt"] / (g["buy_amt"] + g["sell_amt"]).replace(0, np.nan)
    g["賣買比"] = g["sell_amt"] / g["buy_amt"].replace(0, np.nan)
    g["淨買日占比"] = g["net_pos_days"] / g["active_days"].replace(0, np.nan)
    g["淨買億"] = g["net_amt"] / 1e8

    low_ok = False
    if not px_win.empty:
        p = px_win[px_win["stock_id"].isin(stocks)][["date", "stock_id", "close"]].copy()
        if not p.empty and p["close"].notna().any():
            thr = p.groupby("stock_id")["close"].quantile(args.lock_low_pct).rename("low_thr")
            p = p.merge(thr, on="stock_id", how="left")
            p["is_low"] = p["close"] <= p["low_thr"]
            day = df.merge(p[["date", "stock_id", "is_low"]], on=["date", "stock_id"], how="left")
            day["buy_low"] = day["buy_amt"].where(day["is_low"].fillna(False), 0.0)
            low_agg = day.groupby(
                ["stock_id", "securities_trader_id"], as_index=False
            ).agg(buy_low=("buy_low", "sum"), buy_amt_chk=("buy_amt", "sum"))
            low_agg["低檔買占比"] = low_agg["buy_low"] / low_agg["buy_amt_chk"].replace(0, np.nan)
            g = g.merge(
                low_agg[["stock_id", "securities_trader_id", "低檔買占比"]],
                on=["stock_id", "securities_trader_id"],
                how="left",
            )
            low_ok = True
    if "低檔買占比" not in g.columns:
        g["低檔買占比"] = np.nan

    cand = g[
        (g["淨買億"] >= args.lock_min_net_yi)
        & (g["days"] >= args.lock_min_days)
        & (g["買占比"] >= args.lock_min_buy_ratio)
        & (g["賣買比"] <= args.lock_max_sell_to_buy)
        & (g["淨買日占比"] >= args.lock_min_net_day_ratio)
    ].copy()
    if low_ok:
        # Only enforce low-share when the stock had any priced days in-window.
        priced_stocks = set(px_win.loc[px_win["close"].notna(), "stock_id"].astype(str))
        need = cand["stock_id"].isin(priced_stocks)
        cand = cand.loc[~need | (cand["低檔買占比"] >= args.lock_min_low_buy_share)].copy()

    if cand.empty:
        return empty
    cand["鎖倉分數"] = (
        cand["買占比"].fillna(0) * 0.35
        + cand["淨買日占比"].fillna(0) * 0.25
        + (1 - cand["賣買比"].clip(0, 1).fillna(1)) * 0.20
        + cand["低檔買占比"].fillna(0) * 0.20
    )
    best = cand.sort_values(["stock_id", "鎖倉分數", "淨買億"], ascending=[True, False, False])
    best = best.groupby("stock_id", as_index=False).head(1)
    out = empty.copy()
    out["has_lock_chip_branch"] = out["stock_id"].isin(set(best["stock_id"]))
    m = best.set_index("stock_id")
    mapper = {
        "lock_branch": "securities_trader",
        "lock_branch_id": "securities_trader_id",
        "lock_net_yi": "淨買億",
        "lock_buy_ratio": "買占比",
        "lock_sell_to_buy": "賣買比",
        "lock_net_day_ratio": "淨買日占比",
        "lock_low_buy_share": "低檔買占比",
        "lock_score": "鎖倉分數",
    }
    for dest, src in mapper.items():
        out[dest] = out["stock_id"].map(m[src] if src in m.columns else {})
    return out


def momentum_flags(
    win: pd.DataFrame,
    stocks: list[str],
    t: str,
    bar: pd.DataFrame,
    chip_days: list[str],
    args: argparse.Namespace,
) -> pd.DataFrame:
    empty = pd.DataFrame(
        {
            "stock_id": stocks,
            "has_momentum_branch": False,
            "mom_branch": pd.NA,
            "mom_branch_id": pd.NA,
            "mom_impulse_date": pd.NA,
            "mom_impulse_yi": np.nan,
            "mom_flip": np.nan,
            "mom_sticky": False,
            "mom_is_huili": False,
        }
    )
    if win.empty:
        return empty
    df = win[win["stock_id"].isin(stocks)].copy()
    if df.empty:
        return empty
    impulse_amt = args.impulse_yi * 1e8
    imp = df[df["net_amt"] >= impulse_amt].copy()
    if imp.empty:
        return empty

    px_cols = bar.loc[
        bar["stock_id"].isin(stocks),
        ["date", "stock_id", "open", "close", "ret_vs_prev", "is_big_red", "is_limit_up_lock"],
    ]
    imp = imp.merge(px_cols, on=["date", "stock_id"], how="left")
    imp["strong"] = (
        ((imp["ret_vs_prev"] >= args.big_red_ret) & (imp["close"] > imp["open"]))
        | imp["is_big_red"].fillna(False)
        | imp["is_limit_up_lock"].fillna(False)
    )
    imp = imp[imp["strong"]].copy()
    if imp.empty:
        return empty

    pair_day = {
        (r.stock_id, r.securities_trader_id, r.date): (float(r.buy_amt), float(r.sell_amt), float(r.net_amt))
        for r in df.itertuples(index=False)
    }

    rows = []
    for r in imp.itertuples(index=False):
        s = r.date
        if s > t:
            continue
        after = [d for d in chip_days if s < d <= t]
        fwd = after[: args.fwd_days]
        fwd_sell = fwd_net = 0.0
        for nd in fwd:
            key = (r.stock_id, r.securities_trader_id, nd)
            if key in pair_day:
                _b, sell, net = pair_day[key]
                fwd_sell += sell
                fwd_net += net
        if s == t or not fwd:
            flip = 0.0
            sticky = True
        else:
            flip = (fwd_sell / r.net_amt) if r.net_amt > 0 else np.nan
            sticky = (flip <= args.max_flip) and (fwd_net >= -0.2 * r.net_amt)
        if not sticky:
            continue
        rows.append(
            {
                "stock_id": r.stock_id,
                "mom_branch": r.securities_trader,
                "mom_branch_id": r.securities_trader_id,
                "mom_impulse_date": s,
                "mom_impulse_yi": float(r.net_amt) / 1e8,
                "mom_flip": flip,
                "mom_sticky": True,
                "mom_is_huili": bool(getattr(r, "is_huili", False)),
            }
        )
    if not rows:
        return empty
    hits = pd.DataFrame(rows)
    hits = hits.sort_values(
        ["stock_id", "mom_impulse_date", "mom_impulse_yi"],
        ascending=[True, False, False],
    )
    best = hits.groupby("stock_id", as_index=False).head(1)
    out = empty.copy()
    out["has_momentum_branch"] = out["stock_id"].isin(set(best["stock_id"]))
    m = best.set_index("stock_id")
    for col in (
        "mom_branch",
        "mom_branch_id",
        "mom_impulse_date",
        "mom_impulse_yi",
        "mom_flip",
        "mom_sticky",
        "mom_is_huili",
    ):
        out[col] = out["stock_id"].map(m[col] if col in m.columns else {})
    return out


def attach_forward_returns(events: pd.DataFrame, bar: pd.DataFrame) -> pd.DataFrame:
    px = bar[["date", "stock_id", "open", "max", "min", "close"]].copy()
    px = px.sort_values(["stock_id", "date"])
    dates_by_stock = {
        sid: grp["date"].tolist() for sid, grp in px.groupby("stock_id", sort=False)
    }
    keyed = px.set_index(["stock_id", "date"])

    recs = []
    for r in events.itertuples(index=False):
        ds = dates_by_stock.get(r.stock_id, [])
        row = {c: np.nan for c in RET_COLS}
        row["fwd_n"] = 0
        row["fwd_complete"] = False
        if r.date not in ds:
            recs.append(row)
            continue
        i = ds.index(r.date)
        fwd = ds[i + 1 : i + 6]
        row["fwd_n"] = len(fwd)
        row["fwd_complete"] = len(fwd) >= 5
        try:
            t_close = float(keyed.loc[(r.stock_id, r.date), "close"])
        except KeyError:
            recs.append(row)
            continue
        if not np.isfinite(t_close) or t_close <= 0:
            recs.append(row)
            continue
        highs = []
        lows = []
        for k, nd in enumerate(fwd, start=1):
            rec = keyed.loc[(r.stock_id, nd)]
            o, h, lo, c = (
                float(rec["open"]),
                float(rec["max"]),
                float(rec["min"]),
                float(rec["close"]),
            )
            if k == 1 and np.isfinite(o):
                row["ret_t1_open"] = o / t_close - 1
            if np.isfinite(c):
                if k == 1:
                    row["ret_t1_close"] = c / t_close - 1
                elif k == 2:
                    row["ret_t2_close"] = c / t_close - 1
                elif k == 3:
                    row["ret_t3_close"] = c / t_close - 1
                elif k == 5:
                    row["ret_t5_close"] = c / t_close - 1
            if np.isfinite(h):
                highs.append(h)
            if np.isfinite(lo):
                lows.append(lo)
        if highs:
            row["mfe_t5"] = max(highs) / t_close - 1
        if lows:
            row["mae_t5"] = min(lows) / t_close - 1
        recs.append(row)
    return pd.concat([events.reset_index(drop=True), pd.DataFrame(recs)], axis=1)


def _style_group(lock: bool, mom: bool) -> str:
    if lock and mom:
        return "dual"
    if lock:
        return "single_lock"
    if mom:
        return "single_mom"
    return "neither"


def summarize(events: pd.DataFrame, min_samples: int) -> tuple[pd.DataFrame, list[str]]:
    groups: list[tuple[str, str, pd.Series]] = [
        ("all", "all", pd.Series(True, index=events.index)),
        ("top1_ge_1yi", "yes", events["top1_ge_hi"].fillna(False)),
        ("top1_ge_1yi", "no", ~events["top1_ge_hi"].fillna(False)),
        ("has_lock_chip_branch", "yes", events["has_lock_chip_branch"].fillna(False)),
        ("has_lock_chip_branch", "no", ~events["has_lock_chip_branch"].fillna(False)),
        ("has_momentum_branch", "yes", events["has_momentum_branch"].fillna(False)),
        ("has_momentum_branch", "no", ~events["has_momentum_branch"].fillna(False)),
        ("style_group", "dual", events["style_group"] == "dual"),
        (
            "style_group",
            "single",
            events["style_group"].isin(["single_lock", "single_mom"]),
        ),
        ("style_group", "single_lock", events["style_group"] == "single_lock"),
        ("style_group", "single_mom", events["style_group"] == "single_mom"),
        ("style_group", "neither", events["style_group"] == "neither"),
        (
            "top1_ge_1yi_x_lock",
            "yes_yes",
            events["top1_ge_hi"].fillna(False) & events["has_lock_chip_branch"].fillna(False),
        ),
        (
            "top1_ge_1yi_x_lock",
            "yes_no",
            events["top1_ge_hi"].fillna(False) & ~events["has_lock_chip_branch"].fillna(False),
        ),
        (
            "top1_ge_1yi_x_lock",
            "no_yes",
            ~events["top1_ge_hi"].fillna(False) & events["has_lock_chip_branch"].fillna(False),
        ),
        (
            "top1_ge_1yi_x_lock",
            "no_no",
            ~events["top1_ge_hi"].fillna(False) & ~events["has_lock_chip_branch"].fillna(False),
        ),
        (
            "top1_ge_1yi_x_mom",
            "yes_yes",
            events["top1_ge_hi"].fillna(False) & events["has_momentum_branch"].fillna(False),
        ),
        (
            "top1_ge_1yi_x_mom",
            "yes_no",
            events["top1_ge_hi"].fillna(False) & ~events["has_momentum_branch"].fillna(False),
        ),
        (
            "top1_ge_1yi_x_mom",
            "no_yes",
            ~events["top1_ge_hi"].fillna(False) & events["has_momentum_branch"].fillna(False),
        ),
        (
            "top1_ge_1yi_x_mom",
            "no_no",
            ~events["top1_ge_hi"].fillna(False) & ~events["has_momentum_branch"].fillna(False),
        ),
        ("top1_is_huili", "yes", events["top1_is_huili"].fillna(False)),
        ("top1_is_huili", "no", ~events["top1_is_huili"].fillna(False)),
        ("market", "twse", events["market"].astype(str) == "twse"),
        ("market", "tpex", events["market"].astype(str) == "tpex"),
    ]

    warn: list[str] = []
    rows = []
    for group, subgroup, mask in groups:
        sub = events.loc[mask]
        base = {
            "group": group,
            "subgroup": subgroup,
            "n_events": int(len(sub)),
            "n_fwd_complete": int(sub["fwd_complete"].fillna(False).sum()) if len(sub) else 0,
        }
        if len(sub) < min_samples:
            warn.append(f"{group}/{subgroup} n={len(sub)} < min_samples={min_samples}")
        for col in RET_COLS:
            s = pd.to_numeric(sub[col], errors="coerce").dropna() if len(sub) else pd.Series(dtype=float)
            n = int(len(s))
            base[f"{col}_n"] = n
            if n == 0:
                base[f"{col}_win_rate"] = np.nan
                base[f"{col}_mean"] = np.nan
                base[f"{col}_median"] = np.nan
                base[f"{col}_p25"] = np.nan
                base[f"{col}_p75"] = np.nan
            else:
                base[f"{col}_win_rate"] = float((s > 0).mean())
                base[f"{col}_mean"] = float(s.mean())
                base[f"{col}_median"] = float(s.median())
                base[f"{col}_p25"] = float(s.quantile(0.25))
                base[f"{col}_p75"] = float(s.quantile(0.75))
        rows.append(base)
    return pd.DataFrame(rows), warn


def print_summary_tables(summary: pd.DataFrame, events: pd.DataFrame) -> None:
    pd.set_option("display.width", 220)
    pd.set_option("display.max_rows", 80)
    pd.set_option("display.float_format", lambda x: f"{x:,.3f}")

    def slice_metric(metric: str) -> pd.DataFrame:
        cols = [
            "group",
            "subgroup",
            "n_events",
            f"{metric}_n",
            f"{metric}_win_rate",
            f"{metric}_mean",
            f"{metric}_median",
            f"{metric}_p25",
            f"{metric}_p75",
        ]
        return summary[cols].copy()

    print("\n----- T+1 close vs T close -----")
    print(slice_metric("ret_t1_close").to_string(index=False))
    print("\n----- T+1 open vs T close (overnight gap) -----")
    print(slice_metric("ret_t1_open").to_string(index=False))
    print("\n----- T+5 close vs T close -----")
    print(slice_metric("ret_t5_close").to_string(index=False))

    print("\n----- event counts -----")
    print(f"events: {len(events)}")
    if not events.empty:
        print(
            events.groupby("style_group").size().rename("n").to_string()
        )
        print(
            "top1_ge_hi:",
            int(events["top1_ge_hi"].fillna(False).sum()),
            "/",
            len(events),
        )
        print(
            "lock / mom / dual:",
            int(events["has_lock_chip_branch"].fillna(False).sum()),
            "/",
            int(events["has_momentum_branch"].fillna(False).sum()),
            "/",
            int(events["dual_main"].fillna(False).sum()),
        )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Close-locked limit-up × domestic branch backtest"
    )
    p.add_argument("--start", default=None, help="YYYY-MM-DD inclusive (chip dates)")
    p.add_argument("--end", default=None, help="YYYY-MM-DD inclusive (chip dates)")
    p.add_argument("--lookback", type=int, default=20, help="chip days ending at T")
    p.add_argument("--impulse-yi", type=float, default=1.0, help="surge-day net buy 億")
    p.add_argument(
        "--top1-threshold-yi",
        type=float,
        default=1.0,
        help="Top1 net-buy 億 for the main binary split",
    )
    p.add_argument(
        "--top1-flag-yi",
        type=float,
        default=0.5,
        help="extra Top1 binary threshold 億 (default 0.5)",
    )
    p.add_argument("--include-hq", action="store_true")
    p.add_argument(
        "--exclude-huili",
        action="store_true",
        help="drop 匯立 desks from feature construction",
    )
    p.add_argument("--min-samples", type=int, default=20)
    p.add_argument("--lock-min-net-yi", type=float, default=2.0)
    p.add_argument("--lock-min-buy-ratio", type=float, default=0.65)
    p.add_argument("--lock-max-sell-to-buy", type=float, default=0.40)
    p.add_argument("--lock-min-net-day-ratio", type=float, default=0.60)
    p.add_argument("--lock-min-days", type=int, default=5)
    p.add_argument("--lock-low-pct", type=float, default=0.40)
    p.add_argument("--lock-min-low-buy-share", type=float, default=0.50)
    p.add_argument("--big-red-ret", type=float, default=0.03)
    p.add_argument("--fwd-days", type=int, default=3, help="post-surge sticky horizon through T")
    p.add_argument("--max-flip", type=float, default=0.35)
    p.add_argument("--lock-tol", type=float, default=0.999, help="close >= limit_up * tol")
    p.add_argument("--consec-pad", type=int, default=10, help="extra price days before window")
    p.add_argument("--sleep", type=float, default=0.12, help="seconds between FinMind calls")
    p.add_argument("--no-cache", action="store_true")
    p.add_argument("--dry-run", action="store_true", help="chip coverage only; no price API")
    p.add_argument(
        "--outdir",
        default=str(OUT_DEFAULT),
        help="output directory for events/summary CSV",
    )
    return p.parse_args(argv)


def print_coverage(
    *,
    all_chip: list[str],
    window_chip: list[str],
    start: str | None,
    end: str | None,
    trading_missing: list[str] | None,
) -> None:
    print("===== LIMIT-UP × BRANCH BACKTEST =====")
    print(
        f"repo chip days: {len(all_chip)} "
        f"({all_chip[0] if all_chip else '-'} → {all_chip[-1] if all_chip else '-'})"
    )
    print(
        f"window chip days: {len(window_chip)} "
        f"({window_chip[0] if window_chip else '-'} → {window_chip[-1] if window_chip else '-'})"
        f"  start={start or all_chip[0] if all_chip else '-'} end={end or (all_chip[-1] if all_chip else '-')}"
    )
    if len(all_chip) < 60:
        print("WARNING: chip sample is sparse (<60 files); results are not a full-year study.")
    if trading_missing:
        print(
            f"WARNING: {len(trading_missing)} trading days in the window have no chip parquet "
            f"(lookback jumps these). head={trading_missing[:8]}"
        )
        print(f"         missing span hint: {trading_missing[0]} … {trading_missing[-1]}")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    all_chip = chip_dates_on_disk()
    if not all_chip:
        raise SystemExit(f"No parquet files under {DATA_DIR}")
    window_chip = filter_chip_dates(all_chip, args.start, args.end)
    if not window_chip:
        raise SystemExit("No daily files in the requested date window")

    # Lookback buffer: chip dates before window start (still on disk).
    first_t, last_t = window_chip[0], window_chip[-1]
    pre = [d for d in all_chip if d < first_t][-args.lookback :]
    feature_chip = pre + window_chip

    if args.dry_run:
        print_coverage(
            all_chip=all_chip,
            window_chip=window_chip,
            start=args.start,
            end=args.end,
            trading_missing=None,
        )
        print("dry-run: skipped price fetch and event construction")
        print("====================================")
        return 0

    token = require_token()
    use_cache = not args.no_cache
    request_counter = [0]

    # Calendar pad so T+5 and consecutive locks are computable.
    cal_start = (pd.Timestamp(feature_chip[0]) - pd.Timedelta(days=45)).strftime("%Y-%m-%d")
    cal_end = (pd.Timestamp(last_t) + pd.Timedelta(days=21)).strftime("%Y-%m-%d")
    trading = fetch_trading_dates(token, cal_start, cal_end, sleep_sec=args.sleep)
    request_counter[0] += 1
    if not trading:
        raise SystemExit("TaiwanStockTradingDate empty")

    # Price days: consec-pad trading days before first feature chip date, through +7 after last.
    first_px = next((d for d in trading if d >= feature_chip[0]), feature_chip[0])
    i0 = trading.index(first_px) if first_px in trading else 0
    i0 = max(0, i0 - args.consec_pad)
    if last_t in trading:
        i1 = min(len(trading) - 1, trading.index(last_t) + 7)
    else:
        later = [i for i, d in enumerate(trading) if d > last_t]
        i1 = later[6] if len(later) >= 7 else (later[-1] if later else len(trading) - 1)
    price_days = trading[i0 : i1 + 1]

    trading_in_window = [d for d in trading if first_t <= d <= last_t]
    missing_chip = [d for d in trading_in_window if d not in set(window_chip)]
    print_coverage(
        all_chip=all_chip,
        window_chip=window_chip,
        start=args.start,
        end=args.end,
        trading_missing=missing_chip,
    )
    print(
        f"filters: exclude_hq={not args.include_hq} exclude_huili={args.exclude_huili} "
        f"lookback={args.lookback} impulse>={args.impulse_yi}億 top1_hi>={args.top1_threshold_yi}億"
    )
    print("NOTE: associative backtest; T-close entry is not a fillable limit-up print.")

    info = fetch_stock_info(token, sleep_sec=args.sleep, use_cache=use_cache)
    print(f"universe: {len(info)} ordinary TWSE/TPEx", file=sys.stderr)
    px, lim = load_price_panel(
        price_days,
        token,
        sleep_sec=args.sleep,
        use_cache=use_cache,
        request_counter=request_counter,
    )
    ev, bar = build_event_frame(
        px, lim, info, window_chip, args.lock_tol, args.big_red_ret
    )
    if ev.empty:
        print("events: 0 (no close-locked limit-up on chip dates in window)")
        print("====================================")
        return 0

    event_stocks = set(ev["stock_id"].astype(str))
    store = ChipStore(
        event_stocks,
        exclude_hq=not args.include_hq,
        max_cached_days=args.lookback + 3,
    )

    feat_rows = []
    event_dates = sorted(ev["date"].astype(str).unique())
    for j, t in enumerate(event_dates, start=1):
        stocks = ev.loc[ev["date"] == t, "stock_id"].astype(str).tolist()
        win_days = lookback_days(feature_chip, t, args.lookback)
        day_df = store.day(t)
        leaders_all = day_leaders(day_df, stocks, args.top1_flag_yi, args.top1_threshold_yi)
        day_feat = day_df.loc[~day_df["is_huili"]].copy() if args.exclude_huili and not day_df.empty else day_df
        win = store.window(win_days)
        win_feat = win.loc[~win["is_huili"]].copy() if args.exclude_huili and not win.empty else win
        if args.exclude_huili:
            leaders = day_leaders(day_feat, stocks, args.top1_flag_yi, args.top1_threshold_yi)
            leaders["top1_is_huili"] = leaders_all["top1_is_huili"]
        else:
            leaders = leaders_all
        px_win = bar[bar["date"].isin(win_days)][["date", "stock_id", "close"]]
        locks = lock_chip_flags(win_feat, stocks, px_win, args)
        moms = momentum_flags(win_feat, stocks, t, bar, win_days, args)
        part = leaders.merge(locks, on="stock_id", how="left").merge(moms, on="stock_id", how="left")
        part["date"] = t
        feat_rows.append(part)
        if j % 15 == 0 or j == len(event_dates):
            print(f"features {j}/{len(event_dates)} dates", file=sys.stderr)

    feat = pd.concat(feat_rows, ignore_index=True)
    events = ev.merge(feat, on=["date", "stock_id"], how="left")
    events["has_lock_chip_branch"] = events["has_lock_chip_branch"].fillna(False)
    events["has_momentum_branch"] = events["has_momentum_branch"].fillna(False)
    events["dual_main"] = events["has_lock_chip_branch"] & events["has_momentum_branch"]
    events["style_group"] = [
        _style_group(bool(l), bool(m))
        for l, m in zip(events["has_lock_chip_branch"], events["has_momentum_branch"])
    ]
    events = attach_forward_returns(events, bar)

    keep_cols = [
        "date",
        "stock_id",
        "stock_name",
        "market",
        "open",
        "max",
        "min",
        "close",
        "prev_close",
        "limit_up_used",
        "limit_src",
        "touched_limit_up",
        "consec_limit_up",
        "chip_present",
        "top1_trader",
        "top1_trader_id",
        "top1_net_yi",
        "top3_net_yi",
        "top1_ge_lo",
        "top1_ge_hi",
        "top1_is_huili",
        "has_lock_chip_branch",
        "lock_branch",
        "lock_branch_id",
        "lock_net_yi",
        "lock_buy_ratio",
        "lock_sell_to_buy",
        "lock_net_day_ratio",
        "lock_low_buy_share",
        "lock_score",
        "has_momentum_branch",
        "mom_branch",
        "mom_branch_id",
        "mom_impulse_date",
        "mom_impulse_yi",
        "mom_flip",
        "mom_sticky",
        "mom_is_huili",
        "dual_main",
        "style_group",
        "fwd_n",
        "fwd_complete",
        *RET_COLS,
    ]
    for c in keep_cols:
        if c not in events.columns:
            events[c] = pd.NA
    events = events[keep_cols].sort_values(["date", "stock_id"]).reset_index(drop=True)

    summary, warn = summarize(events, args.min_samples)
    n_complete = int(events["fwd_complete"].fillna(False).sum())
    print(f"events: {len(events)}  fwd_complete T+1..T+5: {n_complete}")
    print(f"limit_src official/approx: "
          f"{int((events['limit_src']=='official').sum())}/"
          f"{int((events['limit_src']=='approx_10pct').sum())}")
    print(f"FinMind api_calls this run (cache misses): {request_counter[0]}")
    if n_complete < len(events):
        print(
            f"WARNING: {len(events) - n_complete} events lack full T+5 prices "
            "(window end / halt / missing bar). Those rows stay in events.csv; "
            "each return column aggregates only its non-null sample."
        )
    for w in warn:
        print(f"WARNING: min-samples {w}")

    print_summary_tables(summary, events)

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    ev_csv = outdir / "events.csv"
    sm_csv = outdir / "summary_by_group.csv"
    events.to_csv(ev_csv, index=False)
    summary.to_csv(sm_csv, index=False)
    try:
        events.to_parquet(outdir / "events.parquet", index=False)
    except Exception as exc:  # noqa: BLE001
        print(f"(parquet skip: {exc})", file=sys.stderr)
    print(f"wrote {ev_csv}")
    print(f"wrote {sm_csv}")
    print("====================================")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
