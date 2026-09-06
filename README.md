# tw-broker-chip-data

台股「全市場個股分點買賣」籌碼資料倉（FinMind）。

## 資料規格

- 路徑：`data/daily/YYYY-MM-DD.parquet`
- 欄位：`date, stock_id, securities_trader_id, securities_trader, buy, sell, buy_amt, sell_amt`
- 狀態：`state/checkpoint.json`、`state/latest.json`
- 大檔透過 Git LFS（見 `.gitattributes`）

## 執行

```bash
pip install -r requirements.txt
export FINMIND_TOKEN=***   # 勿寫入程式碼或 commit
python scripts/fetch_branch_daily.py --mode auto --max-requests 500
```

`auto` 會判斷：

- `data/daily/` 少於 5 個交易日 → `backfill`（約一年，可斷點續跑）
- 已有資料 → `daily`（補最近缺的交易日）

流量限制：FinMind Sponsor 約 600 req/hour；實務每輪最多約 500。  
一日全市場約 2100+ requests，需數小時／多次續跑。遇到 HTTP 429 會退避重試並保留 checkpoint。

## 排程建議

每個交易日 21:15（Asia/Taipei）執行；若 `state/checkpoint.json` 仍有 `in_progress_date` 或 `pending_dates`，下一小時／下次排程再跑同一指令即可。
