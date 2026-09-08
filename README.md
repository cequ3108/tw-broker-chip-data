# tw-broker-chip-data

台股「全市場個股分點買賣」籌碼資料倉。

## 資料來源（唯一）

全部由 **FinMind** 抓取，不是證交所頁面爬蟲：

- API：`GET https://api.finmindtrade.com/api/v4/taiwan_stock_trading_daily_report`
- 參數：`data_id={股票代碼}` + `date={YYYY-MM-DD}`（每股每日 1 request）
- 認證：環境變數 `FINMIND_TOKEN`（Sponsor；不可把 token 寫進 repo）
- 限制：Sponsor **不能**用整日 `storage_objects` 全市場 parquet（需 SponsorPro），因此採逐檔抓取 + 斷點續跑

輔助資料集（同樣 FinMind）：

- `TaiwanStockInfo`：上市櫃普通股清單
- `TaiwanStockTradingDate`：交易日曆

## 資料規格

- 路徑：`data/daily/YYYY-MM-DD.parquet`
- 欄位：`date, stock_id, securities_trader_id, securities_trader, buy, sell, buy_amt, sell_amt`
- 狀態：`state/checkpoint.json`、`state/latest.json`
- 大檔透過 Git LFS（見 `.gitattributes`）

## 執行

```bash
pip install -r requirements.txt
export FINMIND_TOKEN=***   # 勿寫入程式碼或 commit
python3 scripts/fetch_branch_daily.py --mode auto --max-requests 500
```

`auto` 會判斷：

- `data/daily/` 少於 5 個交易日 → `backfill`（約一年）
- 已有資料 → `daily`（補最近缺的交易日）
- 若 checkpoint 尚有 `pending_dates`／`in_progress_date` → 繼續未完成工作

**回補順序：由最近交易日往過去補**（newest → oldest），讓近半年先到位；同一天若已有 partial 會先做完再換日。

流量限制：FinMind Sponsor 約 600 req/hour；實務每輪最多約 500。  
一日全市場約 2000+ requests，需數小時／多次續跑。遇到 HTTP 429 會退避重試並保留 checkpoint。

## 排程建議

每個交易日 21:15（Asia/Taipei）執行；若 `state/checkpoint.json` 仍有 `in_progress_date` 或 `pending_dates`，下一小時／下次排程再跑同一指令即可。

## 分點建倉雷達

排除外資／自營後，掃描國內分點累計淨買超（`buy_amt - sell_amt`）：

```bash
python3 scripts/radar_branch_accumulation.py --min-net-yi 50
```

預設門檻 50 億、並排除總公司型簡稱（如「富邦」「凱基」）；若要連總公司帳戶一起看可加 `--include-hq`。  
資料覆蓋取決於 `data/daily/` 已回補天數。
