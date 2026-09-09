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
python3 scripts/fetch_branch_daily.py --mode auto --max-requests 5500
```

`auto` 會判斷：

- `data/daily/` 少於 5 個交易日 → `backfill`（約一年）
- 已有資料 → `daily`（補最近缺的交易日）
- 若 checkpoint 尚有 `pending_dates`／`in_progress_date` → 繼續未完成工作

**優先順序（重要）**：台北時間約 **21:00 後**，若「最新可抓交易日」（通常是今天）還沒進 `data/daily/`，會**插到佇列最前面先抓完**，再繼續歷史回補。21:00 前則以昨天為最新可抓日。歷史日若被暫時打斷，`state/partial_*.parquet` 可續跑。

**回補順序：由最近交易日往過去補**（newest → oldest），讓近半年先到位。

流量限制：FinMind token 約 **6000 req/hour**；實務每輪預設 **5500**（預留約 500 次給你臨時查詢）。  
實測約 3.5～4 req/秒 → 5500 次大約 **25 分鐘**打完，然後等小時配額回復再續跑。  
一日全市場約 2000 req（目前宇宙約 1968 檔）→ 理論上 **每小時可補約 2～3 個交易日**。遇到 HTTP 429 會退避重試並保留 checkpoint。

## 排程建議

每個交易日 **21:15（Asia/Taipei）** 先跑一輪（優先抓當日）；若仍有歷史 `pending_dates`，之後每小時再續跑同一指令即可。

## 分點建倉雷達

排除外資／自營後，掃描國內分點累計淨買超（`buy_amt - sell_amt`）：

```bash
python3 scripts/radar_branch_accumulation.py --min-net-yi 50
```

預設門檻 50 億、並排除總公司型簡稱（如「富邦」「凱基」）；若要連總公司帳戶一起看可加 `--include-hq`。  
資料覆蓋取決於 `data/daily/` 已回補天數。

### 低檔鎖倉型分點（買多賣少、長時間淨買、建倉≥10億）

```bash
python3 scripts/radar_lock_chip.py --min-net-yi 10 --start 2026-07-29 --end 2026-09-04
```

額外條件：買進金額占比高、賣／買比低、多數有成交日為淨買、且多數買進落在該股區間內相對低檔日（需 `FINMIND_TOKEN` 抓日線）。  
「匯立」等電子交易帳戶可能混有機構／演算法流量，解讀時請與一般營業分點分開看。

### 動能型分點（大買常伴長紅、且短天不急賣）＋雙主力

```bash
python3 scripts/radar_momentum_branch.py --start 2026-07-29 --end 2026-09-04 --exclude-huili
```

定義「衝量日」為單日淨買 ≥1 億；若當日常走出長紅（收漲 ≥3%），且後 3 個交易日不大量倒貨，則視為動能型。  
同股若另有鎖倉型分點，腳本會列出「雙主力」配對。
