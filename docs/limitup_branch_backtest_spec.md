# 收盤鎖漲停 × 分點（broker-branch）回測規格

本文件與 `scripts/backtest_limitup_branch.py` **一一對應**。這是關聯性回測（associative backtest），**不是因果證明**。

## 1. 目的

在「收盤鎖漲停」事件上，觀察國內券商分點籌碼特徵（鎖倉型／動能型／雙主力／當日淨買龍頭）與後續報酬的關聯。

不推論新聞內容；收盤鎖漲停 ≠ 盤中封關強度（本回測沒有未成交買盤、內外盤或逐筆資料）。

## 2. 宇宙與資料

### 2.1 股票宇宙

與 `scripts/fetch_branch_daily.py` 的 `fetch_stock_universe` 相同：

- FinMind `TaiwanStockInfo`，取有效 `YYYY-MM-DD` 列中**列數最多的快照日**
- `type` ∈ {`twse`, `tpex`}（大小寫不敏感）
- `stock_id` 為 4 位數字（普通股）
- 排除產業／名稱含：`ETF|ETN|權證|牛證|熊證|特別股|受益憑證|存託憑證`

上市／上櫃別寫入事件列的 `market`（`twse` / `tpex`）。

### 2.2 籌碼樣本

- **只使用** `data/daily/YYYY-MM-DD.parquet` 已存在的日期（chip dates）
- 欄位：`date, stock_id, securities_trader_id, securities_trader, buy, sell, buy_amt, sell_amt`
- 事件日 T 必須是 chip date；T 當天沒有該檔分點列時，事件仍保留，但 `chip_present=false`，分點特徵為空／0／false
- 鎖倉／動能的回看窗是「樣本內、≤ T 的最近 N 個 **chip date**」，不是完整交易日曆。樣本缺口會拉長日曆跨度——腳本會印出交易日缺口警告

已知缺口（寫規格時）：約 2025-09-18～2025-11-24 籌碼未齊；2026-02-11～2026-02-23 多為農曆年休市，以交易日曆為準。

### 2.3 價格與漲停價

- 日線：FinMind `TaiwanStockPrice`（OHLCV：`open, max, min, close`）
- 官方漲跌停：FinMind `TaiwanStockPriceLimit`（`reference_price, limit_up, limit_down`）
- Sponsor 可用「不帶 `data_id`、只帶當日 `start_date=end_date`」一次抓全市場
- 需環境變數 `FINMIND_TOKEN`（Sponsor）；**禁止**把 token 寫進 repo
- 未設 token 且非 `--dry-run`：以明確錯誤結束
- `--dry-run`：只報告籌碼覆蓋，不打價格 API
- 快取：`data/cache/price/TaiwanStockPrice/YYYY-MM-DD.parquet`、`.../TaiwanStockPriceLimit/`、`.../TaiwanStockInfo.parquet`（已 gitignore）
- 尊重 429／quota：指數退避；請求間預設 sleep，避免燒 token

價格區間：事件窗之前再往前抓 `--consec-pad` 個交易日（連漲停計數），事件窗之後再抓足夠交易日以計算 T+1…T+5。

## 3. 事件定義：收盤鎖漲停

一個「股票 × 日」在同時滿足時為事件：

1. 該日為 chip date，且股票在宇宙內
2. T 日 `close`、`limit_up`（或後備近似）皆可計算
3. **收盤鎖漲停**：`close >= limit_up * 0.999`  
   （0.999 為浮點／tick 容差，台股實務上常用的「視為碰到漲停價」寬鬆比對）

這是 **收盤鎖在漲停價**，不是盤中觸及後打開。腳本另記 `touched_limit_up`（`high >= limit_up * 0.999`）僅供對照，**不列入事件**。

### 3.1 漲停價來源

1. **優先**：`TaiwanStockPriceLimit.limit_up > 0` → `limit_src=official`
2. **後備**：官方缺或為 0（無漲跌幅限制、部分新股／處置／資料洞）→  
   `limit_up ≈ tick_round(prev_close * 1.10)`，`limit_src=approx_10pct`  
   tick 依現行上市櫃價位表：`<10→0.01`, `<50→0.05`, `<100→0.10`, `<500→0.50`, `<1000→1`, `≥1000→5`，四捨五入到 tick
3. 連 `prev_close` 都沒有：不列為事件（`price` 在 T 不完整）

後備 **不會**重現處置股收窄幅度、創新板／上市初期特殊規定；能抓到官方價時應以官方為準。

### 3.2 缺價處理

- T 缺收盤／無法定漲停價：不是事件
- T+1…T+5 缺日線：事件仍輸出，對應報酬為空，`fwd_n` = 有價的期數，`fwd_complete=true` 僅當 T+1…T+5 收盤皆在
- 各報酬欄的彙總只用該欄非空的事件（T+1 統計不要求 T+5 齊）

## 4. 分點過濾（與現有雷達一致）

與 `radar_lock_chip.py` / `radar_momentum_branch.py` / `radar_branch_accumulation.py` 相同：

**排除（視為外資／自營／法人通道，非國內營業分點）：**

- `securities_trader_id` 以 `T` 結尾（國內自營）
- 已知外資券商代號：`1360,1440,1470,1480,1520,1560,1570,1590,1650,8440,8890,8900,8960`
- 名稱符合：`自營|投信|摩根|瑞銀|花旗|美林|高盛|野村|麥格理|法銀|港商|美商|新加坡商|大和國泰|上海匯豐|匯豐|渣打|巴克萊|德意志|法國興業|瑞士信貸|星洲瑞銀`

**總公司型簡稱（預設排除，`--include-hq` 才保留）：**

去掉 `-`／`－` 後，名稱完全等於：  
`元大|富邦|凱基|永豐金|統一|國泰綜合|國票|兆豐|群益|台新|玉山|宏遠|康和|福邦|第一金|合庫|華南永昌|致和|大昌|台灣匯立`

**匯立：**

- 名稱含「匯立」→ `top1_is_huili` 等旗標
- 電子帳號常混有機構／演算法流量，**單獨出現不視為強訊號**
- `--exclude-huili`：從 Top1/Top3、鎖倉、動能的分點宇宙中移除匯立（旗標仍依當日未排除前的 Top1 名稱判斷）

淨額：`net_amt = buy_amt - sell_amt`，單位「億」= `/ 1e8`。

## 5. 事件日 T 的分點特徵

回看預設 `--lookback 20`（樣本內 chip dates，含 T）。

### 5.1 國內分點淨買龍頭（僅 T 當日）

在過濾後的國內分點中，按 `net_amt` 排序：

- `top1_net_yi`、`top1_trader`、`top1_trader_id`
- `top3_net_yi` = 前三名淨買合計（不足三名則合計現有）
- 二元：`top1_ge_lo`（預設 ≥ 0.5 億，`--top1-flag-yi`）、`top1_ge_hi`（預設 ≥ 1 億，`--top1-threshold-yi`）
- `top1_is_huili`

### 5.2 鎖倉型（`has_lock_chip_branch`）

精神對齊 `radar_lock_chip.py`，但窗為 **結束於 T 的 lookback 個 chip dates**（雷達則是掃描整段 `--start/--end`）。

任一國內分點同時滿足則該股 T 日為 true：

| 條件 | 預設 | CLI |
| --- | --- | --- |
| 窗內累計淨買 | ≥ 2 億 | `--lock-min-net-yi`（短於雷達預設 10 億，因窗較短） |
| 出現天數 | ≥ 5 | `--lock-min-days` |
| 買占比 `buy_amt/(buy_amt+sell_amt)` | ≥ 0.65 | `--lock-min-buy-ratio` |
| 賣／買比 `sell_amt/buy_amt` | ≤ 0.40 | `--lock-max-sell-to-buy` |
| 有成交日中淨買日占比 | ≥ 0.60 | `--lock-min-net-day-ratio` |
| 低檔買占比（可選） | ≥ 0.50 | `--lock-min-low-buy-share` |

低檔日：窗內該股收盤 ≤ 窗內收盤的 `--lock-low-pct` 分位數（預設 0.40）。無日線的日子不進入分母。若窗內完全無價，略過低檔條件（不因缺價而否決，`lock_low_buy_share` 為空）。

鎖倉分數（僅記錄，不另設門檻）：

`0.35*買占比 + 0.25*淨買日占比 + 0.20*(1-clip(賣買比,0,1)) + 0.20*低檔買占比(缺則 0)`

事件列記錄分數最高的符合分點：`lock_branch*`。

### 5.3 動能型（`has_momentum_branch`）

精神對齊 `radar_momentum_branch.py`，但 **在 T 打標時不看 T 之後的賣出**（避免報酬洩漏）。

- **衝量日**：單日 `net_amt >= --impulse-yi` 億（預設 1.0）
- 衝量發生在 **T 或 T 之前 lookback 個 chip dates 內**
- **強勢日**（衝量當日）：`ret = close/prev_close - 1 >= --big-red-ret`（預設 3%）且 `close > open`；**或**該日本身為收盤鎖漲停（含「開在漲停、收也在漲停」）
- **不急賣（sticky）到 T**：衝量日 S 之後、至 T 為止的 chip dates，最多 `--fwd-days` 日（預設 3）  
  - `flip = 其間 sell_amt 合計 / 衝量日 net_amt`  
  - sticky iff `flip <= --max-flip`（預設 0.35）且 `其間 net_amt >= -0.2 * 衝量日 net_amt`  
  - **S = T**：沒有「到 T 為止」的後續日，視為 sticky（不使用 T+1… 的分點資料）

該股任一國內分點有「衝量 + 強勢日 + sticky」→ `has_momentum_branch`。記錄最近一次符合的衝量（同日取淨買最大）：`mom_branch*`, `mom_impulse_date`。

**與 Top1 的包含關係（預設參數）：** 當 `--impulse-yi` 等於 `--top1-threshold-yi`（皆 1 億）時，T 日 Top1 淨買 ≥ 1 億 ⇒ 該分點在 T 有衝量，T 又是收盤鎖漲停（強勢日），且 S=T 視為 sticky。因此 `top1_ge_hi` 是 `has_momentum_branch` 的子集；交叉表 `top1_ge_1yi_x_mom / yes_no` 會是空的。這是定義使然，不是實作錯誤。動能旗標仍可能來自 T 之前 lookback 內的衝量（此時 Top1 可以 < 1 億）。

雷達全窗的「至少 2 個衝量日、窗內累計淨買 ≥ 5 億」等 **不用於事件打標**（那些是全窗掃描門檻）。

### 5.4 雙主力（`dual_main` / `style_group`）

與動能雷達「同股同時存在鎖倉型與動能型」相同，**允許不同分點**：

| `style_group` | 定義 |
| --- | --- |
| `dual` | `has_lock_chip_branch` 且 `has_momentum_branch` |
| `single_lock` | 僅鎖倉 |
| `single_mom` | 僅動能 |
| `neither` | 皆無 |

`dual_main` = (`style_group == dual`)。

### 5.5 其他控制

- `market`：twse / tpex
- `top1_is_huili`：見 §4
- `consec_limit_up`：在**價格交易日曆**上，含 T、往回連續收盤鎖漲停的天數（受 `--consec-pad` 與快取區間截斷；至少為 1）

## 6. 報酬（結果）

**主方案：以 T 收盤為買入基準**（不是 T+1 開盤買入）。  
T+1 開盤報酬衡量隔夜跳空（常再鎖或開低），與「能不能在 T 收盤成交」是不同問題；漲停收盤未必買得到，解讀時必須當成限制。

對每個事件：

| 欄位 | 定義 |
| --- | --- |
| `ret_t1_open` | `open_{T+1} / close_T - 1` |
| `ret_t1_close` | `close_{T+1} / close_T - 1` |
| `ret_t2_close` | `close_{T+2} / close_T - 1` |
| `ret_t3_close` | `close_{T+3} / close_T - 1` |
| `ret_t5_close` | `close_{T+5} / close_T - 1` |
| `mfe_t5` | `max(high_{T+1..T+k}) / close_T - 1`（有價的 k≤5） |
| `mae_t5` | `min(low_{T+1..T+k}) / close_T - 1` |

T+n 是價格交易日曆上 T 之後第 n 個交易日，**不要求**該日也有籌碼 parquet。

### 6.1 分組彙總

對每個（group, subgroup）與每個報酬欄（非空樣本）：

- `n`：該欄非空列數
- `win_rate`：報酬 `> 0` 的比例
- `mean`, `median`, `p25`, `p75`

`n < --min-samples`（預設 20）印警告，列仍寫入。

### 6.2 比較組（刻意不交叉爆炸）

1. `all` / `all` — 基線
2. `top1_ge_1yi` / `yes` vs `no`（門檻 = `--top1-threshold-yi`）
3. `has_lock_chip_branch` / `yes` vs `no`
4. `has_momentum_branch` / `yes` vs `no`
5. `style_group` / `dual` vs `single`（`single_lock`+`single_mom`）vs `neither`；另列 `single_lock`、`single_mom`
6. 可讀交叉：`top1_ge_1yi × lock`、`top1_ge_1yi × mom`
7. 控制：`top1_is_huili` / `yes` vs `no`；`market` / `twse` vs `tpex`

## 7. 輸出

- stdout：覆蓋警告 + 分組表
- `output/limitup_branch_backtest/events.csv`（及 `.parquet`）
- `output/limitup_branch_backtest/summary_by_group.csv`

CLI 範例：

```bash
export FINMIND_TOKEN=***   # 勿 commit
python3 scripts/backtest_limitup_branch.py --start 2026-01-01 --end 2026-09-11
```

常用旗標：`--lookback`, `--impulse-yi`, `--top1-threshold-yi`, `--top1-flag-yi`, `--include-hq`, `--exclude-huili`, `--min-samples`, `--dry-run`, `--outdir`。

## 8. 限制（必須在解讀時一併陳述）

- **非因果**：分點淨買可能與題材、指數、融資同步出現
- **成交可行性**：T 收盤鎖漲停時市價買入常買不到；報酬是標的路徑，不是可執行策略 PnL
- **封關強度未知**：無五檔、未成交買量、內盤外盤
- **樣本缺口**：籌碼不是完整交易日曆；lookback 會跳過缺失日
- **匯立／總公司**：通道混雜，預設不當成「營業員分點」
- **官方漲停價優先**；10% 後備會誤標特殊幅度股票
- 宇宙隨 `TaiwanStockInfo` 最新快照，歷史下市／變更板塊可能偏差

## 9. 成功標準

- 本規格與腳本行為一致
- 在已有 `data/daily/` + 有效 `FINMIND_TOKEN` 時可跑完整區間
- 資料稀疏時仍只跑有檔日期，並印覆蓋警告
- repo 內無 token
