鎖漲停明細交接（算漲停明細）
================================
覆蓋: 2026-06-29 ～ 2026-08-25（41 個籌碼日且有日價快取）
事件數: 1861  優先子集: 1569

檔案
- lockup_journal_handoff.csv : 每個收盤鎖漲停一列
- lockup_journal_priority.csv : Top1 在追蹤名單，或同股在窗內多次鎖漲停
- tracklist_branches.json : 追蹤分點與列入原因

lock_state
- 全部為 locked = 日線收盤價達到／視為漲停價（close >= limit_up * 0.999）
- 這是「收盤鎖漲停」，不是盤中打開再鎖、也不是五檔封關
- 沒有逐筆／五檔，故 open_count_today = NA
- seal_note 固定 daily_close_lock_only; no_orderbook_seal

近似（不是真正的打開再鎖）
- prior_day_not_locked: 價格日曆上前一日未收盤鎖漲停（consec_limit_up==1，首板）
- multi_day_lock: 同一 stock_id 在本窗內有 ≥2 個收盤鎖漲停日（不是盤中開鎖循環）

報酬
- ret_t1_open / ret_t1_close 相對 T 收盤；T 收盤鎖漲停通常買不到，不可當實戰 PnL

風格旗標 style_flags
- 對齊 scripts/backtest_limitup_branch.py：lock_chip / momentum / dual / none
- lookback=20 個籌碼日；動能在 T 打標不看 T 之後賣出

請勿把本資料當成盤中封單或打開再鎖日誌。
