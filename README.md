# 台股雷達 Stock Radar — V1.0

台股多因子篩選 PWA，完全免費，部署在 GitHub Pages。

**資料來源（方案 E，完全免費）：**
| 用途 | 來源 |
|---|---|
| K 線歷史（技術指標） | Yahoo Finance（`yfinance`） |
| 月營收 | MOPS 公開資訊觀測站 |
| 今日三大法人 | TWSE Open API (T86) |
| 今日收盤行情 | TWSE Open API (DAY_ALL) |

---

## 🚀 完整設定教學

### 前置：建立 GitHub Repo

1. 登入 [github.com](https://github.com)
2. 右上角 **`+`** → **New repository**
3. Repository name：填 `taiwan-stock-radar`
4. 設為 **Public**（免費版 Pages 需要）
5. **不要**勾選「Add a README」
6. 點 **Create repository**

---

### 步驟一：上傳所有檔案

**方法 A：網頁上傳（最簡單）**

1. 進入 repo 頁面，點 **「uploading an existing file」**
2. 解壓縮 `taiwan-stock-radar-v1.0.zip`
3. 將所有檔案拖曳上傳

   > ⚠️ **重要**：`.github` 資料夾（點開頭）在 Mac/Windows 預設隱藏。
   > - **Mac**：Finder 按 `Command+Shift+.` 顯示隱藏檔案
   > - **Windows**：資料夾選項 → 勾選「顯示隱藏的項目」
   >
   > 請確認上傳了 `.github/workflows/update_data.yml`，
   > 否則 GitHub Actions 不會出現。

4. Commit message 填 `Initial commit V1.0` → **Commit changes**

**方法 B：Git 指令（進階）**
```bash
cd taiwan-stock-radar
git init
git add -A          # -A 包含隱藏資料夾
git commit -m "Initial commit V1.0"
git branch -M main
git remote add origin https://github.com/你的帳號/taiwan-stock-radar.git
git push -u origin main
```

---

### 步驟二：啟用 GitHub Pages

1. Repo → **Settings** → 左側 **Pages**
2. Source → **Deploy from a branch**
3. Branch → **`main`**，資料夾 → **`/ (root)`**
4. **Save**
5. 約 1~2 分鐘後出現你的網址：
   `https://你的帳號.github.io/taiwan-stock-radar/`

---

### 步驟三：手動執行第一次建置

1. Repo → **Actions** 分頁
2. 左側選「**每日更新台股資料**」
3. 右側 **Run workflow** → **Run workflow**
4. 等待約 **10~20 分鐘**
5. 出現綠色 ✅ 代表成功

建置完成後：
- `data/screener.json` 自動更新
- ~1 分鐘後 PWA 取得最新資料
- 日後每個交易日 **14:30** 自動執行

---

## 📊 篩選指標說明

### 技術面（yfinance K 線）
| 指標 | 計算方式 |
|---|---|
| RS > 90 | 個股 vs 0050（大盤代理）26 週相對強度，99分制 |
| 距月高 ≤ 5% | 現價距近 22 日最高點 |
| 短均排列 | MA5 > MA10 > MA20 |
| 中長均排列 | MA20 > MA60 > MA120 |
| 站上扣抵值 | 現價 > 20日前 且 > 60日前股價 |

### 基本面（MOPS 月營收）
| 指標 | 說明 |
|---|---|
| 營收創高 | 歷史最高 或 > 去年同月 |
| 年增率連2月 > 20% | 近兩月 YoY ≥ 20% |
| 月增率連2月 > 20% | 近兩月 MoM ≥ 20% |
| 毛利/營益率成長 | 需啟用 ENABLE_FINANCIALS（選用） |
| 無虧損 | 需啟用 ENABLE_FINANCIALS（選用） |

### 籌碼面（TWSE T86 今日）
| 指標 | 說明 |
|---|---|
| 外資買超 | 今日外資淨買超 > 0 |
| 投信買超 | 今日投信淨買超 > 0 |
| 其餘籌碼指標 | 無免費來源，顯示 N/A |

---

## 🔧 進階選項

### 啟用財報（毛利率/營益率）
手動 Run workflow 時：「啟用 MOPS 財報」選 **true**
（約多 20~40 分鐘，逐支公司爬取）

### 本機測試
```bash
pip install requests yfinance beautifulsoup4 lxml

# 只跑 50 支（快速測試）
STOCK_LIMIT=50 python3 scripts/build_data.py

# 完整執行
python3 scripts/build_data.py
```

---

## 📁 檔案結構

```
taiwan-stock-radar/
├── .github/workflows/update_data.yml  ← Actions 排程（關鍵）
├── data/screener.json                 ← 預計算資料
├── scripts/build_data.py              ← Python 建置腳本
├── index.html  app.js  style.css
├── sw.js  manifest.json
└── icon-192.svg  icon-512.svg
```

---

## ❓ 常見問題

**Q：Actions 沒出現「每日更新台股資料」？**
A：表示 `.github/workflows/update_data.yml` 沒有上傳成功。請確認隱藏資料夾已顯示並重新上傳。

**Q：非交易日執行怎樣？**
A：TWSE 無行情，但 yfinance 仍提供歷史 K 線，技術指標正常計算。

**Q：Action 逾時失敗？**
A：直接重跑即可（Run workflow 再觸發一次）。

---

## ⚠️ 免責聲明

本工具僅供參考，不構成投資建議。股市有風險，投資決策請自行判斷。
