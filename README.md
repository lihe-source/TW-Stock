# 台股雷達 Stock Radar — V0.1

台股多因子篩選 PWA，整合技術面、基本面、籌碼面指標。

---

## 🚀 快速部署到 GitHub Pages

### 步驟 1：建立 Repository
1. 登入 GitHub，點右上角 **`+`** → **New repository**
2. Repository name 填寫（例如）：`taiwan-stock-radar`
3. 設為 **Public**
4. **不要**勾選 Add README（我們會上傳自己的）
5. 點 **Create repository**

### 步驟 2：上傳檔案
**方法 A：拖曳上傳（最簡單）**
1. 進入剛建立的 repo
2. 點 `uploading an existing file`
3. 將解壓縮後的所有檔案拖曳進去（包含 icons/ 資料夾）
4. 點 **Commit changes**

**方法 B：Git 指令**
```bash
git init
git add .
git commit -m "Initial commit: Stock Radar V0.1"
git branch -M main
git remote add origin https://github.com/你的帳號/taiwan-stock-radar.git
git push -u origin main
```

### 步驟 3：啟用 GitHub Pages
1. 進入 repo → **Settings** → **Pages**（左側選單）
2. Source 選 **Deploy from a branch**
3. Branch 選 **main**，資料夾選 **/ (root)**
4. 點 **Save**
5. 約 1~2 分鐘後，網址會顯示在頁面頂端

🌐 您的 PWA 網址：`https://你的帳號.github.io/taiwan-stock-radar/`

---

## 🔑 申請 FinMind API Token（必要）

大部分股票資料需要 FinMind API Token（免費）。

### 申請步驟：
1. 前往 https://finmindtrade.com
2. 點右上角 **「登入/註冊」** → **「立即加入」**
3. 填寫 Email、密碼完成註冊
4. 至收件匣點選驗證信
5. 登入後，點右上角頭像 → **「個人資料」**
6. 找到 **API Token** 欄位，複製 Token
7. 開啟台股雷達 App，前往 **設定** 頁，貼上 Token 並點 **驗證並儲存**

### 免費版限制：
| 項目 | 限制 |
|------|------|
| 每日 API 請求次數 | 約 300~600 次 |
| 資料更新頻率 | 收盤後更新 |
| 歷史資料深度 | 完整歷史 |

> 一次查詢約消耗 **3~5 次 API 請求/每檔股票**。
> 一次查詢 20 檔 ≈ 消耗 60~100 次請求。

---

## 📁 檔案結構

```
taiwan-stock-radar/
├── index.html          # 主頁面 (PWA 入口)
├── style.css           # 樣式（含深色/淺色主題）
├── app.js              # 主程式邏輯
├── manifest.json       # PWA 設定
├── sw.js               # Service Worker（離線快取）
├── icons/
│   ├── icon-192.svg    # App 圖示 192px
│   └── icon-512.svg    # App 圖示 512px
└── README.md           # 本說明文件
```

---

## 📊 功能說明

### 篩選條件

#### 技術面
| 指標 | 計算方式 |
|------|---------|
| RS 指標 > 90 | 個股 26 週漲幅 vs TAIEX 相對強弱，轉換為 0~100 分 |
| 距月高點 ≤ 5% | 現價 ÷ 近 22 交易日最高價 |
| 短期均線排列 | MA5 > MA10 > MA20 |
| 中長期均線排列 | MA20 > MA60 > MA120 (> MA240) |
| 站上扣抵值 | 現價 > 20日前股價 且 現價 > 60日前股價 |

#### 基本面
| 指標 | 資料來源 |
|------|---------|
| 營收創歷史/同期高 | FinMind TaiwanStockMonthRevenue |
| 年增率連2月 > 20% | FinMind 月營收計算 |
| 月增率連2月 > 20% | FinMind 月營收計算 |
| 毛利率/營益率較去年成長 | FinMind TaiwanStockFinancialStatements |
| 公司無虧損 | FinMind 財報淨利 > 0 |

#### 籌碼面
| 指標 | 資料來源 |
|------|---------|
| 籌碼集中度增加 | FinMind TaiwanStockShareholderStructure |
| 外資買超（近5日） | FinMind TaiwanStockInstitutionalInvestors |
| 投信買超（近5日） | FinMind TaiwanStockInstitutionalInvestors |
| 大戶持股比例增加 | FinMind TaiwanStockShareholderStructure |
| 法人持股創一季新高 | FinMind 法人資料計算 |

> ⚠️ **V0.1 限制**：「近期買賣家數差為負」需 TWSE 分點資料，此版本暫不支援。

---

## 🔄 版本記錄

| 版本 | 更新內容 |
|------|---------|
| V0.1 | 初始版本。實作 16 項篩選指標、自選股管理、深色/淺色主題切換 |

---

## ⚠️ 免責聲明

本工具提供之資料及分析結果**僅供參考**，不構成任何投資建議。
股票市場存在風險，投資決策請依個人判斷並自負盈虧。
