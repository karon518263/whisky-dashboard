# 🥃 P9 威士忌二級市場觀測系統 (P9 Whisky Secondary Market Monitor)

> **將非結構化的混亂資訊，轉化為具備量化價值的市場行情指標。**
> 基於 Python 與 Streamlit 打造的威士忌二級市場即時觀測儀表板。

**作者**：張凱傑
**專案版本**：v1.0

## 🌟 快速導覽 (Quick Links)
- 🚀 **[Streamlit 雲端即時展示](https://p9-whisky-monitor.streamlit.app/)**
- 🎬 **[2 分鐘系統操作展示影片](./demo_video.mp4)**
- 📄 **[完整專案技術報告 ](./docs/P9_Whisky_Market_Report_v1.0.pdf)**
- 📊 **[專案展示簡報](./docs/P9_Whisky_Demo_v1.0.pptx)**

---

## 🏗️ 專案目錄結構 (Project Structure)
本專案嚴格遵守模組化開發原則，目錄結構如下：

```text
whisky-market-system/
├── README.md                      # 專案說明與一鍵部署指南 (本文件)
├── docs/                          # 成果展示文件區
│   ├── P9_Whisky_Market_Report_v1.0.pdf  # 完整專案報告
│   └── P9_Whisky_Demo_v1.0.pptx          # 展示簡報
├── src/                           # 核心程式碼原始檔
│   ├── crawler/                   # P9 論壇爬蟲模組 (Requests + BS4)
│   ├── cleaning/                  # 資料清洗與 Regex 正規化模組 (Pandas)
│   ├── database/                  # SQLite 資料庫操作與防重複寫入機制
│   └── dashboard/                 # Streamlit 儀表板前端介面
├── data/                          # 資料庫與靜態檔案
│   └── whisky_market.db           # 已經過清洗與正規化的威士忌報價資料庫
├── demo_video.mp4                 # 2分鐘系統操作錄影驗證
└── requirements.txt               # 系統環境依賴套件清單

一鍵部署與本機執行說明 (Local Deployment)
本專案已內建真實報價的 SQLite 資料庫（whisky_market.db），評審委員無須重新執行爬蟲即可直接啟動儀表板檢視成果。

請打開您的終端機 (Terminal)，並依序執行以下 3 行指令：

Step 1: 複製專案與進入目錄
git clone https://github.com/karon518263/whisky-dashboard.git
cd whisky-dashboard

Step 2: 安裝依賴套件
pip install -r requirements.txt

Step 3: 啟動 Streamlit 儀表板
streamlit run src/dashboard/app.py

核心技術亮點
1.全自動化數據管線 ：實現從爬取、清洗、關聯儲存到動態展示的端到端流程。

2.精準的正則清洗演算法 ：解決 P9 論壇長年以來報價格式混亂的問題，精準萃取品牌、年份與報價。

3.資料一致性防護 ：於後端 SQLite 實作嚴格的防重複寫入邏輯，避免資料庫無效膨脹。

4.互動式商業洞察 ：前端採用 Streamlit 搭配 Plotly，提供「四維交叉搜尋」與「動態懸停價格走勢圖」，協助使用者精準判斷市場行情。
