P9 Whisky Secondary Market Monitor
威士忌二級市場行情自動化監測儀表板
這是一個針對「P9 品酒網」論壇開發的自動化數據專案 。本專案整合了資料採集、清洗 、存儲  與視覺化的完整數據管線，實現「輸入酒名，一眼看漲跌」的終端應用目標 。

雲端部署連結
[點擊此處查看即時儀表板](https://p9-whisky-monitor.streamlit.app/)

任務目標
使用 Streamlit 開發互動式威士忌行情查詢儀表板，整合爬蟲與資料庫成果，提供快速查詢酒款行情、價格趨勢與品牌熱門度之功能 。

核心功能需求

儀表板總覽 (首頁)：顯示總貼文數、活躍品牌數等關鍵數字，並列出最新 100 筆市場報價快覽 。
酒款行情查詢：支援關鍵字（如「麥卡倫」、「蘇格登12」）搜尋，並統計該酒款之最高價、最低價與平均價格 。
價格趨勢視覺化：透過互動式圖表顯示過去 6 個月的價格走勢，標註平均價格線與最新報價 。
熱門品牌排行榜：統計最近 30 天內 Top 10 熱門品牌，分析聲量佔比與平均價格 。

技術架構與檔案說明
本專案程式架構嚴謹，各模組職責分明 ：
app.py：Streamlit 互動介面主程式，負責頁面佈局與資料呈現 。
db_utils.py：封裝資料庫連線與 SQL 查詢函式，包含 search_whisky 與 get_dashboard_stats 等核心邏輯 。
P9_whiskey_project.py：自動化爬蟲腳本，負責從 P9 論壇抓取原始成交數據。
cleaning_demo.py：資料清洗程式，進行特徵提取（年份、系列、桶型）與價格標準化。
whisky_market.db：最終儲存的 SQLite 資料庫檔案 。
requirements.txt：系統依賴套件清單（包含 streamlit, pandas, plotly 等）

若需在本地環境執行，請確保已安裝 Python 環境，並依序執行以下步驟 ：
複製專案：
git clone https://github.com/karon518263/whisky-dashboard.git
安裝套件：
pip install -r requirements.txt
啟動儀表板：
streamlit run app.py
