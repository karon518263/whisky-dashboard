@echo off
:: 宣告使用 UTF-8 編碼，解決中文亂碼問題
chcp 65001

echo === [1/2] 開始執行每日威士忌爬蟲與清洗 ===

:: 切換到專案資料夾 (請確認您的實際路徑)
D:
cd "D:\新增資料夾\課程\P9_project"

:: 執行爬蟲主程式
python run_all.py

echo === [2/2] 準備將最新資料庫上傳到 GitHub ===

:: 將更新後的資料庫加入上傳清單
git add .

:: 幫這次的上傳寫個標記 (包含日期與時間)
git commit -m "Auto update DB %date% %time%"

:: 推送上 GitHub 雲端
git push origin main

echo === 全部任務完成！儀表板即將自動更新 ===
:: 暫停 10 秒讓您看清楚結果再自動關閉視窗
timeout /t 10