import os
import time
from datetime import datetime

# 取得現在時間
now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(f"[{now}] === 開始執行每日威士忌爬蟲任務 ===")

# 1. 執行爬蟲
print("\n>>> 步驟 1/3: 執行爬蟲 (P9_whiskey_project)...")
os.system("python P9_whiskey_project.py")

# 2. 執行清洗
print("\n>>> 步驟 2/3: 執行清洗 (cleaning_demo)...")
os.system("python cleaning_demo.py")

# 3. 匯入資料庫
print("\n>>> 步驟 3/3: 匯入資料庫 (db_manager)...")
os.system("python db_manager.py")

print("\n✅ 所有作業完成！資料已更新至 whisky_market.db")
time.sleep(5)