import sqlite3
import pandas as pd
import os

# --- 設定區 ---
DB_NAME = 'whisky_market.db'          # 最終儲存的資料庫
SOURCE_DB = 'P9_whiskey_project.db'   # 來源資料庫

def init_db():
    """初始化目標資料庫結構 (新增 series 和 style 欄位)"""
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        # 1. 建立 posts 表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            post_date TEXT,
            title TEXT,
            author TEXT,
            post_url TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );''')
        
        # 2. 建立 whisky_prices 表 (★注意：這裡增加了 series 和 style)
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS whisky_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            post_id INTEGER,
            brand TEXT,
            product_name TEXT,
            year INTEGER,
            series TEXT,     -- 新增欄位
            style TEXT,      -- 新增欄位
            price_per_bottle INTEGER,
            FOREIGN KEY (post_id) REFERENCES posts(id)
        );''')

        # 3. 建立 brands 表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS brands (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            brand_name TEXT UNIQUE,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );''')
        
        conn.commit()
        print('資料表結構建立/檢查完成。')
        conn.close()
    except Exception as e:
        print(f"資料庫初始化失敗: {e}")

def import_data():
    """從 P9 資料庫匯入資料 (包含系列與桶型)"""
    if not os.path.exists(SOURCE_DB):
        print(f"❌ 找不到來源資料庫 {SOURCE_DB}，請先執行清洗程式。")
        return

    try:
        conn_source = sqlite3.connect(SOURCE_DB)
        # 讀取清洗好的資料表 cleaned_data
        df = pd.read_sql("SELECT * FROM cleaned_data", conn_source)
        conn_source.close()
        print(f"📥 成功從 {SOURCE_DB} 讀取 {len(df)} 筆清洗資料")
    except Exception as e:
        print(f'❌ 讀取來源資料表失敗: {e}')
        return
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    posts_added = 0
    prices_added = 0

    try:
        print("開始寫入資料庫...")
        cursor.execute("BEGIN TRANSACTION;")

        for index, row in df.iterrows():
            post_url = row.get('賣場連結', '')
            post_date = row.get('日期', '')
            title = row.get('title', '')
            author = row.get('賣家', 'Unknown')

            # 處理貼文
            cursor.execute('SELECT id FROM posts WHERE post_url = ?', (post_url,))
            result = cursor.fetchone()
            if result:
                post_id = result[0]
            else:
                cursor.execute('''
                    INSERT INTO posts (post_date, title, author, post_url)
                    VALUES (?, ?, ?, ?)
                ''', (post_date, title, author, post_url))
                post_id = cursor.lastrowid
                posts_added += 1

            # 處理品牌
            brand_name = row.get('品牌', 'Other')
            cursor.execute("INSERT OR IGNORE INTO brands (brand_name) VALUES (?)", (brand_name,))

            # 處理價格與特徵 (★注意：這裡要把清洗好的系列跟桶型抓出來)
            price = row.get('價格', 0)
            product_name = row.get('product_name', '') # 這是原始品名，留著當備份
            
            # ★ 關鍵修改：抓取清洗後的特徵
            series = row.get('系列', '')
            style = row.get('桶型', '')
            
            # 處理年份
            year_val = row.get('年份', None)
            year_db = None
            try:
                if pd.notna(year_val) and str(year_val).replace('.0','').isdigit():
                    year_db = int(float(year_val))
            except:
                pass

            # 寫入價格表 (★注意：這裡要存入 series 和 style)
            cursor.execute('''
                INSERT INTO whisky_prices (post_id, brand, product_name, year, series, style, price_per_bottle)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (post_id, brand_name, product_name, year_db, series, style, price))
            prices_added += 1

        conn.commit()
        print(f"✅ 匯入成功！新增 {posts_added} 篇貼文，{prices_added} 筆報價。")

    except Exception as e:
        print(f"匯入過程發生錯誤: {e}")
        conn.rollback()
    finally:
        conn.close()

def show_stats():
    if not os.path.exists(DB_NAME): return
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        for table in ['posts', 'whisky_prices', 'brands']:
            try:
                count = cursor.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                print(f"{table:<15} | {count}")
            except: pass

if __name__ == '__main__':
    init_db()     
    import_data()
    show_stats()