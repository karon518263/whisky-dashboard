import sqlite3
import pandas as pd
from datetime import datetime, timedelta

DB_NAME = 'whisky_market.db'

def get_connection():
    return sqlite3.connect(DB_NAME, check_same_thread=False)

def format_standard_name(row):
    """組合標準品名: Brand + Year + Series + Style"""
    parts = [row['brand']]
    if pd.notna(row['year']) and row['year'] != 0:
        parts.append(f"{int(row['year'])}年")
    if pd.notna(row['series']) and row['series']:
        parts.append(row['series'])
    if pd.notna(row['style']) and row['style']:
        parts.append(row['style'])
    return " ".join(parts)

def search_whisky(keyword, days=30):
    """搜尋並回傳符合您指定欄位的資料"""
    conn = get_connection()
    # 這裡我們撈出所有需要的欄位：年份(year)、桶型(style)、賣家(author)、連結(post_url)
    sql = """
    SELECT p.post_date, w.brand, w.product_name, w.year, w.series, w.style, 
           w.price_per_bottle, p.author, p.post_url
    FROM whisky_prices w
    JOIN posts p ON w.post_id = p.id
    WHERE (w.product_name LIKE ? OR w.brand LIKE ?)
    AND p.post_date >= date('now', '-' || ? || ' days')
    ORDER BY p.post_date DESC
    """
    df = pd.read_sql(sql, conn, params=(f'%{keyword}%', f'%{keyword}%', days))
    conn.close()
    
    if not df.empty:
        # 1. 產生漂亮的標準品名
        df['標準品名'] = df.apply(format_standard_name, axis=1)
        # 2. 處理年份顯示 (把 0 或 NaN 變成空字串)
        df['year'] = df['year'].fillna(0).astype(int).astype(str).replace('0', '')
        # 3. 處理桶型顯示
        df['style'] = df['style'].fillna('')
    
    return df

# --- 其他輔助函式 (保持不變) ---
def get_dashboard_stats():
    conn = get_connection()
    stats = {'total_posts':0, 'total_brands':0, 'recent_posts':0}
    try:
        stats['total_posts'] = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
        stats['total_brands'] = conn.execute("SELECT COUNT(*) FROM brands").fetchone()[0]
        stats['recent_posts'] = conn.execute("SELECT COUNT(*) FROM posts WHERE post_date >= date('now', '-3 days')").fetchone()[0]
    except: pass
    conn.close()
    return stats

def get_latest_posts(limit=10):
    conn = get_connection()
    sql = """
    SELECT p.post_date as 日期, w.brand as 品牌, w.product_name as 品名, 
           w.price_per_bottle as 價格, p.author as 賣家
    FROM whisky_prices w
    JOIN posts p ON w.post_id = p.id
    ORDER BY p.post_date DESC LIMIT ?
    """
    df = pd.read_sql(sql, conn, params=(limit,))
    conn.close()
    return df

def get_top_brands_stats(days=30, limit=10):
    conn = get_connection()
    sql = """
    SELECT w.brand as 品牌, COUNT(*) as 貼文數, ROUND(AVG(w.price_per_bottle),0) as 平均價格
    FROM whisky_prices w JOIN posts p ON w.post_id = p.id
    WHERE p.post_date >= date('now', '-' || ? || ' days') AND w.brand != 'Other'
    GROUP BY w.brand ORDER BY 貼文數 DESC LIMIT ?
    """
    df = pd.read_sql(sql, conn, params=(days, limit))
    conn.close()
    return df