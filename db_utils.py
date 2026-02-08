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

def get_dashboard_stats():
    """取得儀表板首頁的關鍵數字"""
    conn = get_connection()
    stats = {'total_posts':0, 'total_brands':0, 'recent_posts':0}
    try:
        stats['total_posts'] = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
        stats['total_brands'] = conn.execute("SELECT COUNT(*) FROM brands").fetchone()[0]
        stats['recent_posts'] = conn.execute("SELECT COUNT(*) FROM posts WHERE post_date >= date('now', '-3 days')").fetchone()[0]
    except: pass
    conn.close()
    return stats

def get_latest_posts(limit=100):
    """
    取得最新 N 筆報價 (預設改成 100)
    並回傳完整欄位以便前端製作超連結與詳細表格
    """
    conn = get_connection()
    sql = """
    SELECT p.post_date, w.brand, w.product_name, w.year, w.series, w.style, 
           w.price_per_bottle, p.author, p.post_url
    FROM whisky_prices w
    JOIN posts p ON w.post_id = p.id
    ORDER BY p.post_date DESC, p.id DESC
    LIMIT ?
    """
    df = pd.read_sql(sql, conn, params=(limit,))
    conn.close()
    
    if not df.empty:
        # 資料處理：產生標準品名、處理年份與空值
        df['標準品名'] = df.apply(format_standard_name, axis=1)
        df['year'] = df['year'].fillna(0).astype(int).astype(str).replace('0', '')
        df['style'] = df['style'].fillna('')
        df['series'] = df['series'].fillna('')
    
    return df

def search_whisky(keyword, days=30):
    """搜尋並回傳符合您指定欄位的資料"""
    conn = get_connection()
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
        df['標準品名'] = df.apply(format_standard_name, axis=1)
        df['year'] = df['year'].fillna(0).astype(int).astype(str).replace('0', '')
        df['style'] = df['style'].fillna('')
        df['series'] = df['series'].fillna('')
    
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