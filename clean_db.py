import sqlite3

def clean_duplicate_data():
    conn = sqlite3.connect('whisky_market.db')
    cursor = conn.cursor()
    
    print("🧹 開始清理資料庫...")
    
    # 使用 SQL 語法刪除 whisky_prices 中完全重複的資料 (保留 rowid 最小的那一筆)
    cleanup_sql = """
        DELETE FROM whisky_prices
        WHERE rowid NOT IN (
            SELECT MIN(rowid)
            FROM whisky_prices
            GROUP BY post_id, brand, product_name, year, series, style, price_per_bottle
        );
    """
    
    cursor.execute(cleanup_sql)
    deleted_rows = cursor.rowcount
    conn.commit()
    conn.close()
    
    print(f"✅ 清理完成！總共移除了 {deleted_rows} 筆重複的報價垃圾資料。")

if __name__ == "__main__":
    clean_duplicate_data()