import requests
from bs4 import BeautifulSoup
import time
import random
import re
import sqlite3
from datetime import datetime, timedelta
import pandas as pd
import os

DRY_RUN = False #True = 試跑(不存)， False = 正式(存檔)
DB_NAME = 'data/P9_whiskey_project.db'

#抓取的天數
DAYS_TO_SCRAPE = 2

TARGET_DATE = (datetime.now() - timedelta(days=DAYS_TO_SCRAPE)).strftime('%Y/%m/%d')

MISS_LIMIT = 20
MAX_PAGES = 10


cutoff_date = datetime.now() - timedelta(days=DAYS_TO_SCRAPE)
cutoff_str = cutoff_date.strftime('%Y/%m/%d')


#資料庫
def init_db():
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS market_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_date TEXT,
                title TEXT, 
                author TEXT, 
                product_name TEXT,
                price INTEGER, 
                link TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

# 存檔
def save_to_db (item):
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        check = cursor.execute(
            'SELECT id FROM market_prices WHERE post_date=? AND author=? AND product_name=? AND price=?',
            (item['date'], item['author'], item['name'], item['price'])
        ).fetchone()

        if not check:
            cursor.execute(
                'INSERT INTO market_prices (post_date, title, author, product_name, price, link) VALUES (?, ?, ?, ?, ?, ?)',
                (item['date'], item['title'], item['author'], item['name'], item['price'], item['link'])
            )
            return True
    return False


def clean_string(text):
    """
    只保留：中文、英文(a-z, A-Z)、數字(0-9)
    去除：所有標點符號、括號、特殊字元、Emoji、空格(視需求而定)
    """
    if not text: return ""
    # Regex 邏輯： [^\u4e00-\u9fa5a-zA-Z0-9] 代表「除了中英數以外的所有字元」
    # 如果你想「保留空格」，請將 regex 改為 r'[^\u4e00-\u9fa5a-zA-Z0-9\s]'
    text = text.replace('×', 'x').replace('＊', '*').replace('X', 'x')
    cleaned = re.sub(r'[^\u4e00-\u9fa5a-zA-Z0-9\s/.\-*]', '', text)
    return cleaned.strip()

# 抓取工具函數

def get_title(soup):
    try:
        title_node = soup.find('span', id = re.compile(r'lblSubject'))
        if title_node:
            return title_node.text.strip()
    except: pass
    return None


def get_date(soup):
    try:
        date_meta = soup.find('meta', property='og:image')
        if date_meta and date_meta.get('content'):
            dm = re.search(r'/(\d{4}/\d{2}/\d{2})/', date_meta['content'])
            if dm: return dm.group(1)
    except: pass
    try:
        text = soup.get_text(separator=' ', strip=True)
        full_time_match = re.search(r'(\d{4}/\d{2}/\d{2})\s+\d{2}:\d{2}:\d{2}', text)
        if full_time_match: return full_time_match.group(1)
    except: pass
    try:
        fallback_match = re.search(r'發表時間\D*(\d{4}/\d{2}/\d{2})', text)
        if fallback_match: return fallback_match.group(1)
    except: pass
    return '未知'

def get_author(soup):
    try:
        author_td = soup.find('td', class_='pricelist_01')
        if author_td:
            parts = author_td.get_text(separator='\n', strip=True).split('\n')
            if parts:
                name = parts[0].strip()
                if "評價" not in name and len(name) > 0: return name
    except: pass
    return "未知"

def main(DRY_RUN=True):
    if not DRY_RUN : init_db()

    base_url = "https://www.p9.com.tw"
    base_list_url = 'https://www.p9.com.tw/Forum/ForumSection.aspx?id=1&BoardId=5&Sort=Post_Time'
    headers = { "User-Agent": "Mozilla/5.0" }

    session = requests.Session()
    session.headers.update(headers)

    today_date = datetime.now()
    page = 1
    total_added = 0
    stop_crawling = False
    consecutive_miss_count = 0

    blacklist = ['全部','私','一起','總共','共','帶里程','起跑','徵']
    spec_keywords =['cask','no','no:','桶號']
    seen_items = set()

    while page <= MAX_PAGES and not stop_crawling:
        print(f'[進度]讀取第{page}頁...(目前累積落空:{ consecutive_miss_count}篇)')
        try:
            resp = session.get(f'{base_list_url}&Page={page}')
            soup = BeautifulSoup(resp.content, 'html.parser')
            posts = soup.find_all('a', class_='a18')

            if not posts: break

            target_posts = posts

            for post in target_posts:
                list_title = post.text.strip()
                link = base_url + post.get('href')

                #標題有這些關鍵字就跳過
                if any(k in list_title for k in ['徵','收','杯','路跑','殘劑']): continue

                try:
                    resp_inner = session.get(link)
                    soup_inner = BeautifulSoup(resp_inner.text, 'html.parser')

                    post_date = get_date(soup_inner)

                    #抓取完整標題
                    inner_title = get_title(soup_inner)
                    final_title = inner_title if inner_title else list_title

                    #日期判斷
                    if post_date != '未知':
                        if post_date <= cutoff_str:
                            consecutive_miss_count += 1
                            if consecutive_miss_count >= MISS_LIMIT:
                                print(f'連續{MISS_LIMIT} 篇早於 {cutoff_str}，任務結束!')
                                stop_crawling = True
                                break
                            time.sleep(random.uniform(1, 2))
                            continue
                        elif post_date > today_date.strftime('%Y/%m/%d'):
                            continue
                        else:
                            consecutive_miss_count = 0

                    #抓取內容
                    author = get_author(soup_inner)
                    content_tag = soup_inner.find('td' , class_='editZone')
                    if content_tag:
                        raw_text = content_tag.get_text(separator='\n', strip=True)
                        lines = raw_text.split('\n')

                        for line in lines:
                            line = line.strip()
                            if len(line) < 3 : continue
                            #移除千分號
                            clean = line.replace(',','').replace('，','')

                            clean_line = re.sub(r'\d+\s*(?:ml|mL|ML|cc|CC|cl|CL|l|L)(?![a-zA-Z])', '', clean, flags=re.IGNORECASE)

                            regex_pattern = r'(.*)(?:\s+|＝|=|:)(\d+)(\s*(?:跑|公尺|k|K|含.*|/瓶|/一瓶|/組|/一組|/B|/b|/JPEG|/JPG|像素|里程|免運|貓|大嘴鳥|/位|.*)?)?$'
                            match = re.search(regex_pattern, clean_line)

                            match_fallback = None
                            if not match :
                                match_fallback = re.search(r'(?:跑|公尺|里|JPEG|NT|\$)\s*(\d+)', clean, re.IGNORECASE)

                            raw_name, final_price = '', 0
                            if match:
                                raw_name = match.group(1).strip()
                                final_price = int(match.group(2))
                                
                                # 【新增】檢查後綴的數量標示
                                suffix = match.group(3)
                                if suffix:
                                    # 情況 1: 明確數字 (如 /2, /3b, /2瓶) -> 標記為 /N
                                    qty_match = re.search(r'/(\d+)', suffix)
                                    if qty_match:
                                        qty = qty_match.group(1)
                                        raw_name += f" /{qty}"
                                    # 情況 2: 僅有單位無數字 (如 /位, /瓶) -> 標記為 /1 (單價)
                                    elif re.search(r'/[位瓶bB支罐組]', suffix):
                                        raw_name += " /1"
                            elif match_fallback:
                                raw_name = clean_line
                                final_price = int(match_fallback.group(1))


                            if final_price > 0:
                                if final_price > 500000 or final_price < 800: continue
                                if 1950 < final_price < 2030:
                                    if not any(s in line for s in ['跑','含','免運','JPEG','JPG','K','k','/']):continue

                                final_name = clean_string(raw_name)
                                clean_title = clean_string(final_title)

                                check_str = (final_name + clean_title).lower()



                                if any(bad in check_str for bad in blacklist): continue
                                if any(x in check_str for x in spec_keywords): continue
                                if author != '未知' and author in final_name: continue
                                if (final_name, final_price) in seen_items: continue
                                seen_items.add((final_name, final_price))



                                item = {
                                    'date': post_date,
                                    'title': final_title,
                                    'author': author,
                                    'name': final_name[:35],
                                    'price': final_price,
                                    'link':link
                                }

                                log_msg = f"[{item['date']}] {item['author']} - {item['name'][:10]} ${item['price']}"

                                if DRY_RUN:
                                    print(f"[模擬] {log_msg} (標題: {item['title'][:10]}...)")

                                else:
                                    if save_to_db(item):
                                        print(f' [存檔] {log_msg}')
                                        total_added += 1
                    
                    sleep_time = random.uniform(1.5, 3.5)
                    time.sleep(sleep_time)

                except Exception as e:
                    print(f"  [錯誤] 處理內頁失敗: {e}")
                    time.sleep(2)
                    continue


            page += 1


        except Exception as e:
            print(f"[錯誤] 讀取列表頁失敗: {e}")
            break

    print(f"\n--- 爬蟲結束，共新增 {total_added} 筆資料 ---")


        # 驗證
    if not DRY_RUN and total_added >= 0:
        print("\n📊 驗證最新資料：")
        try:
            conn = sqlite3.connect(DB_NAME)
            df = pd.read_sql_query("SELECT post_date, title, price FROM market_prices ORDER BY id DESC LIMIT 5", conn)
            print(df)
            conn.close()
        except: pass


if __name__ == '__main__':
    main(DRY_RUN=DRY_RUN)


            