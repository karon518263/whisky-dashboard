import pandas as pd
import re
import os
import sqlite3

DB_NAME = 'data/P9_whiskey_project.db'  # 資料庫名稱
SOURCE_TABLE = 'market_prices'     # 原始資料表
OUTPUT_TABLE = 'cleaned_data'      # 清洗後要存入的新資料表名稱
TEST_MODE = False  #True: 只跑100筆 / False:跑全部
SAMPLE_SIZE = 100 #測試筆數

#輸入檔案名稱
SORTED_BRAND_KEYS = [] # 請保留您原本的品牌列表
BRAND_MAPPING = {}     # 請保留您原本的品牌對照表
PRODUCT_MAPPING = {}
STYLE_MAPPING = {}
SORTED_PRODUCT_KEYS = []
SORTED_STYLE_KEYS = []

#1.品牌對照表
BRAND_MAPPING = {
    # --- 麥卡倫家族 (Macallan) ---
    '麥卡倫': 'Macallan', 
    'Macallan': 'Macallan', 
    'Macland': 'Macallan', 
    'M': 'Macallan',       
    '麥': 'Macallan',
    '紫鑽': 'Macallan',    
    '臻彩': 'Macallan',

    # --- 約翰走路家族 (Johnnie Walker) ---
    '約翰走路': 'Johnnie Walker', 
    '走路': 'Johnnie Walker', 
    '藍牌': 'Johnnie Walker', 
    'XR': 'Johnnie Walker',
    '黑牌': 'Johnnie Walker',
    '雙黑': 'Johnnie Walker',
    '金牌': 'Johnnie Walker',

    # --- 蘇格蘭-斯貝賽 (Speyside) ---
    '格蘭利威': 'Glenlivet', '利威': 'Glenlivet',
    '格蘭多納': 'Glendronach', '多納': 'Glendronach',
    '格蘭艾樂奇': 'Glenallachie', '艾樂奇': 'Glenallachie',
    '格蘭花格': 'Glenfarclas', '花格': 'Glenfarclas',
    '格蘭菲迪': 'Glenfiddich', '菲迪': 'Glenfiddich',
    '百富': 'Balvenie',
    '格蘭傑': 'Glenmorangie',
    '格蘭冠': 'Glen Grant',
    '格蘭路思': 'Glenrothes',
    '慕赫': 'Mortlach',
    '百樂門': 'Benromach',
    '坦度': 'Tamdhu',
    '班瑞克': 'BenRiach',
    '詩貝': 'Spey',
    '蘇格登': 'Singleton', 
    '亞伯樂': 'Aberlour', 'Aberlour': 'Aberlour', 'A\'bunadh': 'Aberlour',
    '克拉格摩爾': 'Cragganmore',
    '卡杜': 'Cardhu',
    '歐本': 'Oban',
    '老富特尼': 'Old Pulteney', '富特尼': 'Old Pulteney',
    '羅曼德湖': 'Loch Lomond',

    # --- 蘇格蘭-高地 (Highlands) ---
    '格蘭哥尼': 'Glengoyne', '哥尼': 'Glengoyne',
    '丁士頓': 'Deanston', '汀士頓': 'Deanston',
    '皇家禮炮': 'Royal Salute', '禮炮': 'Royal Salute', '馬球': 'Royal Salute',
    '大摩': 'Dalmore',
    '克里尼利基': 'Clynelish', '小山貓': 'Clynelish',
    '艾德多爾': 'Edradour',
    '老酋長': 'Chieftain',
    '林多修道院': 'Lindores Abbey',

    # --- 蘇格蘭-艾雷島 (Islay) & 島嶼區 ---
    '泰斯卡': 'Talisker', '大力斯可': 'Talisker',
    '拉加維林': 'Lagavulin', '拉加': 'Lagavulin', '樂加維林': 'Lagavulin', 
    '雅柏': 'Ardbeg', '阿貝': 'Ardbeg', '雅柏艾雷': 'Ardbeg',
    '布納哈本': 'Bunnahabhain',
    '奧特摩': 'Octomore',
    '齊侯門': 'Kilchoman',
    '高原騎士': 'Highland Park', '高原': 'Highland Park',
    '吉拉': 'Jura',
    '天空之島': 'Isle of Skye',
    '拉弗格': 'Laphroaig', 
    '波摩': 'Bowmore', 
    '卡爾里拉': 'Caol Ila', 
    '布萊迪': 'Bruichladdich', 
    '波夏': 'Port Charlotte',

    # --- 蘇格蘭-坎貝爾鎮 (Campbeltown) ---
    '雲頂': 'Springbank',
    '齊克倫': 'Kilkerran',
    '赫佐本': 'Hazelburn',
    '朗格羅': 'Longrow',

    # --- 蘇格蘭-低地 (Lowlands) ---
    '歐肯': 'Auchentoshan',

    # --- 調和威士忌 (Blended) ---
    '百齡罈': 'Ballantine', '百齡壇': 'Ballantine',
    '帝王': 'Dewar',
    '三隻猴子': 'Monkey',
    '起瓦士': 'Chivas Regal', 'Chivas': 'Chivas Regal',
    '裸鑽': 'Naked Malt', 'Naked Grouse': 'Naked Malt',
    '順風': 'Cutty Sark',
    '教師': "Teacher's",
    '老伯': 'DeLuxe',

    # --- 美國威士忌 (Bourbon/Rye) ---
    '金賓': 'Jim Beam',
    '威勒': 'Weller',
    'STAGG': 'STAGG',
    '美格': "Maker's",      
    '野火雞': 'Wild Turkey',
    '威雀':'Famous Grouse',
    '傑克丹尼': "Jack Daniel's", 'Jack Daniel': "Jack Daniel's",
    '水牛足跡': 'Buffalo Trace', 'Buffalo': 'Buffalo Trace',
    '布蘭頓': "Blanton's", 'Blanton': "Blanton's", 
    '四玫瑰': 'Four Roses',
    '伍德福': 'Woodford Reserve',

    # --- 愛爾蘭威士忌 (Irish) ---
    '尊美醇': 'Jameson',

    # --- 台灣/日本/亞洲 (Asian) ---
    '噶瑪蘭': 'Kavalan', '葛瑪蘭': 'Kavalan', 
    'OMAR': 'OMAR',
    '玉尊': 'Jade Taiwan Whisky',
    
    # 日威
    '響': 'Hibiki',
    '山崎': 'Yamazaki',
    '余市': 'Yoichi',
    '白州': 'Hakushu',
    '竹鶴': 'Taketsuru',
    '宮城峽': 'Miyagikyo',
    '秩父': 'Chichibu',
    '山櫻': 'Yamazakura',
    '一甲': 'Nikka',
    '三得利': 'Suntory',
    '角瓶': 'Suntory Kakubin',
    '信州': 'Mars', 'Mars': 'Mars', '駒之岳': 'Mars',
    '明石': 'Akashi', 'White Oak': 'Akashi',
    '倉吉': 'Kurayoshi', '松井': 'Matsui',
    '厚岸': 'Akkeshi',
    '甲斐之虎': 'Kyoto Whisky',  
    '武田信玄': 'Kyoto Whisky', 
    '京都': 'Kyoto Whisky',     
    '織田信長': 'Kyoto Whisky',
    '天王山': 'Kyoto Whisky'
}
SORTED_BRAND_KEYS = sorted(BRAND_MAPPING.keys(), key=len, reverse=True)

#2.品名/系列
PRODUCT_MAPPING = {
    # 年份限定版 2017-2024
    '2014': '2014限定',
    '2015': '2015限定',
    '2016': '2016限定',
    '2017': '2017限定',
    '2018': '2018限定',
    '2019': '2019限定',
    '2020': '2020限定',
    '2021': '2021限定',
    '2022': '2022限定',
    '2023': '2023限定',
    '2024': '2024限定',

    #生肖系列
    '鼠年': '鼠年',
    '牛年': '牛年',
    '虎年': '虎年',
    '兔年': '兔年',
    '龍年': '龍年',
    '蛇年': '蛇年',
    '馬年': '馬年',
    '羊年': '羊年',
    '猴年': '猴年',
    '雞年': '雞年',
    '狗年': '狗年',
    '豬年': '豬年',

    #麥卡倫系列
    'Rich Cacao': '可可協奏曲', '可可': '可可協奏曲',
    'Intense Arabica': '阿拉比卡咖啡', '咖啡': '阿拉比卡咖啡',
    'Amber Meadow': '萃綠麥穗', '麥穗': '萃綠麥穗',
    'Green Meadow': '綠色麥穗', 
    'Vibrant Oak': '活力橡木',

    #Edition Series (No.1 - No.6)
    'Edition No.1': 'Edition No.1',
    'Edition No.2': 'Edition No.2',
    'Edition No.3': 'Edition No.3',
    'Edition No.4': 'Edition No.4',
    'Edition No.5': 'Edition No.5',
    'Edition No.6': 'Edition No.6',

    #概念
    'Concept No.1': '概念1', 'Concept 1': '概念1',
    'Concept No.2': '概念2', 'Concept 2': '概念2',
    'Concept No.3': '概念3', 'Concept 3': '概念3',

    #探索
    'Quest': '藍天',
    'Lumina': '絢綠',
    'Arura': '澄光',
    'Enigma': '湛藍',

    #麥卡倫其他系列
    'Rare Cask Black': '湛黑', 'Black': '湛黑',
    'Rare Cask': '湛黑',
    '御黑': '御黑',
    '湛黑': '湛黑',
    'Gran Reserva': '紫鑽', '紫鑽': '紫鑽',
    'Estate': '莊園',
    'M Decanter': 'M Decanter',
    'ELEGANCIA':'ELEGANCIA',
    '地球之夜': '春宴', '春宴': '春宴', 
    'Folio': '書冊系列', '書冊': '書冊系列',
    'James Bond': '龐德60週年', '007': '龐德60週年',
    'Home Collection': '家園系列',
    'Colour Collection': '臻彩系列', '臻彩': '臻彩',
    'Litha': 'Litha', '黃金麥穗': 'Litha',
    'Horizon': 'Horizon',
    'Fine & Rare': '珍稀系列',
    'Classic Cut 2017': 'Classic Cut 2017',
    'Classic Cut 2018': 'Classic Cut 2018',
    'Classic Cut 2019': 'Classic Cut 2019',
    'Classic Cut 2020': 'Classic Cut 2020',
    'Classic Cut 2021': 'Classic Cut 2021',
    'Classic Cut 2022': 'Classic Cut 2022',
    'Classic Cut 2023': 'Classic Cut 2023',
    'Classic Cut 2024': 'Classic Cut 2024',
    '紅標':'紅標','綠標':'綠標','藍標':'藍標',


    #約翰走路
    'Blue Label': '藍牌', '藍牌': '藍牌',
    'XR': 'XR', 
    'Ghost and Rare': '幽靈', 'Ghost': '幽靈',
    'King George': '喬治五世', 'KG5': '喬治五世',
    '黑牌':'黑牌',

    #雅柏
    'Uigeadail': '烏迦爹', '烏迦達': '烏迦爹',
    'Corryvreckan': '柯瑞', '漩渦': '漩渦',
    'An Oa': 'An Oa',
    'Wee Beastie': '小野獸',
    'Heavy Vapours': '重蒸氣',
    'Hypernova': '超新星',
    'Scorch': '猛龍',
    'Ardcore': '龐克',
    'Fermutation': '發酵',
    'Blaaack': '黑羊',
    'Drum': '鼓',
    'Grooves': '嬉皮',
    'Kelpie': '海妖',
    'Dark Cove': '暗灣',
    
    #格蘭傑
    'Signet': '稀印', 
    'Lasanta': '勒桑塔',
    'Quinta Ruban': '昆塔',
    'Nectar D\'Or': '納塔朵',

    #百富
    #故事系列
    'Sweet Toast': '糖心', '糖心': '糖心',      
    'Week of Peat': '泥煤週', '泥煤周': '泥煤週',        
    'Edge of Burnhead': '石楠花', 'Burnhead': '石楠花',
    'Red Rose': '玫瑰',                                                     
    'Distant Shores': '濱海尋秘',                      

    #桶號系列
    'Tun 1509': 'Tun 1509',
    'Tun 1401': 'Tun 1401',
    'Tun 1858': 'Tun 1858',
    'Tun 1508': 'Tun 1508',

    #核心/過桶系列
    'Caribbean Cask': '加勒比海', 'Caribbean': '加勒比海', '加勒比海': '加勒比海',
    'French Oak': '皮諾甜酒', 'Pineau': '皮諾甜酒', '皮諾': '皮諾甜酒',            
    'Peated Triple Cask': '泥煤三桶',

    #日威
    'Blossom Harmony': '櫻花', 'Blossom': '櫻花',
    'Limited Edition': '限定版',
    'Tsukuriwake': '職人系列',
    '花鳥':'花鳥風月版', '車輪':'車輪',
    '金花':'金花','碧AO':'碧AO',
    '30周年':'30週年','100周年':'100週年',


    #噶瑪蘭
    'Vistillery': '酒廠珍藏',
    'Concertmaster': '山川', '山川首席': '山川首席',
    'Podium': '堡典',
    '珍選no.1': '珍選no.1',
    '珍選no.2': '珍選no.2',
    
    #大摩
    'Cigar Malt': '雪茄三桶', 'Cigar': '雪茄三桶', '雪茄': '雪茄三桶',
    'King Alexander': '亞歷山大', 'Alexander': '亞歷山大', '亞歷山大': '亞歷山大',
    '築光': '築光',
    
    # --- 其他熱門 ---
    'A\'bunadh': '首選原酒',
    'Naked Malt': '裸鑽', 'Naked Grouse': '裸鑽',
    'Monkey Shoulder': '三隻猴子',
    '105': '105原酒','1公升':'1公升','1l':'1公升',

    '禮盒':'禮盒','大師':'大師精選',
    '馬球1': '第一代藍玉髓', '馬球2': '沙灘馬球','馬球3':'雪地馬球','馬球4':'阿根廷馬球',
    '馬球5': '印度沙漠馬球', '馬球6': '邁阿密馬球','馬球7': '巴西里約馬球',
    '110 proof':'110 proof','家族桶': '家族桶', 'Family Cask': '家族桶',
}
SORTED_PRODUCT_KEYS = sorted(PRODUCT_MAPPING.keys(), key=len, reverse=True)

#3.桶型風味
STYLE_MAPPING = {
    #原酒
    '雪莉單桶': '雪莉單桶',
    '波本單桶': '波本單桶',
    '波特單桶': '波特單桶',
    '泥煤單桶': '泥煤單桶',
    '水楢單桶': '水楢單桶',
    '雪莉原酒': '雪莉原酒',
    '波本原酒': '波本原酒',
    '泥煤原酒': '泥煤原酒',
    '雙桶原酒': '雙桶原酒',
    'PX雪莉': 'PX雪莉桶',
    'Oloroso': 'Oloroso雪莉桶',
    'Pedro Ximenez': 'PX雪莉桶',
    'First Fill': '初次桶',
    'Refill': '二次桶',

    #基礎桶
    '雪莉': '雪莉桶', 'Sherry': '雪莉桶',
    '波本': '波本桶', 'Bourbon': '波本桶',
    '雙桶': '雙桶', 'Double Cask': '雙桶', 'Double Wood': '雙桶',
    '三桶': '三桶', 'Triple Cask': '三桶', 'Triple Wood': '三桶',
    '黃金三桶': '黃金三桶', 'Fine Oak': '黃金三桶',
    '四桶': '四桶',
    
    #葡萄酒/加烈酒/其他酒桶
    '波特': '波特桶', 'Port': '波特桶', 'Ruby Port': '紅寶石波特', 'Tawny Port': '茶色波特',
    '馬德拉': '馬德拉桶', 'Madeira': '馬德拉桶',
    '紅酒': '紅酒桶', 'Wine Cask': '紅酒桶', 'Bordeaux': '波爾多紅酒桶', 'Burgundy': '勃根地紅酒桶',
    '白酒': '白酒桶', 'Chardonnay': '夏多內白酒桶', 'Sauternes': '蘇玳甜白酒桶',
    '蘭姆': '蘭姆桶', 'Rum': '蘭姆桶', 'Caribbean': '蘭姆桶', 
    '干邑': '干邑桶', 'Cognac': '干邑桶',
    '馬沙拉': '馬沙拉桶', 'Marsala': '馬沙拉桶',
    '阿瑪': '阿瑪紅酒桶', 'Amarone': '阿瑪紅酒桶',
    '豬頭桶': '豬頭桶', 'Virgin Oak': '處女桶',

    #特殊橡木
    '水楢': '水楢桶', 'Mizunara': '水楢桶',
    '法國橡木': '法國橡木', 'French Oak': '法國橡木',
    '美國橡木': '美國橡木', 'American Oak': '美國橡木',
    '西班牙橡木': '西班牙橡木', 'Spanish Oak': '西班牙橡木',

    #規格與工藝
    '單桶': '單桶', 'Single Cask': '單桶',
    '原酒': '原酒', 'Cask Strength': '原酒', 'Batch Strength': '原酒',
    '泥煤': '泥煤', 'Peated': '泥煤', 'Peat': '泥煤',
    '煙燻': '煙燻', 'Smoky': '煙燻',
    '1/4桶': '1/4桶', 'Quarter Cask': '1/4桶',
    '小批次': '小批次', 'Small Batch': '小批次',
    '非冷凝': '非冷凝', 'Non Chill Filtered': '非冷凝',
    '手札': '手札',                  
    '私藏': '私藏',                 
    '精選': '精選',
    
}
SORTED_STYLE_KEYS = sorted(STYLE_MAPPING.keys(), key=len, reverse=True)

#年份定義
TARGET_YEARS = {5, 8, 10, 11, 12, 13, 15, 16, 17, 18, 19, 21, 23, 25, 30, 35, 40, 50}
#單位量詞定義
BOTTLE_PATTERN = re.compile(r'(\d+)\s*(?:B|瓶|位|圈|入|組)', re.IGNORECASE)

#轉換成統一的標準品牌名稱
def normalize_brand(text):
    if not isinstance(text, str): return 'Other'
    for key in SORTED_BRAND_KEYS:
        standard_name = BRAND_MAPPING[key]
        if len(key) <= 2 and re.search(r'[a-zA-Z]', key):
            pattern = r'(?<![a-zA-Z0-9])' + re.escape(key) + r'(?![a-zA-Z0-9])'
        else:
            pattern = re.escape(key)
        if re.search(pattern, text, re.IGNORECASE):
            return standard_name
    return 'Other'

#找特徵(桶型&系列)
def extract_attribute(text, mapping_dict, sorted_keys):
    if not isinstance(text, str): return None
    for key in sorted_keys:
        if re.search(re.escape(key), text, re.IGNORECASE):
            return mapping_dict[key]
    return None

#資料清洗
def parse_product_info(row):
    raw_name_text = str(row.get('product_name',''))

    # 1. 品牌解析
    clean_product_name = ''
    if raw_name_text:
        # 移除尾部括號與亂碼
        temp_text = re.sub(r'[(\[\{]+$', '', raw_name_text)
        # 只保留有意義的字元
        clean_product_name = re.sub(r'[^\u4e00-\u9fa5a-zA-Z0-9\s\.\-\(\)]', ' ', temp_text).strip()

    search_text = clean_product_name

    #品牌解析
    resolved_brand = 'Other'
    if search_text: 
        resolved_brand = normalize_brand(search_text)


    #年分解析
    year_str = '無年份'
    resolved_year = None

    if clean_product_name:
        year_search_text = BOTTLE_PATTERN.sub('', search_text)
        year_search_text = re.sub(r'\d+\s*(?:週年|周年|th\s*anniversary|th)', '', year_search_text, flags=re.IGNORECASE)
        NOISE_PATTERN = re.compile(r'(?:第|batch|lot|no\.?|vol|ver)\s*\d+(?:\s*批次|\s*版|\s*batch)?', re.IGNORECASE)
        year_search_text = NOISE_PATTERN.sub('', year_search_text)

        explicit_match = re.search(r'(?<!\d)(\d{1,2})\s*(?:年|yo|y)', year_search_text, re.IGNORECASE)
        if explicit_match:
            try:
                val = int(explicit_match.group(1))
                if 5 <= val <= 50: resolved_year = val
            except: pass
        if resolved_year is None:
            numbers = re.findall(r'(\d+)', year_search_text)
            for num_str in numbers:
                val = int (num_str)
                if val in TARGET_YEARS:
                    resolved_year = val
                    break
    if resolved_year: year_str = str(resolved_year)

    # 抓取桶型及系列
    resolved_series = ''
    resolved_style = ''
    # 解析系列
    series_found = extract_attribute(search_text, PRODUCT_MAPPING, SORTED_PRODUCT_KEYS)
    if series_found:
        resolved_series = series_found
    # 解析桶型
    style_found = extract_attribute(search_text, STYLE_MAPPING, SORTED_STYLE_KEYS)
    if style_found:
        resolved_style = style_found
    if resolved_series and resolved_style:
            if resolved_series == resolved_style:
                resolved_style = ''
    

    return {
        '品牌': resolved_brand,
        '年份': year_str,
        '系列': resolved_series, 
        '桶型': resolved_style,
    }

# 計算單價
def calculate_unit_price(row):
    # 1.取得原始價格
    try:
        raw_price = row.get('price', 0)
        if isinstance(raw_price, str):
            raw_price = re.sub(r'[^\d.]','', raw_price)
        original_price = float(raw_price)
    except:
        return 0
    
    if original_price == 0 : return 0
    
    # 2.鎖定 product_name，轉小寫以利比對
    search_text = str(row.get('product_name','')).strip().lower()

    if not search_text:
        return int(original_price)
    
    explicit_qty_match = re.search(r'[/／]\s*(\d+)(?:\s*(?:b|瓶|支|入|罐|組|位))?(?:$|\s|[^\w])', search_text)
    if explicit_qty_match:
        qty = int(explicit_qty_match.group(1))
        if qty > 0:
            return int(original_price / qty)
    
    resolved_quantity = 1
    # 策略 A: X 偵測 (解決 1公升X2, 0.5x2)
    # 規則：X 前面允許是 數字/中文/單位，但不可以是普通英文單字
    x_match = re.search(r'(?:^|[\s\(\)/,.\-\d\u4e00-\u9fa5lm])([x\*])\s*(\d+)', search_text)

    if x_match:
        num = int(x_match.group(2))
        if 1 < num <= 60:
            resolved_quantity = num

    # 策略 B: 數字加B (解決6B, 一箱6B)

    if resolved_quantity == 1:
        b_match = re.search(r'(\d+)\s*b(?![a-z])', search_text)
        if b_match:
            num = int(b_match.group(1))
            if 1 < num <= 60:
                resolved_quantity = num

    # 策略 C: 關鍵字 "跑" (社團術語)
    if resolved_quantity == 1:
        pao_match = re.search(r'跑\s*(\d+)', search_text)
        if pao_match:
            num = int(pao_match.group(1))
            if num > 1 : resolved_quantity = num

    # 策略 D: 標準單位 (位, 瓶, 入, 支, 罐)
    if resolved_quantity == 1:
        unit_matches = re.findall(r'(\d+)\s*(?:瓶|入|支|罐|組|位)', search_text)
        for val in unit_matches:
            num = int(val)
            if 1 < num <= 60: 
                resolved_quantity = num
                break

    # 策略 E: 中文數字 (兩瓶, 對瓶)
    if resolved_quantity == 1:
        if re.search(r'(?:對)(?:瓶|支|入|b|組|位)?', search_text) and '雙桶' not in search_text:
            resolved_quantity = 2
        else:
            cn_map = {'兩':2, '二':2, '三':3, '四':4, '五':5, '六':6, '十二':12}
            cn_match = re.search(r'([兩二三四五六])\s*(?:瓶|b|支|入|組|位)', search_text)
            if cn_match:
                val = cn_match.group(1)
                resolved_quantity = cn_map.get(val, 1)

    # 3. 計算最終單價
    if resolved_quantity > 1:
        calculated_price = int(original_price / resolved_quantity)
        if calculated_price < 400 and original_price > 5000:
            return int(original_price)
        return calculated_price
    
    return int(original_price)


def load_data_from_db(db_name):
    """連線資料庫並讀取原始資料"""
    if not os.path.exists(db_name):
        print(f"❌ 錯誤: 找不到資料庫檔案 {db_name}")
        return None

    try:
        conn = sqlite3.connect(db_name)
        print(f"📂 連線資料庫: {db_name}")
        
        # 讀取資料 SQL (假設你的原始爬蟲表單叫做 market_prices)
        query = f"SELECT * FROM market_prices"
            
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        print(f"📥 成功讀取 {len(df)} 筆資料")
        return df
    except Exception as e:
        print(f"❌ 資料庫讀取失敗: {e}")
        return None

def save_data_to_db(df, db_name, table_name='cleaned_data'):
    """將清洗後的資料存回資料庫的新表"""
    try:
        conn = sqlite3.connect(db_name)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.close()
        print(f"💾 已將清洗結果存回資料庫表 [{table_name}]")
    except Exception as e:
        print(f"❌ 存回資料庫失敗: {e}")

def clean_dataset(df):
    """
    資料清洗主程序。
    執行步驟：
    1. 欄位重新命名 
    2. 價格計算 
    3. 特徵解析 (品牌/年份/系列/桶型)
    4. 資料過濾 
    5. 格式整理 
    """

    print(" 開始執行資料清洗 (自動過濾 'Other' 品牌)...")

     # 1. 初始化工作資料表
    if TEST_MODE:
        print(f" 測試模式：僅處理前 {SAMPLE_SIZE} 筆")
        working_df = df.head(SAMPLE_SIZE).copy()
    else:
        working_df = df.copy()

    # 2. 欄位對照與預處理
    input_col_mapping = {
        '標題': 'title', 
        '價格': 'price',     
        '作者': 'author', 
        '日期': 'post_date', 
        '品名': 'product_name'
    }

    working_df.rename(columns=input_col_mapping, inplace=True)

    # 填補缺失值，確保後續處理不會報錯
    working_df['title'] = working_df['title'].fillna('').astype(str)
    if 'post_date' not in working_df.columns:
        working_df['post_date'] = ''
    else:
        working_df['post_date'] = working_df['post_date'].fillna('').astype(str)

    try:
        working_df['post_date'] = pd.to_datetime(working_df['post_date'], errors='coerce').dt.strftime('%Y/%m/%d')
        # 如果有轉失敗的(變成NaT)，補回空字串以免報錯
        working_df['post_date'] = working_df['post_date'].fillna('')
    except Exception as e:
        print(f"⚠️ 日期轉換部分失敗: {e}")

    # 3. 計算
    # A. 計算單價
    working_df['unit_price'] = working_df.apply(calculate_unit_price, axis=1)

    # B. 解析產品資訊
    parsed_features = working_df.apply(parse_product_info, axis=1)
    features_df = pd.DataFrame(parsed_features.tolist())

    # 4. 合併與過濾 (Merge & Filter)
    df_merged = pd.concat([working_df.reset_index(drop=True), features_df.reset_index(drop=True)], axis=1)

    # 記錄過濾前的筆數
    original_count = len(df_merged)

    df_final = df_merged[df_merged['品牌'] != 'Other'].copy()

    #價格過濾 (<600 不記入) 
    before_price_filter_count = len(df_final)
    df_final = df_final[df_final['unit_price'] >= 600]
    
    filtered_count = len(df_final)
    print(f" 已過濾非目標品牌: {original_count} -> {filtered_count} 筆")

    # 5. 輸出欄位整理 (Final Output Formatting)
    target_columns = [
        'post_date',    # 日期
        '品牌',         # Resolved Brand
        '年份',         # Year String
        '系列',         # Resolved Series
        '桶型',         # Resolved Style
        'unit_price',   # 計算後的單價
        'product_name',  # 原始品名 (方便除錯對照)
        'title',
        'author',
        'link'
    ]

    existing_cols = [col for col in target_columns if col in df_final.columns]
    df_final = df_final[existing_cols]

    # 最後將欄位名稱改回中文，符合輸出需求
    output_col_mapping = {
        'post_date': '日期',
        'unit_price': '價格', # 將計算後的單價顯示為 "價格"
        'author':'賣家',
        'link':'賣場連結'
    }
    df_final.rename(columns=output_col_mapping, inplace=True)
    
    print(f" 清洗完成！")
    return df_final

# 5. 檔案讀取與執行
if __name__ == "__main__":
    # 1. 從 DB 讀取
    raw_df = load_data_from_db(DB_NAME)
    
    if raw_df is not None and not raw_df.empty:
        try:
            # 2. 清洗資料
            final_df = clean_dataset(raw_df)
            
            # 3. 存回 DB
            save_data_to_db(final_df, DB_NAME, OUTPUT_TABLE)
            
            print("\n--- 成果預覽 (Top 10) ---")
            print(final_df.head(10).to_string(index=False))
                 
        except Exception as e:
            print(f"❌ 發生錯誤: {e}")
            import traceback
            traceback.print_exc()
    else:
        print("❌ 無法讀取資料或資料表為空")



