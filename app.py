import streamlit as st
import pandas as pd
import plotly.express as px
import db_utils

# --- 1. 頁面設定 ---
st.set_page_config(
    page_title="P9 威士忌行情儀表板",
    page_icon="🥃",
    layout="wide"
)

st.sidebar.title("🥃 查詢選項")
menu = st.sidebar.radio("功能導覽", ["儀表板總覽", "酒款搜尋 & 趨勢", "熱門品牌排行"])
st.sidebar.markdown("---")
st.sidebar.caption("資料來源：P9 品酒網")

# --- 2. 主頁面內容 ---

if menu == "儀表板總覽":
    st.title("📊 市場概況總覽")
    
    # 取得統計數據
    stats = db_utils.get_dashboard_stats()
    
    # 顯示 KPI 卡片
    c1, c2, c3 = st.columns(3)
    c1.metric("總收錄貼文", f"{stats['total_posts']} 篇")
    c2.metric("活躍品牌數", f"{stats['total_brands']} 個")
    c3.metric("近期新增", f"{stats['recent_posts']} 篇", delta="New")
    
    st.markdown("---")
    
    # --- 最新報價區塊 (修正重點) ---
    st.subheader("🕒 最新 100 筆市場報價")
    st.caption("💡 提示：點擊表格上方的欄位名稱 (如「價格」、「品牌」) 即可進行排序")

    # 1. 取得最新 100 筆資料 (含完整欄位)
    latest_df = db_utils.get_latest_posts(100)
    
    if not latest_df.empty:
        # 2. 整理顯示欄位
        display_df = latest_df[[
            'post_date', 'brand', '標準品名', 'year', 'series', 'style', 'price_per_bottle', 'author', 'post_url'
        ]].copy()
        
        display_df.columns = [
            '日期', '品牌', '品名', '年份', '系列', '桶號/桶型', '單價', '賣家', '前往賣場'
        ]

        # 3. 顯示互動式表格 (支援排序與超連結)
        st.dataframe(
            display_df,
            column_config={
                "單價": st.column_config.NumberColumn(format="$%d"),
                "前往賣場": st.column_config.LinkColumn(
                    "連結", 
                    display_text="🔗", 
                    help="點擊前往 P9 原始貼文"
                ),
                "年份": st.column_config.TextColumn(),
                "系列": st.column_config.TextColumn(),
            },
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("目前資料庫中沒有資料。")

elif menu == "酒款搜尋 & 趨勢":
    st.title("🔎 酒款行情查詢")
    
    col1, col2 = st.columns([3, 1])
    with col1:
        keyword = st.text_input("請輸入酒款關鍵字", "Macallan")
    with col2:
        days_range = st.selectbox("搜尋範圍", [30, 90, 180, 365], index=1, format_func=lambda x: f"最近 {x} 天")
    
    if keyword:
        df_search = db_utils.search_whisky(keyword, days_range)
        
        if not df_search.empty:
            avg_price = int(df_search['price_per_bottle'].mean())
            min_price = int(df_search['price_per_bottle'].min())
            max_price = int(df_search['price_per_bottle'].max())
            count = len(df_search)
            
            st.markdown(f"### 「{keyword}」 市場統計")
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("平均價格", f"${avg_price:,}")
            k2.metric("最低成交", f"${min_price:,}")
            k3.metric("最高成交", f"${max_price:,}")
            k4.metric("資料筆數", f"{count} 筆")
            
            # --- 圖表區 ---
            st.subheader("📈 價格走勢圖")
            chart_df = df_search.copy()
            chart_df['post_date'] = pd.to_datetime(chart_df['post_date'])
            fig = px.scatter(chart_df, x='post_date', y='price_per_bottle', 
                             color='brand', hover_data=['標準品名', 'author', 'series', 'style'],
                             title=f"{keyword} 價格散佈圖")
            fig.add_hline(y=avg_price, line_dash="dash", line_color="red", annotation_text="平均價")
            st.plotly_chart(fig, use_container_width=True)
            
            # --- 詳細資料表 ---
            st.subheader("📋 詳細報價清單")
            
            display_df = df_search[[
                'post_date', 'brand', '標準品名', 'year', 'series', 'style', 'price_per_bottle', 'author', 'post_url'
            ]].copy()
            
            display_df.columns = [
                '日期', '品牌', '品名', '年份', '系列', '桶號/桶型', '單價', '賣家', '前往賣場'
            ]
            
            display_df['系列'] = display_df['系列'].fillna('')

            st.dataframe(
                display_df,
                column_config={
                    "單價": st.column_config.NumberColumn(format="$%d"),
                    "前往賣場": st.column_config.LinkColumn(
                        "連結", 
                        display_text="🔗", 
                        help="點擊前往 P9 原始貼文"
                    ),
                    "年份": st.column_config.TextColumn(),
                    "系列": st.column_config.TextColumn(),
                },
                use_container_width=True,
                hide_index=True
            )
        else:
            st.warning("找不到資料，請嘗試其他關鍵字。")

elif menu == "熱門品牌排行":
    st.title("🔥 熱門品牌風雲榜")
    days = st.slider("統計天數", 7, 365, 30)
    top_df = db_utils.get_top_brands_stats(days, 15)
    c1, c2 = st.columns([1, 2])
    with c1:
        st.subheader("🏆 排行榜")
        st.dataframe(top_df, use_container_width=True, hide_index=True)
    with c2:
        st.subheader("📊 聲量佔比")
        fig = px.bar(top_df, x='品牌', y='貼文數', color='平均價格')
        st.plotly_chart(fig, use_container_width=True)