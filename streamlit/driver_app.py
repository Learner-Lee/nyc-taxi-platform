import streamlit as st
import pandas as pd
import psycopg2

st.set_page_config(page_title="NYC Cab Co. 司机助手", page_icon="🚕", layout="wide")

PG = dict(host="postgres", dbname="nyc_taxi", user="taxi_user", password="taxi_pass123")

@st.cache_data(ttl=300)
def load_boroughs():
    conn = psycopg2.connect(**PG)
    df = pd.read_sql("SELECT DISTINCT pickup_borough FROM ads.trip_search ORDER BY 1", conn)
    conn.close()
    return df["pickup_borough"].tolist()

@st.cache_data(ttl=300)
def recommend(borough, time_bucket, topn):
    conn = psycopg2.connect(**PG)
    sql = """
        SELECT pickup_zone AS 接单区域,
               SUM(trips) AS 订单数,
               ROUND(SUM(revenue)::numeric, 0) AS 营收,
               ROUND((SUM(revenue)/SUM(trips))::numeric, 2) AS 单均营收
        FROM ads.trip_search
        WHERE pickup_borough = %s AND time_bucket = %s
        GROUP BY pickup_zone
        ORDER BY 单均营收 DESC
        LIMIT %s
    """
    df = pd.read_sql(sql, conn, params=(borough, time_bucket, topn))
    conn.close()
    return df

st.title("🚕 NYC Cab Co. 司机接单助手")
st.caption("基于历史数据,推荐当前时段最赚钱的接单区域")

with st.sidebar:
    st.header("📍 选择你的位置和时段")
    borough = st.selectbox("当前所在区(Borough)", load_boroughs(), index=0)
    time_bucket = st.selectbox("当前时段",
                               ["morning_rush", "day", "evening_rush", "night"],
                               format_func=lambda x: {
                                   "morning_rush": "早高峰 (7-10)",
                                   "day": "日间 (10-17)",
                                   "evening_rush": "晚高峰 (17-20)",
                                   "night": "夜间 (20-7)"
                               }[x])
    topn = st.slider("推荐数量", 3, 10, 5)

df = recommend(borough, time_bucket, topn)
st.subheader(f"💰 {borough} · 该时段 TOP {topn} 最赚钱接单区域")

if df.empty:
    st.warning("该条件下暂无数据,换个区域或时段试试")
else:
    c1, c2, c3 = st.columns(3)
    c1.metric("推荐区域数", len(df))
    c2.metric("最高单均营收", f"${df['单均营收'].max():.2f}")
    c3.metric("总订单数", f"{int(df['订单数'].sum()):,}")
    st.bar_chart(df.set_index("接单区域")["单均营收"])
    st.dataframe(df, use_container_width=True, hide_index=True)
    st.success(f"💡 建议:优先去 **{df.iloc[0]['接单区域']}**,单均营收 ${df.iloc[0]['单均营收']:.2f}")
