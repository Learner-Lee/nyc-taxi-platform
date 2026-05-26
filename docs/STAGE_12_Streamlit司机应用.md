# STAGE 12: Streamlit 司机端应用

## 🎯 本阶段目标

- **业务问题**:运营/财务有了 Superset 看板,但**司机**需要的是一个**手机就能用的轻量工具**——"我现在在 XX 区域,这个时段去哪接单最赚钱?"。Superset 太重(给分析师用),司机要的是**一个输入框 + 一个推荐列表**。Streamlit 让数据团队用**纯 Python**(不碰前端)快速交付这种业务工具。
- **技术能力**:
  - 用 Streamlit 纯 Python 写交互式 Web App
  - 连 PostgreSQL 的 ADS 表做实时查询
  - `st.cache_data` 缓存避免频繁查库
  - Docker 打包 Streamlit 应用
- **产出物**:
  - `streamlit/driver_app.py` 司机端推荐应用
  - http://localhost:8501 可访问的 Web App

---

## 📋 前置检查清单

- [ ] STAGE 08 的 PostgreSQL `ads.trip_search`(230k 行)/ `ads.driver_recommendation` 存在
- [ ] core 组运行中(PostgreSQL 在里面)
- [ ] **内存腾挪**:Streamlit 很轻(~256MB),可以停 ClickHouse/Superset/Airflow 腾内存

### ⚠️ 内存腾挪
```bash
cd /Users/alen/DA/NYC-Taxi-Trip-analysis/nyc-taxi-platform
# STAGE 12 只需要 PostgreSQL(core)+ Streamlit,停掉重的 serving 组件
docker compose -f docker/docker-compose.serving.yml stop clickhouse superset airflow
```

---

## 📚 核心概念(3 分钟读完)

### 概念 1:Streamlit 是什么?为什么数据团队爱用?
**业务类比**:做一个网页通常要前端(React)+ 后端(API)+ 部署,数据工程师不会前端。**Streamlit 让你用写 Jupyter 的方式写网页**——`st.title()` 出标题,`st.selectbox()` 出下拉框,`st.dataframe()` 出表格,**零前端知识**。

**适用场景**:内部工具、数据产品原型、ML 模型 demo。**不适合**:高并发 C 端产品(那要正经前后端)。

### 概念 2:st.cache_data 为什么重要?
Streamlit 的执行模型:**每次用户交互(点按钮/改下拉),整个脚本从头跑一遍**。如果每次都查数据库,数据库会被刷爆。

`@st.cache_data` 装饰器:**缓存函数结果**,相同参数不重复查库。司机改了下拉框,只有变了的查询才真正打 DB。

### 概念 3:为什么连 PostgreSQL 不连 ClickHouse?
司机端是**点查/小范围查询**(某 borough+时段的 TOP 5),数据量小、要快、并发高 → **PostgreSQL + 索引**(STAGE 08 建的覆盖索引正好派上用场)。这又是冷热分层 OLTP/OLAP 分工的体现。

---

## 🛠 操作步骤

### 步骤 1:写 Streamlit 应用 `streamlit/driver_app.py`

```python
import streamlit as st
import pandas as pd
import psycopg2

st.set_page_config(page_title="NYC Cab Co. 司机助手", page_icon="🚕", layout="wide")

PG = dict(host="postgres", dbname="nyc_taxi", user="taxi_user", password="taxi_pass123")

@st.cache_data(ttl=300)  # 缓存 5 分钟,避免频繁查库
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

# ── 页面 ──
st.title("🚕 NYC Cab Co. 司机接单助手")
st.caption("基于历史数据,推荐当前时段最赚钱的接单区域")

# 侧边栏:司机选择条件
with st.sidebar:
    st.header("📍 选择你的位置和时段")
    borough = st.selectbox("当前所在区(Borough)", load_boroughs(),
                           index=0)
    time_bucket = st.selectbox("当前时段", 
                               ["morning_rush", "day", "evening_rush", "night"],
                               format_func=lambda x: {
                                   "morning_rush": "早高峰 (7-10)",
                                   "day": "日间 (10-17)",
                                   "evening_rush": "晚高峰 (17-20)",
                                   "night": "夜间 (20-7)"
                               }[x])
    topn = st.slider("推荐数量", 3, 10, 5)

# 主区:推荐结果
df = recommend(borough, time_bucket, topn)

st.subheader(f"💰 {borough} · 该时段 TOP {topn} 最赚钱接单区域")

if df.empty:
    st.warning("该条件下暂无数据,换个区域或时段试试")
else:
    # 三个关键指标卡片
    c1, c2, c3 = st.columns(3)
    c1.metric("推荐区域数", len(df))
    c2.metric("最高单均营收", f"${df['单均营收'].max():.2f}")
    c3.metric("总订单数", f"{int(df['订单数'].sum()):,}")

    # 柱状图 + 表格
    st.bar_chart(df.set_index("接单区域")["单均营收"])
    st.dataframe(df, use_container_width=True, hide_index=True)

    st.success(f"💡 建议:优先去 **{df.iloc[0]['接单区域']}**,"
               f"单均营收 ${df.iloc[0]['单均营收']:.2f}")
```

### 步骤 2:serving compose 补 Streamlit 服务

```yaml
  streamlit:
    image: python:3.11-slim
    platform: linux/amd64
    container_name: streamlit
    ports:
      - "8501:8501"
    volumes:
      - ../streamlit:/app
    working_dir: /app
    command: >
      bash -c "pip install streamlit pandas psycopg2-binary &&
               streamlit run driver_app.py --server.address 0.0.0.0 --server.port 8501"
    networks:
      - taxi-net
```

### 步骤 3:启动 Streamlit

```bash
docker compose -f docker/docker-compose.serving.yml up -d streamlit
# 首次慢(pip install),等 60s
sleep 60
docker logs streamlit 2>&1 | tail -10
```

访问 http://localhost:8501。

### 步骤 4:体验应用

- 侧边栏选 borough = Manhattan,时段 = evening_rush
- 主区显示 TOP 5 最赚钱区域 + 柱状图 + 建议
- 改时段/区域,看推荐实时变化(st.cache_data 让重复查询秒回)

---

## 📊 应用成果展示(实际产出)

![司机接单助手](../streamlit/screenshots/driver_app.png)

**示例查询**:Manhattan + 晚高峰 + TOP 5

| 接单区域 | 订单数 | 营收 | 单均营收 |
|---------|-------|------|---------|
| Randalls Island | 46 | $3,058 | **$66.48** |
| Inwood Hill Park | 10 | $376 | $37.62 |
| Marble Hill | 22 | $799 | $36.33 |
| Roosevelt Island | 65 | $2,202 | $33.88 |
| Battery Park | 474 | $15,693 | $33.11 |

### 应用自己暴露的业务洞察(司机决策核心)
- **Randalls Island 单均 $66.48 但订单仅 46** → 冷门高价值区(岛上活动/长途为主)
- **Battery Park 订单 474 但单均 $33** → 热门短途(下城金融区通勤)
- **司机的真实抉择**:去"人少单价高"还是"人多单价低"——应用把 230 万行明细浓缩成一眼可用的建议

## 🔬 缓存效果观察

1. 第一次选 Manhattan+evening_rush:查库,稍慢
2. 切到别的再切回来:**秒回**(命中 st.cache_data,5 分钟 TTL)
3. 缓存命中时没有新查询打到 PostgreSQL

---

## 💼 简历可写的成果(STAGE 12 新增 1 条)

> • **Streamlit 司机端数据应用**:用纯 Python(零前端)开发司机接单推荐 Web App,连 PostgreSQL ADS 层实时查询"指定区域+时段的 TOP N 最赚钱接单点",用 `st.cache_data` 缓存避免数据库频繁查询;Docker 容器化部署,体现数据团队快速交付业务工具的能力,完成"数据 → 业务价值"闭环的最后一公里(C 端工具)

---

## 🎤 面试可能被问到的问题

1. **Q: Streamlit 和 Flask/Django 写的应用有什么区别?**
   A: Streamlit 是"声明式 + 全脚本重跑"模型,适合数据应用快速开发,零前端;Flask/Django 是正经 Web 框架,要写路由/模板/前端,适合生产 C 端。**Streamlit 的定位是内部工具和原型,不是高并发产品**。

2. **Q: st.cache_data 和 st.cache_resource 区别?**
   A: `cache_data` 缓存"数据"(DataFrame/计算结果,可序列化,每个 session 独立或共享);`cache_resource` 缓存"资源"(数据库连接/ML 模型,全局单例不可序列化)。**查询结果用 cache_data,DB 连接池用 cache_resource**。

3. **Q: 司机端为什么不直接查 Spark/ClickHouse?**
   A: 司机端是高频点查(每个司机每几分钟刷一次),要低延迟 + 高并发。Spark 启动 Job 几百 ms 受不了;ClickHouse 适合聚合不适合点查。**PostgreSQL + 覆盖索引(STAGE 08)是点查最优**——这就是为什么 ADS 层落 PG。

---

## 🧹 阶段收尾

```bash
cd /Users/alen/DA/NYC-Taxi-Trip-analysis/nyc-taxi-platform
git add docs/ streamlit/ README.md
git commit -m "feat(stage12): Streamlit 司机端推荐应用"
```

---

## ➡️ 下一阶段预告(项目收官)

STAGE 13 **项目复盘与简历输出**:
- 梳理完整故事线(13 阶段一条线讲清楚)
- 输出 GitHub README(架构图 + 看板截图 + 量化成果表)
- 简历 bullet 终稿(已在 PROJECT_OVERVIEW 实测版备好)
- 面试题清单复盘(20 道高频题)
- 整个项目收官

需要本阶段产出:Streamlit 应用跑通。**STAGE 13 不需要启动任何新服务**,纯文档整理。
