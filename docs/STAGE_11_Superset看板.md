# STAGE 11: Superset 看板

## 🎯 本阶段目标

- **业务问题**:前 10 阶段挖到的所有洞察(早高峰 53%、机场金矿 3.5x、黑色周日、$2.53 亿总营收)都藏在 SQL 结果里,**业务方看不到**。运营/财务团队要的是**能点能筛的可视化看板**。这一阶段用 Superset 把数据变成"看得见的价值"——这是项目对外交付的门面,也是 GitHub README 里最吸引面试官的截图。
- **技术能力**:
  - Superset 连接多数据源(ClickHouse 运营 + PostgreSQL 财务)
  - 理解 Dataset → Chart → Dashboard 的层次
  - 制作折线图/柱状图/饼图/数据表/热力图
  - 配置 Dashboard 过滤器(点击联动)
- **产出物**:
  - 运营看板(连 ClickHouse):订单趋势、borough×时段、机场占比
  - 财务看板(连 PostgreSQL):营收报表、Top 区域
  - 看板截图(放 GitHub README)

---

## 📋 前置检查清单

- [ ] STAGE 09 的 ClickHouse `nyc.trips`(920 万行)存在
- [ ] STAGE 08 的 PostgreSQL `ads.*` 表存在
- [ ] **内存腾挪**:停 Airflow(STAGE 11 不需要调度),启动 ClickHouse + Superset

### ⚠️ 内存腾挪
```bash
cd /Users/alen/DA/NYC-Taxi-Trip-analysis/nyc-taxi-platform
# 停 Airflow 腾 ~2GB
docker compose -f docker/docker-compose.serving.yml stop airflow
# 重启 ClickHouse(运营看板要连它)
docker compose -f docker/docker-compose.serving.yml start clickhouse
```
预算:core(~13G) + ClickHouse(4G) + Superset(1G) = ~18G,在 24G 内。

---

## 📚 核心概念(3 分钟读完)

### 概念 1:Superset 的三层结构
```
Database(数据库连接)→ Dataset(表/SQL)→ Chart(图表)→ Dashboard(看板)
```
- **Database**:配一个连接字符串(连 ClickHouse / PG)
- **Dataset**:基于某张表或一段 SQL 定义"数据集"
- **Chart**:基于 Dataset 画一个图(折线/柱状/饼图...)
- **Dashboard**:把多个 Chart 拼成一个看板 + 加过滤器

### 概念 2:为什么运营看板连 ClickHouse,财务连 PostgreSQL?
- **运营看板**:大数据量聚合(920 万行扫描),要快 → **ClickHouse**(STAGE 09 验证快 9.6x)
- **财务看板**:固定报表、小数据量、要准 → **PostgreSQL**(STAGE 08 的 ADS 表)
- **这就是冷热分层 + OLAP/OLTP 分工在 BI 层的体现**

### 概念 3:Superset 连 ClickHouse 需要驱动
Superset 默认不带 ClickHouse 驱动,要装 `clickhouse-connect`,连接字符串用 `clickhousedb://`。PostgreSQL 默认支持(自带 psycopg2)。

---

## 🛠 操作步骤

### 步骤 1:serving compose 补 Superset 服务

确认 `docker/docker-compose.serving.yml` 有 superset。如果没有,追加:

```yaml
  superset:
    image: apache/superset:3.0.1
    platform: linux/amd64
    container_name: superset
    environment:
      - SUPERSET_SECRET_KEY=nyc-taxi-superset-secret-key-2024
    ports:
      - "8088:8088"
    volumes:
      - superset-data:/app/superset_home
    command: >
      bash -c "pip install clickhouse-connect &&
               superset db upgrade &&
               (superset fab create-admin --username admin --firstname A --lastname B --email a@b.com --password admin || true) &&
               superset init &&
               superset run -h 0.0.0.0 -p 8088"
    networks:
      - taxi-net
```

并在文件顶部 volumes 加 `superset-data:`。

### 步骤 2:启动 Superset

```bash
docker compose -f docker/docker-compose.serving.yml up -d superset
# 首次很慢(pip install + db upgrade + init),等 2-3 分钟
sleep 150
docker logs superset 2>&1 | tail -15
```

访问 http://localhost:8088(admin/admin)。看到登录页 + 能进首页就成功。

### 步骤 3:连接两个数据源

Superset UI → 右上角 **Settings → Database Connections → + Database**:

**ClickHouse**(运营看板):
- 连接方式选 "Other",SQLAlchemy URI:
  ```
  clickhousedb://default:@clickhouse:8123/nyc
  ```
- Test Connection → 成功 → 保存

**PostgreSQL**(财务看板):
- SQLAlchemy URI:
  ```
  postgresql://taxi_user:taxi_pass123@postgres:5432/nyc_taxi
  ```
- Test Connection → 保存

### 步骤 4:创建 Dataset

UI → **Datasets → + Dataset**:
- ClickHouse 数据源 → database `nyc` → schema → table `trips`
- PostgreSQL 数据源 → schema `ads` → table `borough_summary`(STAGE 10 DAG 刷新的)
- 或用 **SQL Lab** 写自定义查询存成 Virtual Dataset

### 步骤 5:制作运营看板(连 ClickHouse)

推荐 3-4 个 Chart(都基于 `nyc.trips` Dataset):

| Chart | 类型 | 配置要点 |
|-------|------|---------|
| **每日订单趋势** | Line Chart | X 轴 `toDate(tpep_pickup_datetime)`,Metric `COUNT(*)` |
| **区域×时段热力** | Heatmap / Pivot | 行 `pickup_borough`,列 `time_bucket`,值 `SUM(total_amount)` |
| **机场 vs 市区占比** | Pie Chart | 维度 `is_airport_trip`,Metric `COUNT(*)` |
| **Top 10 上车区域** | Bar Chart | 维度 `pickup_zone`,Metric `SUM(total_amount)`,Row limit 10 |

### 步骤 6:制作财务看板(连 PostgreSQL)

基于 `ads.borough_summary` 或 `ads.daily_borough_summary`:

| Chart | 类型 | 配置要点 |
|-------|------|---------|
| **borough 营收排名** | Bar Chart | 维度 `pickup_borough`,Metric `SUM(total_revenue)` |
| **时段营收分布** | Pie / Bar | 维度 `time_bucket`,Metric `SUM(total_revenue)` |
| **营收数据表** | Table | 直接展示明细,带排序 |

### 步骤 7:组装 Dashboard + 过滤器

UI → **Dashboards → + Dashboard**:
- 拖拽 Chart 到画布布局
- 加 **Filter**(如 `pickup_borough` 下拉),点击联动所有图表
- 保存 → 导出截图放 GitHub README

---

## 📊 看板成果展示(实际产出)

### 运营看板(连 ClickHouse)— `superset/dashboards/operations_dashboard.png`
![运营看板](../superset/dashboards/operations_dashboard.png)

| 图表 | 类型 | 看到的洞察 |
|------|------|-----------|
| Top10 上车区域营收 | Bar | **JFK Airport 居首**,Times Sq $6.74M,Midtown/UES 霸榜——印证机场金矿 + 自然倾斜 |
| 机场 vs 市区占比 | Pie | 机场行程约占 1/6 订单,但结合营收占比更高(3.5x 客单价) |
| 每日订单趋势 | Line | 漂亮的周末周期性波动,3 月底见波谷 |
| 区域时段热力图 | Heatmap | Manhattan 列深色,**morning_rush 行整体偏浅(早高峰需求弱一目了然)** |

### 财务看板(连 PostgreSQL)— `superset/dashboards/finance_dashboard.png`
![财务看板](../superset/dashboards/finance_dashboard.png)

| 图表 | 类型 | 看到的洞察 |
|------|------|-----------|
| Borough 营收排名 | Bar | **Manhattan $1.9 亿一骑绝尘**,Queens 第二,断崖式差距 |
| 时段营收分布 | Pie | day 最大块,morning_rush 最小(与热力图相互印证) |
| 每日营收趋势(多 borough) | Line | Manhattan 远高于其他;**3 月底深 V 暴跌正是 STAGE 07 的"黑色周日"** |

### Dashboard 配置
- 2 个 Dashboard,各配 `pickup_borough` Value 过滤器,**跨图表联动下钻**(选 Manhattan → 所有图表只剩 Manhattan)
- 运营图表全部同源(ClickHouse),过滤器联动顺畅

---

## 🔬 看板讲故事:三大洞察可视化复现

| 洞察(SQL 阶段发现) | 看板可视化验证 |
|--------------------|---------------|
| **机场金矿**(STAGE 04,客单价 3.5x) | 饼图机场占比 + Top 区域 JFK 居首 |
| **早高峰弱**(STAGE 04,仅晚高峰 53%) | 热力图 morning_rush 行偏浅 + 时段饼图最小块 |
| **黑色周日**(STAGE 07,2024-03-31 暴跌) | 每日营收趋势多 borough 线,Manhattan 深 V |

**核心价值**:前 10 阶段用 SQL 挖到的洞察,业务方现在**点开看板一眼可见**——这是数据团队把"数据"变成"决策"的终点。

---

## 🛑 踩坑实录(STAGE 11)

### 坑 12:Superset 连 ClickHouse 需要单独装驱动
**现象**:配 ClickHouse 数据源时 `clickhousedb://` dialect 不识别
**根因**:apache/superset 镜像默认不带 ClickHouse 驱动
**修复**:启动 command 里 `pip install clickhouse-connect`,连接字符串用 `clickhousedb://default:@clickhouse:8123/nyc`(注意走 HTTP 端口 8123)

### 坑 13:时间列要标记为 temporal,时间筛选默认值导致图表空白
**现象**:折线图 X 轴选不到时间列,或图表一片空白
**根因**:(1) Superset 默认不把所有 datetime 列识别为可用时间维度;(2) 图表默认时间范围可能是 "Last week",但我们数据是 2024 Q1
**修复**:(1) Dataset 编辑 → Columns → 勾选时间列的 "Is temporal";(2) 图表时间筛选改成 **No filter** 或显式 2024 范围

---

## 💼 简历可写的成果(STAGE 11 新增 1 条)

> • **Superset 企业级 BI 双看板**:基于冷热分层架构搭建运营看板(连 ClickHouse,920 万行实时聚合:订单趋势/区域时段热力图/机场占比/Top 区域)+ 财务看板(连 PostgreSQL ADS 层:Borough 营收排名/时段分布/多 borough 营收趋势),共 7 个图表;配置 `pickup_borough` 过滤器实现跨图表联动下钻;将早晚高峰需求差(53%)、机场金矿效应(客单价 3.5x)、营收异常事件("黑色周日")等数据洞察可视化交付业务方,印证 BI 层 OLAP(CH)/OLTP(PG)分工

---

## 🎤 面试可能被问到的问题

1. **Q: 为什么运营看板连 ClickHouse,财务连 PostgreSQL?**
   A: 运营看板是大数据量实时聚合(920 万行扫描),ClickHouse 列式 + 向量化快 10x;财务报表是固定小数据量、要精确、可能有事务,PostgreSQL 的 ADS 表更合适。**BI 层的数据源选择本质是 OLAP vs OLTP 的延续**。

2. **Q: Superset 的 Dataset 和直接写 SQL 有什么区别?**
   A: Dataset 是"语义层"——定义好维度(dimension)和指标(metric),业务用户拖拽就能出图,不用懂 SQL;Virtual Dataset 可以基于复杂 SQL。**好的 Dataset 设计让自助分析成为可能**,这是 BI 平台的核心价值。

3. **Q: 看板查询慢怎么优化?**
   A: 四个层面——(1) 数据源用 OLAP(ClickHouse);(2) 预聚合(我们的 DWS/ADS 层);(3) Superset 开缓存(cache_timeout);(4) 物化常用查询。**我项目的 DWS 预聚合就是为看板性能铺路**。

---

## 🧹 阶段收尾

```bash
cd /Users/alen/DA/NYC-Taxi-Trip-analysis/nyc-taxi-platform
# 导出看板配置(可选,放 git)
# Superset UI → Dashboard → ... → Export

git add docs/ superset/ README.md
git commit -m "feat(stage11): Superset 运营/财务双看板"
```

---

## ➡️ 下一阶段预告

STAGE 12 **Streamlit 司机端应用**:
- 用纯 Python 写一个司机端 Web App(不需要前端知识)
- 连 PostgreSQL 的 `ads.trip_search` / `driver_recommendation`
- 功能:"当前时段 XX 区域附近最赚钱的接单地点 TOP 5"
- 加 st.cache 避免频繁查库

需要本阶段产出:PostgreSQL ADS 表。**STAGE 12 可以停 ClickHouse/Superset 腾内存**(Streamlit 很轻)。
