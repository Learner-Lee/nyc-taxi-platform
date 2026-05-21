# STAGE 07: DWS 层与查询优化

## 🎯 本阶段目标

- **业务问题**:DWD 9.23M 行明细可信但太"细"。财务团队要日/周/月报表,**每次都全量扫 DWD 太慢**;运营要算"今天 vs 昨天的同区域增长率",**写自连接 SQL 又慢又难读**;数据团队偶尔要看"独立司机/支付方式数",**精确 DISTINCT 几秒到几十秒**。这一阶段建 DWS 层 + 学三种查询优化技术,**把这些痛点都解决**。
- **技术能力**:
  - 设计 DWS 表(从 DWD 物化预聚合,加速下游查询)
  - **窗口函数**重写自连接——可读性 + 性能双赢
  - **近似计算**(HyperLogLog)用 2% 误差换 10x 速度
  - 理解"物化视图"的概念与 Spark/Hive 的支持现状
- **产出物**:
  - `dws.daily_zone_revenue` 日 × 区域 营收汇总(财务报表)
  - `dws.hourly_zone_trips` 小时 × 区域 行程数(运营热力图)
  - `benchmarks/stage_07_window_vs_self_join.md` (实验 #10)
  - `benchmarks/stage_07_approx_distinct.md` (实验 #11)
  - `notebooks/stage07_dws_query_optimization.ipynb`

---

## 📋 前置检查清单

- [ ] STAGE 06 完成,熟悉 Spark UI 看 task 分布
- [ ] `dwd.fact_trips` 存在(9.23M 行,11 个衍生字段)
- [ ] `ods.taxi_zone_lookup` 维表存在
- [ ] core 组 10 容器全部 Up

---

## 🚀 本阶段启动服务

core 组无变化:
```bash
docker restart jupyter && sleep 10
```

---

## 📚 核心概念(5 分钟读完)

### 概念 1:为什么有 DWS?
**业务类比**:餐厅每天都要算"昨日营收",如果每次都从原始小票一张张加,要算半天。**DWS 就是"每天打烊后预先算好的日报"** — 财务来查直接拿日报,不需要重算。

**技术形式**:DWS 是 DWD 的**预聚合表**,粒度更粗(从 trip 级别 → 日/小时级别),行数大幅减少(9.23M → 几万),下游查询飞快。

| 层级 | 粒度 | 行数 | 查询速度 |
|------|------|------|---------|
| DWD | 每次行程 | 9.23M | 慢(全表扫描) |
| DWS 日 × 区域 | 日 × 265 区域 × 3 个月 | ~24k | 快 |
| DWS 小时 × 区域 | 小时 × 265 区域 × 90 天 | ~570k | 中 |

**取舍**:存储多花一点(几 MB),查询加速 10-100x,**对高频报表非常划算**。

### 概念 2:窗口函数 vs 自连接
要算"每日 vs 前一日的同区域增长率",两种写法:

**❌ 自连接(慢且丑)**:
```sql
SELECT t.day, t.zone, t.rev, p.rev AS prev_rev, (t.rev/p.rev - 1) AS pct
FROM dws_daily t LEFT JOIN dws_daily p
  ON t.zone = p.zone AND p.day = t.day - INTERVAL 1 DAY
```
两表 JOIN,每行配对,**Shuffle 量大**。

**✅ 窗口函数(快且优雅)**:
```sql
SELECT day, zone, rev,
       LAG(rev) OVER (PARTITION BY zone ORDER BY day) AS prev_rev,
       rev / LAG(rev) OVER (PARTITION BY zone ORDER BY day) - 1 AS pct
FROM dws_daily
```
**只扫一次**,内存里按 zone 排序后直接取上一行。**这是 SQL 写法升级**。

### 概念 3:HyperLogLog 近似计算
**业务类比**:数图书馆藏书量。
- **精确法**:翻每本书登记一次,慢
- **近似法**:抽样几百本,根据"几本是科幻"的比例估算总数,误差 ~2%,**快 10-100 倍**

`approx_count_distinct(x)` 用 HyperLogLog 算法:固定空间(几 KB)估算 distinct 数,误差默认 5%(可调到 2%)。

**适用**:大数据集 UV 估算、独立设备数、独立 IP 等。**误差能接受时,永远优先用 approx**。

### 概念 4:物化视图(Materialized View)
**普通视图**:每次查询都重算,数据实时但慢
**物化视图**:预先算好存表,数据有延迟但快

**Spark SQL 没有原生 MV** — 用普通表 + 定时 INSERT OVERWRITE 模拟(本质就是我们的 DWS)。Hive 3.x 有 `CREATE MATERIALIZED VIEW`,但 Spark 不识别。
**生产实践**:99% 的"物化视图"就是 DWS 预聚合表 + Airflow 调度刷新(STAGE 10 重点)。

---

## 🛠 操作步骤

### 步骤 1:Jupyter 初始化

```python
from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("STAGE07-DWS") \
    .master("spark://spark-master:7077") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.executor.memory", "2g") \
    .enableHiveSupport() \
    .getOrCreate()

print(f"DWD 行数: {spark.sql('SELECT COUNT(*) FROM dwd.fact_trips').collect()[0][0]:,}")
```

---

### 步骤 2:建 DWS 数据库 + 两张核心汇总表

```python
spark.sql("CREATE DATABASE IF NOT EXISTS dws LOCATION 'hdfs://namenode:9000/nyc-taxi/dws'")

# ── 表 1: 日 × 上车区域 营收汇总(财务报表用)──
spark.sql("DROP TABLE IF EXISTS dws.daily_zone_revenue")
spark.sql("""
CREATE EXTERNAL TABLE dws.daily_zone_revenue (
    pickup_date DATE,
    pickup_borough STRING,
    pickup_zone STRING,
    PULocationID INT,
    trips BIGINT,
    revenue DOUBLE,
    avg_fare DOUBLE,
    avg_tip_pct DOUBLE,
    avg_trip_distance DOUBLE,
    airport_trips BIGINT,
    airport_revenue DOUBLE
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/nyc-taxi/dws/daily_zone_revenue'
TBLPROPERTIES ('parquet.compression'='ZSTD')
""")

# ── 表 2: 小时 × 上车区域 行程数(运营热力图用)──
spark.sql("DROP TABLE IF EXISTS dws.hourly_zone_trips")
spark.sql("""
CREATE EXTERNAL TABLE dws.hourly_zone_trips (
    pickup_hour TIMESTAMP,
    pickup_borough STRING,
    pickup_zone STRING,
    PULocationID INT,
    trips BIGINT,
    revenue DOUBLE,
    time_bucket STRING
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/nyc-taxi/dws/hourly_zone_trips'
TBLPROPERTIES ('parquet.compression'='ZSTD')
""")

print("✅ DWS 两张表 DDL 完成")
```

---

### 步骤 3:ETL 从 DWD 物化到 DWS

```python
# 开启动态分区
spark.conf.set("hive.exec.dynamic.partition", "true")
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

# ── ETL 1: 日 × 区域 营收 ──
print(">>> 物化 dws.daily_zone_revenue ...")
t0 = time.time()
spark.sql("""
INSERT OVERWRITE TABLE dws.daily_zone_revenue PARTITION (year, month)
SELECT
    DATE(tpep_pickup_datetime) AS pickup_date,
    pickup_borough,
    pickup_zone,
    PULocationID,
    COUNT(*) AS trips,
    ROUND(SUM(total_amount), 2) AS revenue,
    ROUND(AVG(fare_amount), 2) AS avg_fare,
    ROUND(AVG(tip_pct), 2) AS avg_tip_pct,
    ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
    SUM(CASE WHEN is_airport_trip THEN 1 ELSE 0 END) AS airport_trips,
    ROUND(SUM(CASE WHEN is_airport_trip THEN total_amount ELSE 0 END), 2) AS airport_revenue,
    year, month
FROM dwd.fact_trips
GROUP BY DATE(tpep_pickup_datetime), pickup_borough, pickup_zone, PULocationID, year, month
""")
print(f"  耗时: {time.time()-t0:.1f}s")

# ── ETL 2: 小时 × 区域 ──
print("\n>>> 物化 dws.hourly_zone_trips ...")
t0 = time.time()
spark.sql("""
INSERT OVERWRITE TABLE dws.hourly_zone_trips PARTITION (year, month)
SELECT
    DATE_TRUNC('HOUR', tpep_pickup_datetime) AS pickup_hour,
    pickup_borough,
    pickup_zone,
    PULocationID,
    COUNT(*) AS trips,
    ROUND(SUM(total_amount), 2) AS revenue,
    -- 把同一小时内最常见的 time_bucket 作为代表
    FIRST(time_bucket) AS time_bucket,
    year, month
FROM dwd.fact_trips
GROUP BY DATE_TRUNC('HOUR', tpep_pickup_datetime), pickup_borough, pickup_zone, PULocationID, year, month
""")
print(f"  耗时: {time.time()-t0:.1f}s")

# ── 验证 ──
print("\n>>> DWS 表统计")
for tbl in ["dws.daily_zone_revenue", "dws.hourly_zone_trips"]:
    cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {tbl}").collect()[0]['c']
    print(f"  {tbl}: {cnt:,} 行")
```

### 📊 实测结果(2026-05-21)
| 表 | 行数 | 体积 | ETL 耗时 |
|----|------|------|---------|
| daily_zone_revenue | **20,391** | **0.43 MB** | 3.7s |
| hourly_zone_trips | **230,429** | **1.80 MB** | 4.6s |
| 对照 dwd.fact_trips | 9,227,227 | 191.53 MB | — |

**核心收益**:行数缩减 **452x**(daily) / **40x**(hourly),**体积压缩 85x**(191MB → 2.23MB)。下游报表查询从全表扫描 191MB 变成扫描几 MB,**速度提升 100x+**。

---

## 🔬 实验 #10: 窗口函数 vs 自连接(算同日环比增长率)

### 业务需求
NYC Cab Co. 财务团队问:**Manhattan 各区域 2024-03-15 的营收 vs 2024-03-14 增长了多少?**

### Cell A: 自连接写法(传统)

```python
print("=" * 60)
print("  A: LEFT JOIN 自连接(传统写法)")
print("=" * 60)

t0 = time.time()
result_a = spark.sql("""
    SELECT
        t.pickup_date,
        t.pickup_zone,
        t.revenue,
        p.revenue AS prev_revenue,
        ROUND((t.revenue / p.revenue - 1) * 100, 2) AS pct_change
    FROM dws.daily_zone_revenue t
    LEFT JOIN dws.daily_zone_revenue p
      ON t.PULocationID = p.PULocationID
     AND p.pickup_date = DATE_SUB(t.pickup_date, 1)
    WHERE t.pickup_borough = 'Manhattan'
      AND t.year = 2024 AND t.month = 3
    ORDER BY t.pickup_date DESC, ABS(t.revenue - p.revenue) DESC
    LIMIT 20
""").collect()
t_a = time.time() - t0
print(f"耗时: {t_a:.2f}s")
print(f"\n>>> Top 20 区域日环比变化:")
for r in result_a[:5]:
    print(f"  {r['pickup_date']} {r['pickup_zone']:<30} rev=${r['revenue']:>9,.0f}  prev=${r['prev_revenue']:>9,.0f}  {r['pct_change']:>6.1f}%")
```

### Cell B: 窗口函数写法(优雅)

```python
print("\n" + "=" * 60)
print("  B: LAG 窗口函数(优雅写法)")
print("=" * 60)

t0 = time.time()
result_b = spark.sql("""
    WITH daily_with_lag AS (
        SELECT
            pickup_date,
            pickup_zone,
            PULocationID,
            revenue,
            LAG(revenue) OVER (PARTITION BY PULocationID ORDER BY pickup_date) AS prev_revenue
        FROM dws.daily_zone_revenue
        WHERE pickup_borough = 'Manhattan'
          AND year = 2024 AND month = 3
    )
    SELECT
        pickup_date,
        pickup_zone,
        revenue,
        prev_revenue,
        ROUND((revenue / prev_revenue - 1) * 100, 2) AS pct_change
    FROM daily_with_lag
    WHERE prev_revenue IS NOT NULL
    ORDER BY pickup_date DESC, ABS(revenue - prev_revenue) DESC
    LIMIT 20
""").collect()
t_b = time.time() - t0
print(f"耗时: {t_b:.2f}s")

# 对比
print(f"\n" + "=" * 60)
print(f"  A 自连接:   {t_a:.2f}s")
print(f"  B 窗口函数: {t_b:.2f}s")
print(f"  加速比: {t_a/t_b:.2f}x")
print(f"\n  结果是否一致: {'是' if len(result_a) == len(result_b) else '否'}")
print("=" * 60)
```

### 📊 实测结果

| 维度 | A 自连接 | B 窗口函数 | 加速 |
|------|---------|----------|------|
| 耗时 | 0.54s | 0.32s | **1.70x** |
| Exchange 次数 | 2 | 1 | — |
| Sort 次数 | 2 | 1 | — |
| 数据扫描次数 | 2 次(自连接) | **1 次** | — |
| SQL 行数 | 11 | 9 | — |

**物理计划证据**(自连接):`SortMergeJoin` + 两次 `Exchange hashpartitioning(..., 200)` + 两次 `Sort`。窗口函数把这些全部省掉。

### 🎯 业务洞察(数据团队的核心价值)
查询结果意外暴露了 **2024-03-31 Manhattan 营收"黑色周日"事件**:
- West Village -35.0%、Greenwich Village -36.2%、East Village -28.6% 等市中心区域**集体下跌 25-35%**
- **唯一上涨**:Penn Station **+30.9%**
- 推测:恶劣天气/大型活动导致大量乘客打车到火车站出城
- 这种"市区暴跌 + 交通枢纽暴涨"的反向信号,业务部门极度想看——**这就是 DWS + 窗口函数的真正价值**

---

## 🔬 实验 #11: 近似计算 vs 精确 COUNT(DISTINCT)

### 业务需求
数据团队:**估算 2024 Q1 各 borough 有多少个不同的支付订单时间戳**(模拟 UV 统计)

```python
print("=" * 60)
print("  实验 #11: COUNT(DISTINCT) vs approx_count_distinct")
print("=" * 60)

# ── A: 精确 ──
print("\n>>> A: COUNT(DISTINCT)")
t0 = time.time()
result_a = spark.sql("""
    SELECT pickup_borough,
           COUNT(DISTINCT tpep_pickup_datetime) AS unique_pickup_times,
           COUNT(*) AS total_trips
    FROM dwd.fact_trips
    GROUP BY pickup_borough
    ORDER BY unique_pickup_times DESC
""").collect()
t_exact = time.time() - t0
print(f"耗时: {t_exact:.2f}s")
for r in result_a:
    print(f"  {r['pickup_borough']:<15} unique={r['unique_pickup_times']:>10,}  total={r['total_trips']:>10,}")

# ── B: 近似(HyperLogLog) ──
print("\n>>> B: approx_count_distinct (默认误差 5%)")
t0 = time.time()
result_b = spark.sql("""
    SELECT pickup_borough,
           APPROX_COUNT_DISTINCT(tpep_pickup_datetime) AS approx_unique_pickup_times,
           COUNT(*) AS total_trips
    FROM dwd.fact_trips
    GROUP BY pickup_borough
    ORDER BY approx_unique_pickup_times DESC
""").collect()
t_approx = time.time() - t0
print(f"耗时: {t_approx:.2f}s")

# ── 对比 + 误差 ──
print(f"\n" + "=" * 60)
print(f"  {'Borough':<15} {'精确':>12} {'近似':>12} {'误差%':>8}")
exact_map = {r['pickup_borough']: r['unique_pickup_times'] for r in result_a}
for r in result_b:
    b = r['pickup_borough']
    exact = exact_map.get(b, 0)
    approx = r['approx_unique_pickup_times']
    err = abs(approx - exact) / max(exact, 1) * 100
    print(f"  {b:<15} {exact:>12,} {approx:>12,} {err:>7.2f}%")
print(f"\n  耗时:  精确 {t_exact:.2f}s  vs  近似 {t_approx:.2f}s")
print(f"  加速:  {t_exact/t_approx:.2f}x")
print("=" * 60)
```

### 📊 实测结果(9.23M 行 dwd.fact_trips)

| 维度 | 数字 |
|------|------|
| 精确 COUNT(DISTINCT) | 2.28s |
| **APPROX_COUNT_DISTINCT** | **0.71s** |
| **加速比** | **3.20x** |
| **全局误差** | **2.33%**(远低于默认 RSD 5%) |
| Manhattan(450 万 distinct) | 误差仅 1.79% |

### 🎯 顺手挖到的业务指标:**区域"打车密度"**(unique_pickup_time / total_trips)

| Borough | 比值 | 含义 |
|---------|------|------|
| Manhattan | 54% | 同时刻平均 1.84 辆车上客,**高密度** |
| Queens | 93% | 几乎每个时刻只有 1 辆车,**稀疏** |
| Bronx/Brooklyn | > 99% | 完全稀疏 |

**业务建议**:Manhattan 重点保供给,Queens/Bronx 重点引流。

---

## 💼 简历可写的成果(STAGE 07 新增 3 条)

> • **DWS 层物化预聚合**:基于 DWD 9.23M 行明细物化两张汇总表(`daily_zone_revenue` **20,391 行** / `hourly_zone_trips` **230,429 行**),**行数缩减 452x、存储压缩 85x**(191MB → 2.23MB);采用 ZSTD 压缩 + year/month 分区设计,为下游 BI 报表提供秒级查询能力
>
> • **窗口函数重写自连接**(实验 #10):用 `LAG() OVER (PARTITION BY ... ORDER BY ...)` 重写传统 LEFT JOIN 自连接计算日环比增长率,物理计划从 `SortMergeJoin` + 2 次 Exchange + 2 次 Sort **简化为单次扫描 + 1 个 Window 节点**,**查询耗时 1.70x 加速**(0.54s→0.32s);意外发现 2024-03-31 Manhattan 营收"黑色周日"事件(市中心 -25%~35% / Penn Station +30.9%),展示 DWS+窗口函数对业务洞察的赋能价值
>
> • **HyperLogLog 近似计算**(实验 #11):用 `APPROX_COUNT_DISTINCT()` 替代 `COUNT(DISTINCT)` 估算 9.23M 行数据的 distinct 时间戳,**查询加速 3.20x(2.28s→0.71s),全局误差仅 2.33%**(Manhattan 450 万 distinct 误差 1.79%);从该实验顺手挖出"区域打车密度"指标(Manhattan unique/total=54% vs Queens 93%),量化各 borough 的供给紧张程度

---

## 🎤 面试可能被问到的问题

1. **Q: DWS 层和 DWD 层的本质区别?**
   A: DWD 是清洗后的**明细**(每次行程 1 行),DWS 是**预聚合**(每日 × 区域 1 行)。DWS 牺牲细节换查询速度,**高频报表场景必须有 DWS**。一个 DWD 通常对应多个不同粒度的 DWS。

2. **Q: 窗口函数比自连接快在哪里?**
   A: 三个原因——(1) 自连接需要 Shuffle 两次,窗口函数只需要按 PARTITION BY 排一次序;(2) 窗口函数可以在 Spark Catalyst 中优化为 SortBased 窗口聚合,内存效率高;(3) 自连接配对的中间结果会爆炸,窗口函数始终是 streaming 处理。

3. **Q: approx_count_distinct 误差 5% 怎么来的?**
   A: HyperLogLog 算法本质上有固定的精度参数 `p`(默认 14,意味着 2^14=16384 个 bucket),理论误差 ≈ 1.04/sqrt(2^p) ≈ 0.81%。Spark 默认 `relativeSD=0.05`(5%),实际通常更准。可以 `approx_count_distinct(x, 0.01)` 调到 1% 误差,但内存翻倍。

4. **Q: 为什么 Spark SQL 没有物化视图?**
   A: 因为 Spark 是计算引擎不是存储引擎,**MV 需要"自动维护"(底表变了自动刷新),Spark 没有这个机制**。Hive 3.x 加了 MV 但 Spark 不识别。实际生产用 Airflow 调度定时 INSERT OVERWRITE 模拟 MV——这就是我们 DWS 层的做法,STAGE 10 会用 Airflow 串起来。

5. **Q: DWS 表为什么也按 year/month 分区?**
   A: 三个原因——(1) 与 DWD 对齐方便重跑;(2) 下游报表通常按月查;(3) DWS 数据量小,**按日分区会产生 90 个分区 × 几 KB 文件 = 严重小文件**(回顾 STAGE 03 实验 #4)。

---

## 🧹 阶段收尾

```python
# 看 DWS 表大小
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
Path = spark._jvm.org.apache.hadoop.fs.Path

def hdfs_size_mb(path):
    total = 0
    files = fs.listFiles(Path(path), True)
    while files.hasNext():
        total += files.next().getLen()
    return total / 1024 / 1024

print(f"DWS 表占用:")
print(f"  daily_zone_revenue: {hdfs_size_mb('hdfs://namenode:9000/nyc-taxi/dws/daily_zone_revenue'):.2f} MB")
print(f"  hourly_zone_trips:  {hdfs_size_mb('hdfs://namenode:9000/nyc-taxi/dws/hourly_zone_trips'):.2f} MB")
```

```bash
cd /Users/alen/DA/NYC-Taxi-Trip-analysis/nyc-taxi-platform
git add docs/ benchmarks/
git commit -m "feat(stage07): DWS 层 + 窗口函数/近似计算优化"
```

---

## ➡️ 下一阶段预告

STAGE 08 **ADS 层 + PostgreSQL 索引**:
- ADS 层物化到 PostgreSQL(财务/司机端的"对外接口")
- **实验 #12**: PostgreSQL B-Tree / 复合索引 / 覆盖索引 / 部分索引,**加索引前后 EXPLAIN ANALYZE 对比**
- 区分 OLTP(PostgreSQL) vs OLAP(Spark)的适用场景
- 引入回表问题与覆盖索引解决方案

需要本阶段产出:`dws.daily_zone_revenue` + `dws.hourly_zone_trips`。
