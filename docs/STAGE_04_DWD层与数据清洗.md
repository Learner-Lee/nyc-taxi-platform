# STAGE 04: DWD 层与数据清洗

## 🎯 本阶段目标

- **业务问题**:STAGE 02 数据剖析时我们识别了一批"匪夷所思"的脏数据(312,722 英里行程、-1000 美元车费、跨年时间戳)。这些数据直接给业务分析会出**严重错误结论**。这一步要把它们洗干净,产出第一张可信的明细表 `dwd.fact_trips`。
- **技术能力**:
  - 设计基于数据剖析的清洗规则(SQL CASE WHEN / WHERE)
  - 掌握维度关联(`yellow_trips` × `taxi_zone_lookup`)
  - **第一次用 Broadcast Join**(广播小表消除 Shuffle)
  - 设计业务衍生字段(时段桶、机场行程标识)
  - 数据质量监控:**前后对比**每条规则筛掉了多少行
- **产出物**:
  - `dwd.fact_trips` 表(分区 + Snappy/Zstd 压缩 + 清洗后明细)
  - `jobs/dwd/build_fact_trips.py` (可重跑的 ETL 脚本)
  - `benchmarks/stage_04_data_quality_report.md` (清洗前后对比)
  - `benchmarks/stage_04_broadcast_vs_shuffle_join.md` (实验 #5 结果)

---

## 📋 前置检查清单

- [ ] STAGE 02/03 完成,`ods.yellow_trips` 表存在,3 个分区 (year=2024/month=1,2,3) 注册到位
- [ ] `ods.taxi_zone_lookup` 维表存在(265 行,Parquet 格式)
- [ ] HDFS `/nyc-taxi/dwd` 目录就绪(STAGE 01 已建)
- [ ] core 组 10 容器全部 Up,Spark Worker 资源未被旧 SparkContext 占满
- [ ] 已经把 STAGE 02 的"脏数据集邮册"在心里过一遍——清洗规则是基于那些观察来的

---

## 🚀 本阶段启动服务

core 组无变化。如果上一次 Jupyter Kernel 已经积累了多个 Spark Session,**建议重启 jupyter 容器一次**,确保 Worker 资源完全释放:

```bash
docker restart jupyter
# 等 10 秒后浏览器刷新 http://localhost:8888,Restart Kernel
```

---

## 📚 核心概念(4 分钟读完)

### 概念 1:为什么要分 ODS 和 DWD?
**ODS = 原料库,DWD = 加工后的零件库**
- ODS 是数据进入数仓的第一站,**原样保留**——可追溯,出问题能回到源头
- DWD 是清洗+标准化后的**事实表**,业务部门拿来分析的"可信底库"
- 如果直接在 ODS 上加 WHERE 清洗,每次查询都重复清洗逻辑,**慢且容易写错**

### 概念 2:数据清洗的两种处理方式
| 方式 | 例子 | 适用场景 |
|------|------|---------|
| **过滤(filter)** | `WHERE total_amount > 0` 直接扔掉负金额行 | 明确是脏数据,业务不需要 |
| **标记(flag)** | 加一列 `is_anomaly = CASE WHEN ... THEN 1 END` | 业务可能要分析"有多少异常单" |
| **修正(impute)** | `passenger_count IS NULL → 默认 1` | 字段大部分有用,只是个别空值 |

**生产实践**:能保留就保留(用标记),不轻易丢数据。但学习项目里**过滤更直观**,我们以过滤为主、修正为辅。

### 概念 3:Broadcast Join 是什么?为什么 265 行的维表必须用?
**业务类比**:你要给 296 万张出租车订单加上"PU 区域名称"(从 265 行的维表查)。两种方式:
- **Shuffle Join**:把订单和维表都按 PULocationID Hash 分到 Worker,**两边都要 Shuffle**(订单 1000 万行 × Shuffle 网络代价 = 灾难)
- **Broadcast Join**:把 265 行的维表**直接复制一份给每个 Worker**(只占几 KB),订单不动,Worker 在本地直接查表(O(1) Hash 查找)

**适用条件**:小表 < `spark.sql.autoBroadcastJoinThreshold`(默认 10MB)。**Spark 通常自动判断,但偶尔会失效**(如统计信息不准),需要手动加 `broadcast()` 提示。

### 概念 4:时间桶(time bucket)的业务价值
出租车业务最常问的"早晚高峰的供需差异",在 SQL 里需要把 0-23 小时映射到几个桶:
- `morning_rush` (07:00-09:59)
- `day` (10:00-16:59)
- `evening_rush` (17:00-19:59)
- `night` (20:00-23:59 + 00:00-06:59)

这是典型的**业务衍生字段**——原始数据里没有,但下游每个分析查询都要算,**放在 DWD 里算一次,DWS/ADS 复用**,符合"算一次用多次"的数仓原则。

---

## 🛠 操作步骤

### 步骤 1:在 Jupyter 里探索清洗规则(先观察,再下手)

**🤔 为什么这么做**:清洗规则不能拍脑袋定。每条规则要回答"这条规则会过滤掉多少行?业务能接受吗?"

```python
from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("STAGE04-DWD清洗") \
    .master("spark://spark-master:7077") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.executor.memory", "2g") \
    .enableHiveSupport() \
    .getOrCreate()

# 各清洗规则会筛掉多少行(逐条诊断)
spark.sql("""
WITH base AS (
    SELECT
        COUNT(*) AS total,
        SUM(CASE WHEN tpep_pickup_datetime IS NULL OR tpep_dropoff_datetime IS NULL THEN 1 ELSE 0 END) AS bad_null_time,
        SUM(CASE WHEN tpep_pickup_datetime < '2024-01-01' OR tpep_pickup_datetime >= '2024-04-01' THEN 1 ELSE 0 END) AS bad_out_of_range,
        SUM(CASE WHEN tpep_dropoff_datetime <= tpep_pickup_datetime THEN 1 ELSE 0 END) AS bad_dropoff_le_pickup,
        SUM(CASE WHEN UNIX_TIMESTAMP(tpep_dropoff_datetime) - UNIX_TIMESTAMP(tpep_pickup_datetime) > 6*3600 THEN 1 ELSE 0 END) AS bad_too_long_trip,
        SUM(CASE WHEN trip_distance <= 0 OR trip_distance > 200 THEN 1 ELSE 0 END) AS bad_distance,
        SUM(CASE WHEN total_amount <= 0 OR total_amount > 1000 THEN 1 ELSE 0 END) AS bad_amount,
        SUM(CASE WHEN passenger_count IS NULL OR passenger_count < 1 OR passenger_count > 6 THEN 1 ELSE 0 END) AS bad_passenger,
        SUM(CASE WHEN PULocationID < 1 OR PULocationID > 265 OR DOLocationID < 1 OR DOLocationID > 265 THEN 1 ELSE 0 END) AS bad_location
    FROM ods.yellow_trips
)
SELECT
    total,
    bad_null_time, ROUND(bad_null_time*100.0/total, 2) AS pct_null_time,
    bad_out_of_range, ROUND(bad_out_of_range*100.0/total, 2) AS pct_out_of_range,
    bad_dropoff_le_pickup, ROUND(bad_dropoff_le_pickup*100.0/total, 2) AS pct_dropoff_bad,
    bad_too_long_trip, ROUND(bad_too_long_trip*100.0/total, 2) AS pct_too_long,
    bad_distance, ROUND(bad_distance*100.0/total, 2) AS pct_bad_distance,
    bad_amount, ROUND(bad_amount*100.0/total, 2) AS pct_bad_amount,
    bad_passenger, ROUND(bad_passenger*100.0/total, 2) AS pct_bad_passenger,
    bad_location, ROUND(bad_location*100.0/total, 2) AS pct_bad_location
FROM base
""").show(vertical=True, truncate=False)
```

**预期发现**:
- `bad_out_of_range` 应该有少量(跨年污染)
- `bad_passenger` 应该 ~8%(STAGE 02 看到空值率 7.87%)
- 其他规则各 0-3%

**关键决策点**:总清洗率应该在 **5-15%** 之间——太少说明规则太松,太多说明规则太严或者数据质量极差。

---

### 步骤 2:设计 DWD 表结构

**🤔 为什么这么做**:DWD 表的字段设计直接影响下游所有分析。多一列要算成本,少一列下游每次都要 JOIN。**好的 DWD 表 = 业务的"宽表",自包含所有常用字段**。

DWD 表会比 ODS 多这些列:
| 列 | 类型 | 来源 |
|----|------|------|
| trip_duration_minutes | DOUBLE | `(dropoff - pickup) / 60` |
| pickup_hour | INT | `HOUR(pickup_datetime)` |
| pickup_dow | INT | `DAYOFWEEK(pickup_datetime)` (1=周日, 7=周六) |
| time_bucket | STRING | CASE WHEN 映射到 morning_rush/day/evening_rush/night |
| is_weekend | BOOLEAN | dow IN (1, 7) |
| is_airport_trip | BOOLEAN | PU 或 DO 在 (1, 132, 138):EWR/JFK/LGA |
| tip_pct | DOUBLE | `tip_amount / fare_amount * 100`(避免除零) |
| pickup_borough | STRING | 维表关联(taxi_zone_lookup) |
| pickup_zone | STRING | 维表关联 |
| dropoff_borough | STRING | 维表关联 |
| dropoff_zone | STRING | 维表关联 |

分区策略:**仍然按 year/month 分区**(与 ODS 对齐,避免按天分区导致的小文件问题——回顾 STAGE 03 实验 #4)。

---

### 步骤 3:编写 ETL 脚本(一次性写好,可重跑)

放到 `jobs/dwd/build_fact_trips.py`,核心代码框架:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, hour, dayofweek, unix_timestamp, lit, broadcast
)

spark = SparkSession.builder \
    .appName("DWD-build-fact-trips") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# 1. 读 ODS 主表 + 维表
trips = spark.table("ods.yellow_trips")
zones = spark.table("ods.taxi_zone_lookup")

# 2. 清洗(用 filter,直观)
clean = trips.filter("""
    tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime IS NOT NULL
    AND tpep_pickup_datetime >= '2024-01-01' AND tpep_pickup_datetime < '2024-04-01'
    AND tpep_dropoff_datetime > tpep_pickup_datetime
    AND UNIX_TIMESTAMP(tpep_dropoff_datetime) - UNIX_TIMESTAMP(tpep_pickup_datetime) <= 6*3600
    AND trip_distance > 0 AND trip_distance <= 200
    AND total_amount > 0 AND total_amount <= 1000
    AND passenger_count IS NOT NULL AND passenger_count BETWEEN 1 AND 6
    AND PULocationID BETWEEN 1 AND 265 AND DOLocationID BETWEEN 1 AND 265
""")

# 3. 添加业务衍生字段
enriched = clean \
    .withColumn("trip_duration_minutes",
                (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60.0) \
    .withColumn("pickup_hour", hour("tpep_pickup_datetime")) \
    .withColumn("pickup_dow", dayofweek("tpep_pickup_datetime")) \
    .withColumn("is_weekend", col("pickup_dow").isin([1, 7])) \
    .withColumn("is_airport_trip",
                col("PULocationID").isin([1, 132, 138]) | col("DOLocationID").isin([1, 132, 138])) \
    .withColumn("time_bucket",
                when((col("pickup_hour") >= 7) & (col("pickup_hour") < 10), "morning_rush")
                .when((col("pickup_hour") >= 10) & (col("pickup_hour") < 17), "day")
                .when((col("pickup_hour") >= 17) & (col("pickup_hour") < 20), "evening_rush")
                .otherwise("night")) \
    .withColumn("tip_pct",
                when(col("fare_amount") > 0, col("tip_amount") / col("fare_amount") * 100).otherwise(0))

# 4. 关联维表(Broadcast Join — 维表只有 265 行)
result = enriched \
    .join(broadcast(zones.select(
        col("LocationID").alias("PULocationID_join"),
        col("Borough").alias("pickup_borough"),
        col("Zone").alias("pickup_zone"))),
        col("PULocationID") == col("PULocationID_join"), "left") \
    .drop("PULocationID_join") \
    .join(broadcast(zones.select(
        col("LocationID").alias("DOLocationID_join"),
        col("Borough").alias("dropoff_borough"),
        col("Zone").alias("dropoff_zone"))),
        col("DOLocationID") == col("DOLocationID_join"), "left") \
    .drop("DOLocationID_join")

# 5. 落地为 DWD 表
result.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .option("compression", "zstd") \
    .parquet("hdfs://namenode:9000/nyc-taxi/dwd/fact_trips")

print("✅ DWD fact_trips 写入完成")
```

**注意细节**:
- `broadcast(zones)` 显式提示 Spark 用广播 Join,避免被 AQE 误判
- `partitionBy("year", "month")` 与 ODS 对齐
- `compression="zstd"` 应用 STAGE 03 的发现
- 写入用 `overwrite` 而不是 `append`——保证 ETL 幂等

---

### 步骤 4:在 Jupyter 跑 ETL + 注册 Hive 表

```python
# 跑 ETL
exec(open("/home/jovyan/work/jobs/dwd/build_fact_trips.py").read())

# 注册外部表
spark.sql("CREATE DATABASE IF NOT EXISTS dwd LOCATION 'hdfs://namenode:9000/nyc-taxi/dwd'")
spark.sql("DROP TABLE IF EXISTS dwd.fact_trips")
spark.sql("""
CREATE EXTERNAL TABLE dwd.fact_trips (
    VendorID INT,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count BIGINT,
    trip_distance DOUBLE,
    RatecodeID BIGINT,
    store_and_fwd_flag STRING,
    PULocationID INT,
    DOLocationID INT,
    payment_type BIGINT,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    congestion_surcharge DOUBLE,
    Airport_fee DOUBLE,
    trip_duration_minutes DOUBLE,
    pickup_hour INT,
    pickup_dow INT,
    is_weekend BOOLEAN,
    is_airport_trip BOOLEAN,
    time_bucket STRING,
    tip_pct DOUBLE,
    pickup_borough STRING,
    pickup_zone STRING,
    dropoff_borough STRING,
    dropoff_zone STRING
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/nyc-taxi/dwd/fact_trips'
""")

# 自动发现并注册所有分区
spark.sql("MSCK REPAIR TABLE dwd.fact_trips")

# 验证
spark.sql("SHOW PARTITIONS dwd.fact_trips").show()
spark.sql("SELECT COUNT(*) FROM dwd.fact_trips").show()
```

---

### 步骤 5:数据质量对比(清洗前后)

```python
ods_cnt = spark.sql("SELECT COUNT(*) AS cnt FROM ods.yellow_trips").collect()[0]['cnt']
dwd_cnt = spark.sql("SELECT COUNT(*) AS cnt FROM dwd.fact_trips").collect()[0]['cnt']
filtered = ods_cnt - dwd_cnt

print(f"ODS 原始:  {ods_cnt:,} 行")
print(f"DWD 清洗后: {dwd_cnt:,} 行")
print(f"过滤掉:    {filtered:,} 行 ({filtered/ods_cnt*100:.2f}%)")
print(f"数据可信率: {dwd_cnt/ods_cnt*100:.2f}%")

# 抽样看清洗后的明细
spark.sql("""
    SELECT tpep_pickup_datetime, pickup_zone, dropoff_zone, time_bucket,
           is_airport_trip, trip_distance, total_amount, tip_pct
    FROM dwd.fact_trips
    LIMIT 10
""").show(truncate=False)
```

**预期看到**:
- 清洗率应该在 5-15% 之间
- 抽样 10 行能看到:**完整的区域名称、合理的金额、清晰的时段桶**——这就是 DWD 层"业务可用"的体感

---

## 🔬 对比实验 #5:Broadcast Join vs Shuffle Join

### 实验目的
亲眼看到广播小表如何**完全消除 Shuffle**。这是 STAGE 05 计算优化的关键铺垫。

### 实验代码

```python
trips = spark.table("ods.yellow_trips").filter("year=2024 AND month=1")
zones = spark.table("ods.taxi_zone_lookup")

# ── 实验 A: 强制 Shuffle Join (关闭 Broadcast)─────────
print("=" * 60)
print("  A: Shuffle Hash Join (autoBroadcastJoinThreshold = -1)")
print("=" * 60)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)  # 禁用自动广播

t0 = time.time()
result_shuffle = trips.join(zones, trips.PULocationID == zones.LocationID, "left") \
                       .agg({"total_amount": "sum"}).collect()
t_shuffle = time.time() - t0
print(f"  耗时: {t_shuffle:.2f}s")

# 看 explain — 应该看到 SortMergeJoin 或 ShuffleHashJoin
trips.join(zones, trips.PULocationID == zones.LocationID, "left").explain()

# ── 实验 B: Broadcast Join (默认开启)────────────────
print("\n" + "=" * 60)
print("  B: Broadcast Join (恢复默认 10MB 阈值)")
print("=" * 60)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10*1024*1024)

t0 = time.time()
result_bc = trips.join(broadcast(zones), trips.PULocationID == zones.LocationID, "left") \
                  .agg({"total_amount": "sum"}).collect()
t_bc = time.time() - t0
print(f"  耗时: {t_bc:.2f}s")

# 看 explain — 应该看到 BroadcastHashJoin
trips.join(broadcast(zones), trips.PULocationID == zones.LocationID, "left").explain()

# ── 汇总 ──────────────────────────────────────────
print("\n" + "=" * 60)
print(f"  Shuffle Join:   {t_shuffle:.2f}s")
print(f"  Broadcast Join: {t_bc:.2f}s")
print(f"  加速比: {t_shuffle/t_bc:.2f}x")
print("=" * 60)
```

### 📊 实测结果(2026-05-17,2024-01 数据 296 万行)

| 维度 | Shuffle Join (SortMergeJoin) | Broadcast Join | 加速 |
|------|------------------------------|----------------|------|
| 耗时 | 0.87s | **0.23s** | **3.75x** |
| 主表 Exchange | hashpartitioning(200 分区) | 无 | — |
| 维表 Exchange | hashpartitioning(200 分区) | BroadcastExchange | — |
| Sort 操作 | 两侧都 Sort | 无 | — |

### EXPLAIN 物理证据
**Shuffle Join (A)**:
```
SortMergeJoin [PULocationID], [LocationID], LeftOuter
:- Sort [PULocationID ASC] + Exchange hashpartitioning(PULocationID, 200)
+- Sort [LocationID ASC] + Exchange hashpartitioning(LocationID, 200)
```

**Broadcast Join (B)**:
```
BroadcastHashJoin [PULocationID], [LocationID], LeftOuter, BuildRight
:- FileScan parquet yellow_trips    ← 主表直接读,零 Exchange
+- BroadcastExchange HashedRelationBroadcastMode  ← 维表广播一次
```

### 决策原则(写到 benchmark)
- 维表 < 10MB(默认 `spark.sql.autoBroadcastJoinThreshold`)→ Spark 自动判断
- 维表稍大但能放进 Executor 内存 → 显式 `/*+ BROADCAST(z) */` 提示
- 维表 > 100MB 或类型不匹配 → Shuffle Join 老老实实跑

---

## 🎯 业务洞察(数据团队的核心价值)

数据清洗+维度关联后,DWD 表立刻能产出**业务部门可消费的 insight**:

### 洞察 1:早晚高峰需求严重不对称(运营调度依据)
| 时段 | 行程数 | 占比 |
|------|-------|------|
| day (10-17) | 3.56M | 38.6% |
| night (20-7) | 2.77M | 30.0% |
| evening_rush (17-20) | 1.90M | 20.5% |
| **morning_rush (7-10)** | **1.01M** | **10.9%** |

**早高峰行程仅为晚高峰的 53%**——早高峰大家赶地铁,晚高峰下班 + 晚餐 + social 更愿意打车。**业务结论**:晚高峰需要重点排班,早高峰可以减少在路车辆。

### 洞察 2:机场是利润金矿(9.5% 行程贡献 ~30% 营收)
| 类型 | 行程数(占比) | 客单价 | 距离 |
|------|------------|-------|------|
| 机场(EWR/JFK/LGA) | 875k(9.5%) | **$77.65** | 13.5 英里 |
| 市区 | 8.35M(90.5%) | $22.18 | 2.2 英里 |

**机场客单价是市区的 3.5x**。**业务结论**:机场需要专门的调度和定价策略,可以做机场专车产品。

---

## 💼 简历可写的成果(STAGE 04 新增 2 条)

> • **DWD 数据清洗与建模**:基于 STAGE 02 数据剖析设计 8 条 SQL 清洗规则,**仅 3.43% 异常率(96.57% 数据可信率)**——其中 r7 乘客数采用"NULL/0 填 1"策略保留 86 万行有效数据;输出标准化事实表 `dwd.fact_trips` 包含 11 个业务衍生字段(时段桶/机场行程/小费比例/区域名称等),通过两次 SQL Hint `/*+ BROADCAST */` 实现 265 行维表零 Shuffle 关联;从清洗后数据直接挖掘出"早高峰需求仅为晚高峰 53%""机场行程 9.5% 贡献 ~30% 营收"两个可执行业务洞察
>
> • **Broadcast Join 量化优化**(实验 #5):对比 `SortMergeJoin`(autoBroadcastJoinThreshold=-1) vs `BroadcastHashJoin`(SQL Hint 显式提示)物理计划,**Join 耗时加速 3.75x**(0.87s→0.23s),主表 296 万行数据**零 Exchange 零 Sort**——EXPLAIN 物理计划提供从 `Exchange hashpartitioning + Sort` 退化为 `BroadcastExchange HashedRelationBroadcastMode` 的直接证据

---

## 🎤 面试可能被问到的问题

1. **Q: 数据清洗规则的来源?怎么确定一个 trip_distance > 200 就是异常?**
   A: 基于 STAGE 02 的数据剖析——看到 max=312,722 英里,远超 NYC 到任何城市的合理距离。200 英里是 NYC 到费城往返的极限,作为保守边界。**规则不能拍脑袋,要有数据剖析作为依据**。

2. **Q: 清洗为什么用 filter 不用 flag(标记)?**
   A: 这个项目里两种都用过。filter 直观、下游查询不用考虑异常;flag 保留数据、可分析"异常单的特征"。**生产实践通常是先 flag,DWS/ADS 层按需 filter**——但学习项目里用 filter 让 DWD 一步到位、教学价值高。

3. **Q: Broadcast Join 什么时候不能用?**
   A: 三种情况:(1) 小表实际大于 `autoBroadcastJoinThreshold`(默认 10MB);(2) 小表统计信息缺失,Spark 不知道它小;(3) Right Outer / Full Outer Join + 大表在被广播侧。第二种最坑——可以用 `ANALYZE TABLE ... COMPUTE STATISTICS` 让 Spark 知道表大小。

4. **Q: DWD 表为什么也按 year/month 分区,不按 pickup_date 按天分区?**
   A: STAGE 03 实验 #4 验证过——文件越小、数量越多,Spark Task 调度开销越大。按天分区一年就 365 个分区,如果每月数据量 50MB,按天每个文件 1.6MB → 严重小文件。按月分区是性能和粒度的平衡。

5. **Q: 业务衍生字段(time_bucket、tip_pct)放 DWD 还是放 DWS?**
   A: 看复用度——time_bucket 几乎每个分析都用,放 DWD"算一次用多次";tip_pct 只用于"小费分析"场景,可以延后到 DWS。**原则:高复用字段放 DWD,场景特定字段放 DWS**。

---

## 🧹 阶段收尾

```python
# 看 DWD 表占用了多少空间
size_mb = hdfs_size_mb("hdfs://namenode:9000/nyc-taxi/dwd/fact_trips")
print(f"DWD fact_trips: {size_mb:.2f} MB ({dwd_cnt:,} 行)")
```

```bash
# git commit
cd /Users/alen/DA/NYC-Taxi-Trip-analysis/nyc-taxi-platform
git add docs/ benchmarks/ jobs/
git commit -m "feat(stage04): DWD 层清洗 + Broadcast Join 优化"
```

---

## ➡️ 下一阶段预告

STAGE 05 **计算层深度优化**:
- 谓词下推 + 列裁剪的更深入实验(看 explain 的优化过程)
- AQE(Adaptive Query Execution)开关对比 — 同一查询开关 AQE,执行计划差异
- CBO(基于代价的优化) — 通过 `ANALYZE TABLE COMPUTE STATISTICS` 让 Spark 看到数据真相
- 引入 Spark UI 的 SQL 标签页深度阅读

需要本阶段产出:稳定的 `dwd.fact_trips`(2024 Q1 清洗后明细)+ Broadcast Join 的体感。
