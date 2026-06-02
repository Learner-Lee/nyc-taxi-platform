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

# 初始化 Spark 会话
spark = SparkSession.builder \
    .appName("STAGE04-DWD清洗诊断") \
    .master("spark://spark-master:7077") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.executor.memory", "2g") \
    .enableHiveSupport() \
    .getOrCreate()

# 8 条清洗规则过滤率诊断（垂直展示，方便查看大量字段）
spark.sql("""
WITH base AS (
    SELECT
        COUNT(*) AS total_rows,
        
        -- 规则1：时间字段非空
        SUM(CASE WHEN tpep_pickup_datetime IS NULL OR tpep_dropoff_datetime IS NULL
                 THEN 1 ELSE 0 END) AS r1_null_time,
        
        -- 规则2：时间在 2024 Q1 范围内
        SUM(CASE WHEN tpep_pickup_datetime < '2024-01-01'
                  OR tpep_pickup_datetime >= '2024-04-01'
                 THEN 1 ELSE 0 END) AS r2_out_of_range,
        
        -- 规则3：下车时间 > 上车时间（逻辑合法）
        SUM(CASE WHEN tpep_dropoff_datetime <= tpep_pickup_datetime
                 THEN 1 ELSE 0 END) AS r3_dropoff_le_pickup,
        
        -- 规则4：行程时长 ≤ 6 小时
        SUM(CASE WHEN UNIX_TIMESTAMP(tpep_dropoff_datetime) -
                      UNIX_TIMESTAMP(tpep_pickup_datetime) > 6 * 3600
                 THEN 1 ELSE 0 END) AS r4_too_long,
        
        -- 规则5：行程距离合法 (0, 200]
        SUM(CASE WHEN trip_distance <= 0 OR trip_distance > 200
                 THEN 1 ELSE 0 END) AS r5_bad_distance,
        
        -- 规则6：总金额合法 (0, 1000]
        SUM(CASE WHEN total_amount <= 0 OR total_amount > 1000
                 THEN 1 ELSE 0 END) AS r6_bad_amount,
        
        -- 规则7：乘客数合法 [1,6]（空值也过滤）
        SUM(CASE WHEN passenger_count IS NULL
                  OR passenger_count < 1 OR passenger_count > 6
                 THEN 1 ELSE 0 END) AS r7_bad_passenger,
        
        -- 规则8：上下车区域 ID 合法 [1,265]
        SUM(CASE WHEN PULocationID < 1 OR PULocationID > 265
                  OR DOLocationID < 1 OR DOLocationID > 265
                 THEN 1 ELSE 0 END) AS r8_bad_location
    FROM ods.yellow_trips
)
SELECT
    total_rows,
    r1_null_time,         ROUND(r1_null_time         * 100.0 / total_rows, 2) AS pct_r1,
    r2_out_of_range,      ROUND(r2_out_of_range      * 100.0 / total_rows, 2) AS pct_r2,
    r3_dropoff_le_pickup, ROUND(r3_dropoff_le_pickup * 100.0 / total_rows, 2) AS pct_r3,
    r4_too_long,          ROUND(r4_too_long          * 100.0 / total_rows, 2) AS pct_r4,
    r5_bad_distance,      ROUND(r5_bad_distance      * 100.0 / total_rows, 2) AS pct_r5,
    r6_bad_amount,        ROUND(r6_bad_amount        * 100.0 / total_rows, 2) AS pct_r6,
    r7_bad_passenger,     ROUND(r7_bad_passenger     * 100.0 / total_rows, 2) AS pct_r7,
    r8_bad_location,      ROUND(r8_bad_location      * 100.0 / total_rows, 2) AS pct_r8
FROM base
""").show(vertical=True, truncate=False)
```

```ini
-RECORD 0-----------------------
 total_rows           | 9554778 
 r1_null_time         | 0       
 pct_r1               | 0.00    
 r2_out_of_range      | 21      
 pct_r2               | 0.00    
 r3_dropoff_le_pickup | 2801    
 pct_r3               | 0.03    
 r4_too_long          | 5435    
 pct_r4               | 0.06    
 r5_bad_distance      | 215934  
 pct_r5               | 2.26    
 r6_bad_amount        | 117196  
 pct_r6               | 1.23    
 r7_bad_passenger     | 857992  
 pct_r7               | 8.98    
 r8_bad_location      | 0       
 pct_r8               | 0.00 
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

**维度字段**

> 原始 ODS 数据里只有区域 ID,没有区域名:
> PULocationID = 132    ← 只有数字,人看不懂
> DOLocationID = 161
>
> DWD 层通过 Broadcast Join 关联 taxi_zone_lookup 维表(265 行),把 ID 翻译成人能看懂的名字

这叫 **反范式(Denormalization)/ 宽表设计**——**空间换时间**

| 方案                            | 代价                                |
| ------------------------------- | ----------------------------------- |
| ❌ 不存名字,每次查询时 JOIN 维表 | 下游每个查询都要 JOIN 一次,重复劳动 |
| ✅ DWD 提前 JOIN 好存进去        | 多占一点存储,但下游零 JOIN 直接用   |

**为什么分 borough 和 zone 两个粒度?**

| 字段           | 粒度                                                         | 用途                       |
| -------------- | ------------------------------------------------------------ | -------------------------- |
| pickup_borough | 粗(5 个行政区:Manhattan/Queens/Brooklyn/Bronx/Staten Island) | 高层报表、过滤器、宏观分析 |
| pickup_zone    | 细(265 个具体区域:JFK Airport/Midtown Center...)             | 司机精确推荐、热点定位     |

**为什么 PU 和 DO 都存?(上车 + 下车)**

  - pickup_*:供给分析——司机在哪接单最赚钱(司机端主用)
  - dropoff_*:流向分析——乘客都去哪(可做"潮汐调度":早高峰从住宅区流向商务区)

分区策略:**仍然按 year/month 分区**(与 ODS 对齐,避免按天分区导致的小文件问题——回顾 STAGE 03 实验 #4)。

```sql
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
    -- 业务衍生字段
    trip_duration_minutes DOUBLE,
    pickup_hour INT,
    pickup_dow INT,
    is_weekend BOOLEAN,
    is_airport_trip BOOLEAN,
    time_bucket STRING,
    tip_pct DOUBLE,
    -- 维度字段
    pickup_borough STRING,
    pickup_zone STRING,
    dropoff_borough STRING,
    dropoff_zone STRING
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET -- 
LOCATION 'hdfs://namenode:9000/nyc-taxi/dwd/fact_trips'
TBLPROPERTIES ('parquet.compression'='ZSTD')
""")

print("✅ DDL 完成,dwd.fact_trips 空表已建好(ZSTD 压缩 + year/month 分区)")
```



---

### 步骤 3:核心 ETL SQL — INSERT OVERWRITE(清洗 + 维表关联 + 衍生字段一步到位)

```python
# 开启动态分区(分区值来自 SELECT 列而不是手动指定)
spark.conf.set("hive.exec.dynamic.partition", "true")
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

import time
t0 = time.time()

spark.sql("""
INSERT OVERWRITE TABLE dwd.fact_trips PARTITION (year, month)
SELECT /*+ BROADCAST(pu_z, do_z) */
    t.VendorID,
    t.tpep_pickup_datetime,
    t.tpep_dropoff_datetime,
    -- r7 修正: NULL/0 → 1
    CASE WHEN t.passenger_count IS NULL OR t.passenger_count = 0 THEN 1
         ELSE t.passenger_count END AS passenger_count,
    t.trip_distance,
    t.RatecodeID,
    t.store_and_fwd_flag,
    t.PULocationID,
    t.DOLocationID,
    t.payment_type,
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.improvement_surcharge,
    t.total_amount,
    t.congestion_surcharge,
    t.Airport_fee,
    -- 业务衍生字段
    (UNIX_TIMESTAMP(t.tpep_dropoff_datetime) - UNIX_TIMESTAMP(t.tpep_pickup_datetime)) / 60.0
        AS trip_duration_minutes,
    HOUR(t.tpep_pickup_datetime) AS pickup_hour,
    DAYOFWEEK(t.tpep_pickup_datetime) AS pickup_dow,
    DAYOFWEEK(t.tpep_pickup_datetime) IN (1, 7) AS is_weekend,
    (t.PULocationID IN (1, 132, 138) OR t.DOLocationID IN (1, 132, 138)) AS is_airport_trip,
    CASE
        WHEN HOUR(t.tpep_pickup_datetime) BETWEEN 7 AND 9   THEN 'morning_rush'
        WHEN HOUR(t.tpep_pickup_datetime) BETWEEN 10 AND 16 THEN 'day'
        WHEN HOUR(t.tpep_pickup_datetime) BETWEEN 17 AND 19 THEN 'evening_rush'
        ELSE 'night'
    END AS time_bucket,
    CASE WHEN t.fare_amount > 0 THEN t.tip_amount / t.fare_amount * 100 ELSE 0.0 END AS tip_pct,
    -- 维度字段(通过 Broadcast Join 关联)
    pu_z.Borough AS pickup_borough,
    pu_z.Zone    AS pickup_zone,
    do_z.Borough AS dropoff_borough,
    do_z.Zone    AS dropoff_zone,
    -- 动态分区列必须在 SELECT 末尾
    t.year,
    t.month
FROM ods.yellow_trips t
LEFT JOIN ods.taxi_zone_lookup pu_z ON t.PULocationID = pu_z.LocationID
LEFT JOIN ods.taxi_zone_lookup do_z ON t.DOLocationID = do_z.LocationID
WHERE
    t.tpep_pickup_datetime IS NOT NULL
    AND t.tpep_dropoff_datetime IS NOT NULL
    AND t.tpep_pickup_datetime >= '2024-01-01'
    AND t.tpep_pickup_datetime < '2024-04-01'
    AND t.tpep_dropoff_datetime > t.tpep_pickup_datetime
    AND UNIX_TIMESTAMP(t.tpep_dropoff_datetime)
        - UNIX_TIMESTAMP(t.tpep_pickup_datetime) <= 6 * 3600
    AND t.trip_distance > 0 AND t.trip_distance <= 200
    AND t.total_amount > 0 AND t.total_amount <= 1000
    AND (t.passenger_count IS NULL OR t.passenger_count <= 6)
    AND t.PULocationID BETWEEN 1 AND 265
    AND t.DOLocationID BETWEEN 1 AND 265
""")

print(f"✅ ETL 完成,耗时 {time.time()-t0:.1f}s")
```

**/*+ BROADCAST(pu_z, do_z) */:广播小表**

```SQL
SELECT /*+ BROADCAST(pu_z, do_z) */
```

告诉 Spark:把 pu_z 和 do_z(都是 265 行的维表)广播到每个 Executor,而不是 Shuffle。
  - 920 万行的主表不动,265 行的维表复制一份给每个 Worker,本地查表
  - 没这个 hint,Spark 可能选 SortMergeJoin,两边都 Shuffle(慢)——STAGE 04 实验 #5 实测广播快 3.75x

---

### 步骤 4:数据质量对比(清洗前后)

```python
# ── 1. 清洗前后行数对比 ─────────────────────────
ods_cnt = spark.sql("SELECT COUNT(*) AS cnt FROM ods.yellow_trips").collect()[0]['cnt']
dwd_cnt = spark.sql("SELECT COUNT(*) AS cnt FROM dwd.fact_trips").collect()[0]['cnt']
filtered = ods_cnt - dwd_cnt

print("=" * 55)
print("  ETL 数据质量对比")
print("=" * 55)
print(f"  ODS 原始:    {ods_cnt:>12,} 行")
print(f"  DWD 清洗后:  {dwd_cnt:>12,} 行")
print(f"  过滤掉:      {filtered:>12,} 行 ({filtered/ods_cnt*100:.2f}%)")
print(f"  数据可信率:  {dwd_cnt/ods_cnt*100:.2f}%")
print("=" * 55)

# ── 2. 分区是否都注册成功 ────────────────────────
print("\n>>> 分区列表(应该看到 3 个分区):")
spark.sql("SHOW PARTITIONS dwd.fact_trips").show()

# ── 3. 各月数据分布(对比 ODS 看清洗均匀性)─────
print(">>> 清洗后各月数据分布:")
spark.sql("""
    SELECT year, month, COUNT(*) AS dwd_rows
    FROM dwd.fact_trips
    GROUP BY year, month
    ORDER BY year, month
""").show()

# ── 4. 抽样看 10 行(验证维度关联 + 衍生字段)──
print(">>> 抽样 10 行(看维度关联和衍生字段)")
spark.sql("""
    SELECT
        DATE_FORMAT(tpep_pickup_datetime, 'MM-dd HH:mm') AS pickup_time,
        ROUND(trip_duration_minutes, 1) AS dur_min,
        pickup_zone,
        dropoff_zone,
        time_bucket,
        is_airport_trip AS airport,
        passenger_count AS pax,
        ROUND(total_amount, 2) AS amt,
        ROUND(tip_pct, 1) AS tip_pct
    FROM dwd.fact_trips
    LIMIT 10
""").show(truncate=False)

# ── 5. 业务字段合理性聚合(time_bucket 分布)────
print(">>> 时段桶分布(早晚高峰 vs 平峰):")
spark.sql("""
    SELECT time_bucket,
           COUNT(*) AS trips,
           ROUND(AVG(total_amount), 2) AS avg_amount,
           ROUND(AVG(trip_duration_minutes), 1) AS avg_duration
    FROM dwd.fact_trips
    GROUP BY time_bucket
    ORDER BY trips DESC
""").show()

# ── 6. 机场行程占比(业务上 NYC 机场行程约 5-8%)─
print(">>> 机场行程统计:")
spark.sql("""
    SELECT is_airport_trip,
           COUNT(*) AS trips,
           ROUND(AVG(total_amount), 2) AS avg_amount,
           ROUND(AVG(trip_distance), 1) AS avg_distance
    FROM dwd.fact_trips
    GROUP BY is_airport_trip
""").show()

# ── 7. DWD 文件大小(看 Zstd 压缩效果)──────────
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
Path = spark._jvm.org.apache.hadoop.fs.Path

def hdfs_size_mb(path):
    total = 0
    files = fs.listFiles(Path(path), True)
    while files.hasNext():
        total += files.next().getLen()
    return total / 1024 / 1024

size_mb = hdfs_size_mb("hdfs://namenode:9000/nyc-taxi/dwd/fact_trips")
print(f">>> DWD 表 HDFS 占用: {size_mb:.2f} MB  (vs ODS 153 MB)")
print(f">>> 压缩 + 多字段后体积比: {size_mb/153:.2f}x (含 11 个衍生/维度字段)")
```

```
=======================================================
  ETL 数据质量对比
=======================================================
  ODS 原始:       9,554,778 行
  DWD 清洗后:     9,227,227 行
  过滤掉:           327,551 行 (3.43%)
  数据可信率:  96.57%
=======================================================

>>> 分区列表(应该看到 3 个分区):
+-----------------+
|        partition|
+-----------------+
|year=2024/month=1|
|year=2024/month=2|
|year=2024/month=3|
+-----------------+

>>> 清洗后各月数据分布:
+----+-----+--------+
|year|month|dwd_rows|
+----+-----+--------+
|2024|    1| 2870077|
|2024|    2| 2904523|
|2024|    3| 3452627|
+----+-----+--------+

>>> 抽样 10 行(看维度关联和衍生字段)
+-----------+-------+----------------------------+---------------------+-----------+-------+---+-----+-------+
|pickup_time|dur_min|pickup_zone                 |dropoff_zone         |time_bucket|airport|pax|amt  |tip_pct|
+-----------+-------+----------------------------+---------------------+-----------+-------+---+-----+-------+
|01-01 00:57|19.8   |Penn Station/Madison Sq West|East Village         |night      |false  |1  |22.7 |0.0    |
|01-17 14:01|15.4   |Upper East Side South       |Midtown East         |day        |false  |1  |23.9 |33.6   |
|01-01 00:03|6.6    |Lenox Hill East             |Upper East Side North|night      |false  |1  |18.75|37.5   |
|01-17 14:07|12.7   |Union Sq                    |Midtown South        |day        |false  |2  |18.1 |16.5   |
|01-01 00:17|17.9   |Upper East Side North       |East Village         |night      |false  |1  |31.3 |12.9   |
|01-17 14:00|8.8    |Lenox Hill West             |Upper East Side North|day        |false  |1  |15.4 |14.0   |
|01-01 00:36|8.3    |East Village                |SoHo                 |night      |false  |1  |17.0 |20.0   |
|01-17 14:43|2.7    |Upper East Side North       |Upper East Side North|day        |false  |1  |10.08|38.2   |
|01-01 00:46|6.1    |SoHo                        |Lower East Side      |night      |false  |1  |16.1 |40.5   |
|01-17 14:08|7.9    |Lincoln Square East         |Upper West Side South|day        |false  |1  |16.8 |28.0   |
+-----------+-------+----------------------------+---------------------+-----------+-------+---+-----+-------+

>>> 时段桶分布(早晚高峰 vs 平峰):
+------------+-------+----------+------------+
| time_bucket|  trips|avg_amount|avg_duration|
+------------+-------+----------+------------+
|         day|3557361|     27.65|        16.9|
|       night|2766591|     27.62|        14.0|
|evening_rush|1895736|     27.63|        15.4|
|morning_rush|1007539|     25.89|        15.3|
+------------+-------+----------+------------+

>>> 机场行程统计:
+---------------+-------+----------+------------+
|is_airport_trip|  trips|avg_amount|avg_distance|
+---------------+-------+----------+------------+
|           true| 875077|     77.65|        13.5|
|          false|8352150|     22.18|         2.2|
+---------------+-------+----------+------------+

>>> DWD 表 HDFS 占用: 191.53 MB  (vs ODS 153 MB)
>>> 压缩 + 多字段后体积比: 1.25x (含 11 个衍生/维度字段)
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
import time

# ── 实验 A: 禁用自动广播,强制 Shuffle Join ────
print("=" * 60)
print("  A: Shuffle Hash Join (autoBroadcastJoinThreshold = -1)")
print("=" * 60)

# 临时禁用自动广播
spark.sql("SET spark.sql.autoBroadcastJoinThreshold = -1")

t0 = time.time()
result_a = spark.sql("""
    SELECT SUM(t.total_amount) AS total_rev
    FROM ods.yellow_trips t
    LEFT JOIN ods.taxi_zone_lookup z ON t.PULocationID = z.LocationID
    WHERE t.year = 2024 AND t.month = 1
""").collect()
t_shuffle = time.time() - t0
print(f"  耗时: {t_shuffle:.2f}s, 结果: {result_a[0]['total_rev']:,.2f}")

print("\n--- Shuffle Join 物理计划(应该看到 SortMergeJoin 或 ShuffleHashJoin)---")
spark.sql("""
    EXPLAIN
    SELECT SUM(t.total_amount)
    FROM ods.yellow_trips t
    LEFT JOIN ods.taxi_zone_lookup z ON t.PULocationID = z.LocationID
    WHERE t.year = 2024 AND t.month = 1
""").show(truncate=False)

# ── 实验 B: SQL Hint 显式广播 ────────────────────
print("\n" + "=" * 60)
print("  B: Broadcast Hash Join (SQL Hint /*+ BROADCAST(z) */)")
print("=" * 60)

# 恢复默认阈值
spark.sql("SET spark.sql.autoBroadcastJoinThreshold = 10485760")

t0 = time.time()
result_b = spark.sql("""
    SELECT /*+ BROADCAST(z) */ SUM(t.total_amount) AS total_rev
    FROM ods.yellow_trips t
    LEFT JOIN ods.taxi_zone_lookup z ON t.PULocationID = z.LocationID
    WHERE t.year = 2024 AND t.month = 1
""").collect()
t_bc = time.time() - t0
print(f"  耗时: {t_bc:.2f}s, 结果: {result_b[0]['total_rev']:,.2f}")

print("\n--- Broadcast Join 物理计划(应该看到 BroadcastHashJoin)---")
spark.sql("""
    EXPLAIN
    SELECT /*+ BROADCAST(z) */ SUM(t.total_amount)
    FROM ods.yellow_trips t
    LEFT JOIN ods.taxi_zone_lookup z ON t.PULocationID = z.LocationID
    WHERE t.year = 2024 AND t.month = 1
""").show(truncate=False)

# ── 汇总 ──────────────────────────────────────────
print("\n" + "=" * 60)
print(f"  Shuffle Join:   {t_shuffle:.2f}s")
print(f"  Broadcast Join: {t_bc:.2f}s")
print(f"  加速比: {t_shuffle/t_bc:.2f}x")
print("=" * 60)
print("\n💡 同时打开 http://localhost:8080 看两次 Job 的 Stage,")
print("   重点对比 'Shuffle Read/Write Size' 字段")
print("   - Shuffle Join:应该有 MB-GB 级 Shuffle Read/Write")
print("   - Broadcast Join:Shuffle 应该接近 0")
```

```
============================================================
  A: Shuffle Hash Join (autoBroadcastJoinThreshold = -1)
============================================================
  耗时: 0.87s, 结果: 79,456,384.28

--- Shuffle Join 物理计划(应该看到 SortMergeJoin 或 ShuffleHashJoin)---
+--------------------------------------------------------------------------------------------------------------------------+
|plan|
+--------------------------------------------------------------------------------------------------------------------------+
|== Physical Plan ==\nAdaptiveSparkPlan isFinalPlan=false\n+- HashAggregate(keys=[], functions=[sum(total_amount#1340)])\n   +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=1055]\n      +- HashAggregate(keys=[], functions=[partial_sum(total_amount#1340)])\n         +- Project [total_amount#1340]\n            +- SortMergeJoin [PULocationID#1331], [LocationID#1345], LeftOuter\n               :- Sort [PULocationID#1331 ASC NULLS FIRST], false, 0\n               :  +- Exchange hashpartitioning(PULocationID#1331, 200), ENSURE_REQUIREMENTS, [plan_id=1047]\n               :     +- Project [PULocationID#1331, total_amount#1340]\n               :        +- FileScan parquet spark_catalog.ods.yellow_trips[PULocationID#1331,total_amount#1340,year#1343,month#1344] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[hdfs://namenode:9000/nyc-taxi/ods/yellow_trips/year=2024/month=01], PartitionFilters: [isnotnull(year#1343), isnotnull(month#1344), (year#1343 = 2024), (month#1344 = 1)], PushedFilters: [], ReadSchema: struct<PULocationID:int,total_amount:double>\n               +- Sort [LocationID#1345 ASC NULLS FIRST], false, 0\n                  +- Exchange hashpartitioning(LocationID#1345, 200), ENSURE_REQUIREMENTS, [plan_id=1048]\n                     +- Filter isnotnull(LocationID#1345)\n                        +- FileScan parquet spark_catalog.ods.taxi_zone_lookup[LocationID#1345] Batched: true, DataFilters: [isnotnull(LocationID#1345)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[hdfs://namenode:9000/nyc-taxi/ods/taxi_zone_lookup_parquet], PartitionFilters: [], PushedFilters: [IsNotNull(LocationID)], ReadSchema: struct<LocationID:int>\n\n|
+--------------------------------------------------------------------------------------------------------------------------+


============================================================
  B: Broadcast Hash Join (SQL Hint /*+ BROADCAST(z) */)
============================================================
  耗时: 0.23s, 结果: 79,456,384.28

--- Broadcast Join 物理计划(应该看到 BroadcastHashJoin)---
+--------------------------------------------------------------------------------------------------------------------------+
|plan|
+--------------------------------------------------------------------------------------------------------------------------+
|== Physical Plan ==\nAdaptiveSparkPlan isFinalPlan=false\n+- HashAggregate(keys=[], functions=[sum(total_amount#1422)])\n   +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=1240]\n      +- HashAggregate(keys=[], functions=[partial_sum(total_amount#1422)])\n         +- Project [total_amount#1422]\n            +- BroadcastHashJoin [PULocationID#1413], [LocationID#1427], LeftOuter, BuildRight, false\n               :- Project [PULocationID#1413, total_amount#1422]\n               :  +- FileScan parquet spark_catalog.ods.yellow_trips[PULocationID#1413,total_amount#1422,year#1425,month#1426] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[hdfs://namenode:9000/nyc-taxi/ods/yellow_trips/year=2024/month=01], PartitionFilters: [isnotnull(year#1425), isnotnull(month#1426), (year#1425 = 2024), (month#1426 = 1)], PushedFilters: [], ReadSchema: struct<PULocationID:int,total_amount:double>\n               +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1235]\n                  +- Filter isnotnull(LocationID#1427)\n                     +- FileScan parquet spark_catalog.ods.taxi_zone_lookup[LocationID#1427] Batched: true, DataFilters: [isnotnull(LocationID#1427)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[hdfs://namenode:9000/nyc-taxi/ods/taxi_zone_lookup_parquet], PartitionFilters: [], PushedFilters: [IsNotNull(LocationID)], ReadSchema: struct<LocationID:int>\n\n|
+--------------------------------------------------------------------------------------------------------------------------+


============================================================
  Shuffle Join:   0.87s
  Broadcast Join: 0.23s
  加速比: 3.75x
============================================================

💡 同时打开 http://localhost:8080 看两次 Job 的 Stage,
   重点对比 'Shuffle Read/Write Size' 字段
   - Shuffle Join:应该有 MB-GB 级 Shuffle Read/Write
   - Broadcast Join:Shuffle 应该接近 0
```

### Spark Join有两种方式

**方式 A:Shuffle Join(普通方式,要 Shuffle)**

解决"不在一起"的办法:把两张表都按 key 重新分区(这就是 Shuffle)。

Shuffle 规则:hash(key) % 分区数 → 决定去哪个 Worker
主表的 132 → hash(132) → Worker-2
维表的 132 → hash(132) → Worker-2   ← 相同 key 算出相同目标,必然汇合

代价:
  - 主表 920 万行全部要按 key 重新洗牌,跨网络搬到对应 Worker
  - 维表 265 行也要洗牌
  - 920 万行的网络传输 = 灾难



**方式 B:Broadcast Join(广播,不要 Shuffle)**

换个思路:既然维表只有 265 行(几十 KB),直接复制一份完整维表给每个 Worker。

每个 Worker 现在都有:
    - 自己本地那部分主表(不动!)
        - 一份完整的 265 行维表(广播来的)

这样每个 Worker 的主表行,在本地就能查到对应的维表行——132 → 'Queens' 本地一查就有,根本不需要搬动主表。

Worker-1: 本地主表 300万行 + 完整维表 → 本地 join,不跨网络
Worker-2: 本地主表 300万行 + 完整维表 → 本地 join,不跨网络
Worker-3: 本地主表 320万行 + 完整维表 → 本地 join,不跨网络

| 条件             | 选择                        |
| ---------------- | --------------------------- |
| 两张都是大表     | 必须 Shuffle(SortMergeJoin) |
| 一张小表(< 10MB) | 可以 Broadcast(不 Shuffle)  |



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
   A: 三种情况:
   (1) 小表实际大于 `autoBroadcastJoinThreshold`(默认 10MB);
   (2) 小表统计信息缺失,Spark 不知道它小;
   (3) Right Outer / Full Outer Join + 大表在被广播侧。第二种最坑——可以用 `ANALYZE TABLE ... COMPUTE STATISTICS` 让 Spark 知道表大小。
   
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
