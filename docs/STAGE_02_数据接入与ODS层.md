# STAGE 02: 数据接入与 ODS 层

## 🎯 本阶段目标

- **业务问题**:数据要先"进得来",NYC Cab Co. 需要把 NYC TLC 公开的出租车原始数据接入数据平台,作为后续所有分析的源头。
- **技术能力**:
  - 掌握 NYC TLC 公开数据集的结构与字段含义
  - 学会用 PySpark 读写 HDFS 上的 Parquet 与 CSV
  - 在 Hive Metastore 注册外部表,让 Spark SQL 直接查
  - 用 Spark UI 看懂"扫描数据量"这个核心性能指标
- **产出物**:
  - `data/raw/yellow_tripdata_2024-{01,02,03}.parquet` (本地原始数据)
  - `data/lookup/taxi_zone_lookup.csv` (区域维表)
  - HDFS 路径 `hdfs:///nyc-taxi/ods/yellow_trips/year=2024/month=*` (分区入库)
  - Hive 外部表 `ods.yellow_trips`
  - `benchmarks/stage_02_csv_vs_parquet.md` (首次量化对比实验结果)
  - `notebooks/stage02_data_exploration.ipynb` (探索过程留底)

---

## 📋 前置检查清单

- [ ] STAGE 01 已完成,10 个 core 容器全部健康运行
- [ ] HDFS `/nyc-taxi/ods` 目录存在(STAGE 01 步骤 3 已建)
- [ ] Spark 3 个 Worker 全部 ALIVE
- [ ] Jupyter Web UI 可访问(http://localhost:8888)
- [ ] 磁盘剩余 > 5GB (原始数据约 200MB,加上 HDFS 副本和 CSV 对比文件约 1.5GB)
- [ ] 需要启动的容器组:**core 组**(STAGE 01 已经在跑,不动)
- [ ] 当前内存占用应低于 16GB

---

## 🚀 本阶段启动服务

```bash
# core 组已经在跑,不需要重启
docker compose -f docker/docker-compose.core.yml ps
```

确认所有容器 Up 即可,无需新启动任何服务。serving 组仍然不需要。

---

## 📚 核心概念(3 分钟读完)

### 概念 1:为什么有 ODS 层?
**业务类比**:超市进货时,先把厂家发来的货原封不动放进"待检区",检查无误后再上架。ODS(Operational Data Store) 就是数据平台的"待检区",**原样保留**外部数据,**不做任何业务转换**。这样做的好处:

- 出问题能追溯到最原始的数据
- 后续 DWD 层规则改了,可以从 ODS 重跑,不用重新下载
- 上下游解耦——上游数据格式变了只影响 ODS

### 概念 2:外部表(External Table)vs 内部表(Managed Table)
**外部表**:Hive 只管理元数据(表结构、文件位置),不管文件本身。DROP TABLE 时**只删元数据,不删文件**。
**内部表**:Hive 管理一切。DROP TABLE 时连文件一起删。
**ODS 层永远用外部表**,因为原始数据珍贵,不能因为误删表而丢失。

### 概念 3:列式存储(Parquet)的核心优势
**业务类比**:CSV 像一本流水账日记(每页一行,要看"3 月所有上车地点"必须翻完所有日记);Parquet 像一本"按列分卷的账本"(地点单独一卷,需要时只翻地点卷)。
**两个关键优势**:

1. **列裁剪**(Column Pruning):SELECT 一列就只读一列
2. **谓词下推**(Predicate Pushdown):Parquet 文件头记录了每个 row group 的 min/max,WHERE 条件可以跳过整段不读

本阶段的对比实验会让你**亲眼看到**这两个优势的威力。

---

## 🛠 操作步骤

### 步骤 1:下载 NYC TLC 2024 Q1 原始 Parquet

**🤔 为什么这么做**:NYC TLC (出租车与豪华轿车委员会) 每月公开 Yellow Taxi 行程数据,从 2022 年起官方就发 Parquet (之前是 CSV),我们直接拿原生 Parquet。

**⌨️ 操作**(在 Mac 终端,不是容器里):
```bash
cd /Users/alen/DA/NYC-Taxi-Trip-analysis/nyc-taxi-platform

# 下载 2024 Q1 三个月数据(每个文件约 50-70MB)
for m in 01 02 03; do
  curl -L -o data/raw/yellow_tripdata_2024-${m}.parquet \
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-${m}.parquet"
done

# 下载区域维表
curl -L -o data/lookup/taxi_zone_lookup.csv \
  "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

# 验证文件
ls -lh data/raw/ data/lookup/
```

**✅ 预期效果**:
```
data/raw/:
-rw-r--r--  ... 47M ... yellow_tripdata_2024-01.parquet
-rw-r--r--  ... 47M ... yellow_tripdata_2024-02.parquet
-rw-r--r--  ... 55M ... yellow_tripdata_2024-03.parquet

data/lookup/:
-rw-r--r--  ... 12K ... taxi_zone_lookup.csv
```

**🐛 如果出错**:
- `curl: command not found` → macOS 自带 curl,如果真没有就 `brew install curl`
- 下载速度慢 → CloudFront CDN 在中国境内可能慢,挂代理或换源

---

### 步骤 2:把原始数据上传到 HDFS

**🤔 为什么这么做**:本地文件系统不是分布式存储,后续 Spark 集群读取会有 IO 瓶颈。把数据上传到 HDFS 后,Spark 可以"数据本地性"——Worker 优先读自己机器上的块。

**⌨️ 操作**(在 Mac 终端):
```bash
# 进入 namenode 容器,从 /data (挂载点) 上传到 HDFS
docker exec namenode bash -c "
# 创建分区目录(按年月分区,STAGE 03 会展开讨论分区设计)
hdfs dfs -mkdir -p /nyc-taxi/ods/yellow_trips/year=2024/month=01
hdfs dfs -mkdir -p /nyc-taxi/ods/yellow_trips/year=2024/month=02
hdfs dfs -mkdir -p /nyc-taxi/ods/yellow_trips/year=2024/month=03
hdfs dfs -mkdir -p /nyc-taxi/ods/taxi_zone_lookup

# 上传 (注意 namenode 容器里挂载点是 /data,看 docker-compose 配置)
# 但 namenode 没挂载 ../data,需要通过 docker cp
"

# namenode 容器没挂载 data 目录,改用 docker cp 中转
docker cp data/raw/yellow_tripdata_2024-01.parquet namenode:/tmp/
docker cp data/raw/yellow_tripdata_2024-02.parquet namenode:/tmp/
docker cp data/raw/yellow_tripdata_2024-03.parquet namenode:/tmp/
docker cp data/lookup/taxi_zone_lookup.csv namenode:/tmp/

docker exec namenode bash -c "
hdfs dfs -put -f /tmp/yellow_tripdata_2024-01.parquet /nyc-taxi/ods/yellow_trips/year=2024/month=01/
hdfs dfs -put -f /tmp/yellow_tripdata_2024-02.parquet /nyc-taxi/ods/yellow_trips/year=2024/month=02/
hdfs dfs -put -f /tmp/yellow_tripdata_2024-03.parquet /nyc-taxi/ods/yellow_trips/year=2024/month=03/
hdfs dfs -put -f /tmp/taxi_zone_lookup.csv /nyc-taxi/ods/taxi_zone_lookup/
rm /tmp/yellow_tripdata_*.parquet /tmp/taxi_zone_lookup.csv
hdfs dfs -du -h /nyc-taxi/ods
"
```

**✅ 预期效果**:
```
148.5 M  297.0 M  /nyc-taxi/ods/yellow_trips
12.2 K   24.4 K   /nyc-taxi/ods/taxi_zone_lookup
```
(第一列是原始大小,第二列是包含副本数 ×2 的占用)

**🐛 如果出错**:
- `Permission denied` → `hdfs dfs -chmod -R 777 /nyc-taxi`
- `Safe mode` → `docker exec namenode hdfs dfsadmin -safemode leave`

---

### 步骤 3:在 Hive Metastore 注册 ODS 外部表

**🤔 为什么这么做**:让 Spark SQL 能直接 `SELECT * FROM ods.yellow_trips`,不用每次都写 `spark.read.parquet("hdfs://...")`。这是数据平台**对外提供 SQL 接口**的标准做法。

**⌨️ 操作**:在 Jupyter Web UI(http://localhost:8888) 新建 Notebook,执行:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("STAGE02-ODS-DDL") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \ # 配置 Hive 数据仓库在 HDFS 上的存储路径
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \ # 连接 Hive 元数据服务, 不连这个，Spark 就找不到任何 Hive 表
    .enableHiveSupport() \ # 开启 Hive 支持
    .getOrCreate()

# 创建 ods 数据库
spark.sql("CREATE DATABASE IF NOT EXISTS ods LOCATION 'hdfs://namenode:9000/nyc-taxi/ods'")

# 注册外部表(分区表,字段定义贴近 NYC TLC 官方 schema)
spark.sql("DROP TABLE IF EXISTS ods.yellow_trips")
spark.sql("""
CREATE EXTERNAL TABLE ods.yellow_trips (
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
    Airport_fee DOUBLE
)
PARTITIONED BY (year INT, month INT)  -- 分区
STORED AS PARQUET                     -- 存储格式
LOCATION 'hdfs://namenode:9000/nyc-taxi/ods/yellow_trips'
""")

# 手动注册分区(让 Hive Metastore 知道有哪些分区目录)
spark.sql("ALTER TABLE ods.yellow_trips ADD IF NOT EXISTS PARTITION (year=2024, month=1) LOCATION 'hdfs://namenode:9000/nyc-taxi/ods/yellow_trips/year=2024/month=01'")
spark.sql("ALTER TABLE ods.yellow_trips ADD IF NOT EXISTS PARTITION (year=2024, month=2) LOCATION 'hdfs://namenode:9000/nyc-taxi/ods/yellow_trips/year=2024/month=02'")
spark.sql("ALTER TABLE ods.yellow_trips ADD IF NOT EXISTS PARTITION (year=2024, month=3) LOCATION 'hdfs://namenode:9000/nyc-taxi/ods/yellow_trips/year=2024/month=03'")

# 维表:CSV 原始文件用 Spark 直读 → 转 Parquet → 注册外部 Parquet 表
# (CSV 用 Hive ROW FORMAT 解析有"双坑":skip.header 经常失效 + 不处理 quote,详见踩坑实录坑 7)
from pyspark.sql.functions import col

# 1. 用 Spark 直读 CSV(正确处理 header + quote)
df_zone = spark.read \
    .option("header", "true") \ # 第一行是表头（列名）
    .option("quote", '"') \ 
    .option("escape", '"') \ 
    .csv("hdfs://namenode:9000/nyc-taxi/ods/taxi_zone_lookup/taxi_zone_lookup.csv")
# 带表头
# 字段用双引号包裹
# 字段内部可能有逗号
# 字段内部可能有双引号
    
# 2. 类型转换(LocationID 从 STRING 转 INT)
df_zone = df_zone.withColumn("LocationID", col("LocationID").cast("int"))

print("=== 预览原始数据(已正确解析)===")
df_zone.show(5, truncate=False)
df_zone.printSchema()
print(f"维表总行数: {df_zone.count()}")

# 3. 写成 Parquet 到独立路径(原 CSV 文件依然保留,做"溯源备份")
df_zone.write.mode("overwrite").parquet(
    "hdfs://namenode:9000/nyc-taxi/ods/taxi_zone_lookup_parquet"
)

# 4. 重建外部表,指向 Parquet
spark.sql("DROP TABLE IF EXISTS ods.taxi_zone_lookup")
spark.sql("""
CREATE EXTERNAL TABLE ods.taxi_zone_lookup (
    LocationID INT,
    Borough STRING,
    Zone STRING,
    service_zone STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/nyc-taxi/ods/taxi_zone_lookup_parquet'
""")

print("\n=== 重建后的维表(Parquet)===")
spark.sql("SELECT * FROM ods.taxi_zone_lookup LIMIT 5").show()
print(f"总行数: {spark.sql('SELECT COUNT(*) FROM ods.taxi_zone_lookup').collect()[0][0]}")
```

**✅ 预期效果**:看到两张表 + 3 个分区 + 维表前 5 行(`1 | EWR | Newark Airport | EWR`)

**🐛 如果出错**:
- `MetaException: TTransportException` → Hive Metastore 没启动,`docker logs hive-metastore`
- `Parquet column cannot be converted ... Expected: double, Found: INT64` → DDL 字段类型与 Parquet 实际不匹配,详见踩坑实录坑 8
- `Filesystem closed` → Spark Session 状态异常,Kernel → Restart

---

### 步骤 4:首次数据探索

**🤔 为什么这么做**:数据接入后必须先做"健康检查"——总行数、字段空值率、值分布。任何上线的数据集都要先过这关,业内叫**数据剖析**(Data Profiling)。

**⌨️ 操作**:在 Jupyter Notebook 新 cell 跑:

```python
# 4.1 总行数
total = spark.sql("SELECT COUNT(*) AS cnt FROM ods.yellow_trips").collect()[0]['cnt']
print(f"2024 Q1 总行数: {total:,}")

# 4.2 各月行数(看数据完整性)
spark.sql("""
    SELECT year, month, COUNT(*) AS trips
    FROM ods.yellow_trips
    GROUP BY year, month
    ORDER BY year, month
""").show()

# 4.3 关键字段空值率
spark.sql("""
    SELECT
        ROUND(SUM(CASE WHEN passenger_count IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pct_null_passenger,
        ROUND(SUM(CASE WHEN trip_distance IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pct_null_distance,
        ROUND(SUM(CASE WHEN total_amount IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pct_null_amount
    FROM ods.yellow_trips
""").show()

# 4.4 异常值初窥(为 STAGE 04 DWD 清洗规则铺垫)
spark.sql("""
    SELECT
        MIN(trip_distance) AS min_dist, MAX(trip_distance) AS max_dist,
        MIN(total_amount) AS min_amt, MAX(total_amount) AS max_amt,
        MIN(passenger_count) AS min_pax, MAX(passenger_count) AS max_pax
    FROM ods.yellow_trips
""").show()
```

**✅ 预期发现**:
- 总行数约 1000-1200 万(三个月)
- 各月行数大致接近,无明显缺失
- passenger_count 有少量空值(<5%)
- **会看到一些"匪夷所思"的值**:负的 total_amount、几千公里的 trip_distance、200 个乘客等——这些就是 STAGE 04 要清洗掉的"脏数据",**先记着,别动手**。

**💾 把这一步的发现记到** `benchmarks/stage_02_data_profile.md`(可选,后续 STAGE 04 会用到)

---

## 🔬 对比实验 #1:CSV vs Parquet(本阶段核心)

> **这是项目的第一次量化对比实验,请认真执行,数据会直接进你的简历**

### 实验目的
验证两个论断:
1. Parquet 文件比 CSV 体积**小很多**(因为列存 + 压缩)
2. 同样的查询,Spark 扫描 Parquet 比 CSV **快很多**(因为列裁剪 + 谓词下推)

### 实验设计
拿同一份数据(2024-01,约 296 万行),分别存成 Parquet 和 CSV,跑同样的两个查询,记录耗时和扫描量。

### 实验步骤(都在 Jupyter Notebook 里跑)

**第 1 步:准备两种格式的同源数据**

```python
import time

# 读 2024-01 这一个分区(原始 Parquet,约 296 万行)
df_jan = spark.read.parquet("hdfs://namenode:9000/nyc-taxi/ods/yellow_trips/year=2024/month=01")
n = df_jan.count()
print(f"基准数据行数: {n:,}")

# 写一份 CSV(用于对比)
csv_path = "hdfs://namenode:9000/nyc-taxi/ods/_benchmark/yellow_2024_01_csv"
print("\n正在写 CSV...")
t0 = time.time()
df_jan.write.mode("overwrite").option("header", "true").csv(csv_path)
print(f"CSV 写入耗时: {time.time() - t0:.1f}s")

# 同一份数据再写一份 Parquet(保证读取条件完全一致)
parquet_path = "hdfs://namenode:9000/nyc-taxi/ods/_benchmark/yellow_2024_01_parquet"
print("\n正在写 Parquet...")
t0 = time.time()
df_jan.write.mode("overwrite").parquet(parquet_path)
print(f"Parquet 写入耗时: {time.time() - t0:.1f}s")

print("\n✅ 两种格式数据准备就绪")
```

```
基准数据行数: 2,964,624

正在写 CSV...
CSV 写入耗时: 2.8s

正在写 Parquet...
Parquet 写入耗时: 2.1s

✅ 两种格式数据准备就绪
```

**第 2 步:对比文件体积**

```python
# 用 Spark JVM 桥读 HDFS 上的文件实际大小
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())


def hdfs_size_bytes(path):
    total = 0
    files = fs.listFiles(spark._jvm.org.apache.hadoop.fs.Path(path), True)
    while files.hasNext():
        total += files.next().getLen()
    return total


csv_bytes = hdfs_size_bytes(csv_path)
parquet_bytes = hdfs_size_bytes(parquet_path)

print("=" * 55)
print(f"  CSV     总大小: {csv_bytes / 1024 / 1024:>10,.2f} MB")
print(f"  Parquet 总大小: {parquet_bytes / 1024 / 1024:>10,.2f} MB")
print(f"  压缩比 (CSV / Parquet): {csv_bytes / parquet_bytes:>10,.2f} x")
print("=" * 55)
print(f"\n  💡 同样 {n:,} 行数据,Parquet 比 CSV 节省 {(1 - parquet_bytes / csv_bytes) * 100:.1f}% 空间")
```

```
=======================================================
  CSV     总大小:     308.06 MB
  Parquet 总大小:      58.22 MB
  压缩比 (CSV / Parquet):       5.29 x
=======================================================

  💡 同样 2,964,624 行数据,Parquet 比 CSV 节省 81.1% 空间
```

**第 3 步:对比查询耗时**

```python
def timeit(label, fn):
    t0 = time.time()
    result = fn()
    elapsed = time.time() - t0
    print(f"[{label}] 耗时: {elapsed:.2f}s, 结果: {result}")
    return elapsed

# 查询 A:只读 1 列的简单聚合(列裁剪威力)
print("=== 查询 A:SELECT COUNT(*),只触发 metadata 扫描 ===")
t_csv_a = timeit("CSV", lambda: spark.read.option("header", "true").csv(csv_path).count())
t_pq_a  = timeit("Parquet", lambda: spark.read.parquet(parquet_path).count())

# 查询 B:带条件的列聚合
print("\n=== 查询 B:WHERE + SUM,触发列裁剪 + 谓词下推 ===")
t_csv_b = timeit(
    "CSV",
    lambda: spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
        .filter("trip_distance > 5").agg({"total_amount": "sum"}).collect()[0][0]
)
t_pq_b = timeit(
    "Parquet",
    lambda: spark.read.parquet(parquet_path)
        .filter("trip_distance > 5").agg({"total_amount": "sum"}).collect()[0][0]
)

print(f"\n查询 A 加速比: {t_csv_a / t_pq_a:.2f}x")
print(f"查询 B 加速比: {t_csv_b / t_pq_b:.2f}x")
```

```
============================================================
  查询 A: SELECT COUNT(*) — 测列裁剪 + Parquet metadata
============================================================
  [     CSV] 耗时:   0.50s  | 结果: 2964624
  [ Parquet] 耗时:   0.22s  | 结果: 2964624
  → 加速比: 2.24x

============================================================
  查询 B: WHERE + SUM — 测列裁剪 + 谓词下推
============================================================
  业务含义: 行程距离 > 5 英里的总营收
  [     CSV] 耗时:   2.89s  | 结果: 29946845.159999676
  [ Parquet] 耗时:   0.25s  | 结果: 29946845.16000478
  → 加速比: 11.35x

============================================================
  汇总:
    查询 A (COUNT)   : CSV 0.50s  vs  Parquet 0.22s  → 2.2x
    查询 B (WHERE+SUM): CSV 2.89s  vs  Parquet 0.25s  → 11.3x
============================================================
```

### 实验后清理

```python
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
fs.delete(spark._jvm.org.apache.hadoop.fs.Path("hdfs://namenode:9000/nyc-taxi/ods/_benchmark"), True)
print("✅ 临时对比数据已清理")

# 验证
ls_out = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()) \
    .listStatus(spark._jvm.org.apache.hadoop.fs.Path("hdfs://namenode:9000/nyc-taxi/ods"))
print("\nODS 目录现有内容:")
for s in ls_out:
    print(f"  {s.getPath().getName()}")
```

---

## 💼 简历可写的成果

做完本阶段,简历可以加这条 bullet(填入你实际跑出的数据):

> 接入 NYC TLC Yellow Taxi 2024 Q1 全量数据(约 ___万行,148MB Parquet),
> 通过 Hive Metastore 注册分区外部表实现 SQL 直查;
> 完成 CSV vs Parquet 量化对比,验证 Parquet 列式存储使文件体积压缩 ___倍、
> 同等查询扫描量减少 ___倍、耗时提升 ___倍,为后续优化建立量化基线。

---

## 🎤 面试可能被问到的问题

1. **Q: 为什么 ODS 层用外部表而不是内部表?**
   A: ODS 是数据进入数仓的第一站,数据极珍贵。外部表 DROP 只删元数据不删文件,可以安全地重建表结构。生产环境 ODS 层 100% 都是外部表。

2. **Q: 你怎么验证 Parquet 的列裁剪生效了?**
   A: 看 Spark UI 的 SQL 查询详情,Physical Plan 里会有 `ReadSchema` 字段,只列出查询用到的列;同时 Stage 的 "Bytes Read" 远小于文件总大小。还可以用 `df.explain(True)` 看 Catalyst 优化后的扫描计划。

3. **Q: 谓词下推为什么对 CSV 不生效?**
   A: CSV 是行式格式,必须把整行读出来才能判断 WHERE 条件;Parquet 文件头记录了每个 row group 的列级 min/max,Spark 可以根据这个先跳过整段不读。

4. **Q: 数据剖析(Data Profiling)在数仓里的作用?**
   A: 数据接入后第一步,产出空值率、值分布、异常范围的报告,为后续 DWD 层清洗规则提供数据支撑——清洗规则不能拍脑袋定,要有数据依据。

---

## 🛑 踩坑实录(STAGE 02 新增 2 个真实工程问题)

### 坑 7:维表 CSV 用 Hive ROW FORMAT 不能正确处理 header 和 quote
**现象**:用 `STORED AS TEXTFILE` + `ROW FORMAT DELIMITED FIELDS TERMINATED BY ','` 建表后,SELECT 看到:
- 第 1 行是表头字面值(`"Borough"`、`"Zone"`),即 `TBLPROPERTIES ('skip.header.line.count'='1')` 失效
- 所有字段值带双引号(`"Manhattan"`),即 quote 字符没被剥离
**根因**:
1. `skip.header.line.count` 在 Spark SQL 读 Hive 表时**经常被忽略**(Spark 已知行为,Hive 引擎与 Spark 引擎解析路径不同)
2. `ROW FORMAT DELIMITED` 只按分隔符切字段,**不识别 CSV 的 quote 语义**
**修复**:不要硬用 Hive 的 ROW FORMAT 解析"非平凡 CSV",改用 **Spark 直读 CSV → 转 Parquet → 注册外部 Parquet 表** 三段式:
```python
df = spark.read.option("header", "true").option("quote", '"').option("escape", '"').csv(csv_path)
df.write.mode("overwrite").parquet(parquet_path)
# CREATE EXTERNAL TABLE ... STORED AS PARQUET LOCATION parquet_path
```
**面试可讲**:为什么"ODS 层原样保留"在维度表上要打折扣——维表体积小、被频繁关联,转 Parquet 收益巨大;原始 CSV 文件可保留作"溯源备份"

### 坑 8:ODS 表 schema 必须严格匹配 Parquet 文件实际 schema,否则 Spark 拒绝读取
**现象**:`SELECT COUNT(*)` 不报错,但带 WHERE/SUM 的查询报:
```
SchemaColumnConvertNotSupportedException: column: [passenger_count], physicalType: INT64, logicalType: double
```
**根因**:NYC TLC 在不同年份**悄悄改字段类型**:
- 2022 年 `passenger_count` 是 DOUBLE
- 2024 年改成 BIGINT (INT64)
- `RatecodeID` 同样改了
我们的 DDL 沿用了"老经验"用 DOUBLE,Parquet 读取时 INT64→DOUBLE **不做隐式转换**(Spark vectorized reader 严格类型校验)
**修复**:DDL 前先用 `spark.read.parquet(path).printSchema()` 看真实 schema,再据此写 DDL。**永远不要凭记忆/直觉写 ODS 表的类型**。
**面试可讲**:
- ODS 层的"schema first" 原则——以文件为准,不以历史经验为准
- 上游数据契约变更监控的重要性(生产环境通常通过 schema registry + diff 工具自动告警)
- 这种"隐式契约"问题是数据平台最常见的"隐形雷",80% 的数据事故来源

### 复盘
STAGE 01 踩 6 坑,STAGE 02 踩 2 坑,**全部是真实生产里高频出现的问题**。能讲清楚根因和修复路径,在数据工程/大数据架构面试里**碾压只会跑通官方示例的候选人**。

---

## 🧹 阶段收尾

- 保持 core 组运行(STAGE 03 继续用)
- Notebook 保存到 `notebooks/stage02_data_exploration.ipynb`
- benchmark 结果固化到 `benchmarks/stage_02_csv_vs_parquet.md`
- 清理对比实验产生的临时数据(防止占 HDFS 空间):
  ```python
  fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
  fs.delete(spark._jvm.org.apache.hadoop.fs.Path("hdfs://namenode:9000/nyc-taxi/ods/_benchmark"), True)
  ```
- 建议 git commit:`git add . && git commit -m "feat(stage02): ODS 接入 + CSV vs Parquet 对比 + 2 个工程坑沉淀"`

---

## ➡️ 下一阶段预告

STAGE 03 我们用同一份数据做**存储层优化**:
- 实验 #2:Snappy vs Zstd vs 无压缩,看压缩比和读速度
- 实验 #3:全表扫描 vs 分区裁剪,看 Spark 实际扫描的数据量
- 小文件治理:把多个小 Parquet 文件合并成合理大小的大文件
- 引入分桶(Bucketing)的概念,为 STAGE 04 维度关联铺垫

需要本阶段产出:`ods.yellow_trips` 外部表 + 3 个月分区数据。
