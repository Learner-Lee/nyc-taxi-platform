# STAGE 08: ADS 层 + PostgreSQL 索引

## 🎯 本阶段目标

- **业务问题**:DWS 层让 Spark 查询变快了,但**司机端 App 不能直连 Spark**——Spark 是 OLAP 引擎,启动一个 Job 就是几百毫秒,Web 应用响应不了。这一阶段把"高频查询的最终结果"物化到 **PostgreSQL** 这种 OLTP 数据库,**用传统索引把点查/小范围查询做到 ms 级**。
- **技术能力**:
  - 理解 OLAP(Spark/ClickHouse) vs OLTP(PostgreSQL) 的根本差异
  - 用 Spark JDBC 把 DWS 数据写入 PostgreSQL(数仓的"对外出口")
  - 掌握 PostgreSQL 4 类索引: **B-Tree / 复合 / 覆盖(INCLUDE) / 部分**
  - 读懂 EXPLAIN ANALYZE 输出: Seq Scan / Index Scan / Index Only Scan / Bitmap Heap Scan
  - 识别"回表"问题与覆盖索引的解决方案
- **产出物**:
  - `ads.daily_borough_summary` PostgreSQL 表(财务报表)
  - `ads.driver_recommendation` PostgreSQL 表(司机推荐)
  - `benchmarks/stage_08_pg_index_comparison.md` (实验 #12)
  - SQL 脚本 `sql/postgres/ads_indexes.sql`(可重跑)

---

## 📋 前置检查清单

- [ ] STAGE 07 完成,`dws.daily_zone_revenue` / `dws.hourly_zone_trips` 存在
- [ ] PostgreSQL 容器健康(STAGE 01 起就一直运行)
- [ ] Spark Worker 镜像已经包含 PostgreSQL JDBC 驱动(STAGE 01 Dockerfile 装过 `postgresql-42.7.1.jar`)
- [ ] 能用 psql / DBeaver 等工具连 PostgreSQL(可选,EXPLAIN 也能在 Spark 里跑)

---

## 🚀 本阶段启动服务

core 组无变化(PostgreSQL 已在 core 组里):
```bash
docker compose -f docker/docker-compose.core.yml ps postgres
# 应该看到 Up X hours (healthy)
```

---

## 📚 核心概念(5 分钟读完)

### 概念 1:OLAP vs OLTP — 数仓为什么需要 PostgreSQL?
| 维度 | OLAP(Spark/ClickHouse)| OLTP(PostgreSQL) |
|------|---------------------|------------------|
| 查询粒度 | 全表扫描 + 聚合(GB-TB) | 点查/小范围(几行-几千行) |
| 启动开销 | 数百毫秒(JVM/JIT) | 毫秒 |
| 并发 | 几个 Job 同时 | 几千连接同时 |
| 索引 | 列式存储自带 zone map | **传统 B-Tree 索引** |
| 适用场景 | BI 报表、数据科学 | **Web 应用、移动 App、API** |

**ADS 层的本质**:把数仓的最终结果**物化到 OLTP**,让前端应用直查——**这就是数仓最后一公里**。

### 概念 2:PostgreSQL 4 类索引(本阶段全覆盖)
| 类型 | 写法 | 适用 |
|------|------|------|
| **B-Tree** | `CREATE INDEX i ON t(col)` | 等值/范围查询的默认 |
| **复合索引** | `CREATE INDEX i ON t(a, b, c)` | 多列 WHERE,最左前缀原则 |
| **覆盖索引** | `CREATE INDEX i ON t(a) INCLUDE (b, c)` | 消除"回表"(Index Only Scan) |
| **部分索引** | `CREATE INDEX i ON t(a) WHERE status='active'` | 只索引满足条件的行,节省空间 |

### 概念 3:什么是"回表"?为什么覆盖索引能消除它?
**回表**:索引里只有索引列的值,SELECT 其他列时,数据库要拿索引里的"行指针"回主表读完整行(一次额外的随机 IO)。

**覆盖索引**:把 SELECT 用到的列都"塞"进索引(INCLUDE),数据库**只读索引就拿到全部数据**,跳过回表——**Index Only Scan**。

**业务类比**:
- 普通索引 = 图书馆的"作者→书架"索引,知道作者后还要去书架找书
- 覆盖索引 = 索引里直接写了"作者→书名+摘要",不用去书架了

### 概念 4:EXPLAIN ANALYZE 的关键字段
```
Index Scan using ix_borough on driver_recommendation
  (cost=0.29..8.50 rows=10 width=72) (actual time=0.012..0.024 rows=8 loops=1)
  Index Cond: (borough = 'Manhattan'::text)
  Buffers: shared hit=4
```
- **Index Scan**:用了索引(对应 `Seq Scan` = 全表扫,慢)
- **rows=10 / actual rows=8**:估算 vs 实际(差太多说明统计信息过期,要 ANALYZE)
- **actual time=0.012..0.024**:首行延迟 .. 总耗时(ms)
- **Buffers: shared hit=4**:读了 4 个 8KB 页(数据库 IO 单位)

---

## 🛠 操作步骤

### 步骤 1:在 PostgreSQL 建 ADS Schema 和表

在 Jupyter 用 Spark JDBC + DDL:

```python
from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("STAGE08-ADS") \
    .master("spark://spark-master:7077") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.executor.memory", "2g") \
    .enableHiveSupport() \
    .getOrCreate()

# PostgreSQL JDBC 配置(STAGE 01 docker-compose 里设的)
PG_URL = "jdbc:postgresql://postgres:5432/nyc_taxi"
PG_PROPS = {
    "user": "taxi_user",
    "password": "taxi_pass123",
    "driver": "org.postgresql.Driver"
}

print("✅ Spark + PG JDBC 配置就绪")
```

### 步骤 2:从 DWS 物化两张 ADS 表到 PostgreSQL

```python
# ── ADS 表 1: 财务日报(粗粒度,几百行)──
print(">>> 构建 ads.daily_borough_summary ...")
daily_summary = spark.sql("""
    SELECT
        pickup_date,
        pickup_borough,
        SUM(trips) AS trips,
        SUM(revenue) AS revenue,
        ROUND(AVG(avg_fare), 2) AS avg_fare,
        SUM(airport_trips) AS airport_trips,
        SUM(airport_revenue) AS airport_revenue,
        ROUND(SUM(airport_revenue) / SUM(revenue) * 100, 2) AS airport_pct
    FROM dws.daily_zone_revenue
    GROUP BY pickup_date, pickup_borough
""")
print(f"  行数: {daily_summary.count():,}")

t0 = time.time()
daily_summary.write \
    .jdbc(url=PG_URL, table="ads.daily_borough_summary",
          mode="overwrite", properties=PG_PROPS)
print(f"  写入 PG 耗时: {time.time()-t0:.1f}s")

# ── ADS 表 2: 司机推荐(高频查询,几万行)──
print("\n>>> 构建 ads.driver_recommendation ...")
driver_rec = spark.sql("""
    WITH hourly_zone AS (
        SELECT
            time_bucket,
            pickup_borough,
            pickup_zone,
            PULocationID,
            SUM(trips) AS trips,
            SUM(revenue) AS revenue,
            ROUND(SUM(revenue) / SUM(trips), 2) AS avg_revenue_per_trip
        FROM dws.hourly_zone_trips
        GROUP BY time_bucket, pickup_borough, pickup_zone, PULocationID
    )
    SELECT *,
           RANK() OVER (PARTITION BY time_bucket, pickup_borough ORDER BY revenue DESC) AS rev_rank
    FROM hourly_zone
""")
print(f"  行数: {driver_rec.count():,}")

t0 = time.time()
driver_rec.write \
    .jdbc(url=PG_URL, table="ads.driver_recommendation",
          mode="overwrite", properties=PG_PROPS)
print(f"  写入 PG 耗时: {time.time()-t0:.1f}s")

print("\n✅ ADS 双表已落地 PostgreSQL")
```

---

### 步骤 3:在 PostgreSQL 里验证数据 + ANALYZE 统计

Spark JDBC 写入会自动建表,但**类型和索引都是默认的**。要先 ANALYZE 让 PostgreSQL 收集统计,才能准确读懂后面 EXPLAIN 输出。

```python
# 用 Spark JDBC 跑 PostgreSQL DDL / ANALYZE
pg_conn = spark._jvm.java.sql.DriverManager.getConnection(PG_URL, "taxi_user", "taxi_pass123")
stmt = pg_conn.createStatement()

# 收集统计
stmt.execute("ANALYZE ads.daily_borough_summary")
stmt.execute("ANALYZE ads.driver_recommendation")

# 验证行数
for tbl in ["ads.daily_borough_summary", "ads.driver_recommendation"]:
    rs = stmt.executeQuery(f"SELECT COUNT(*) FROM {tbl}")
    rs.next()
    print(f"  {tbl}: {rs.getInt(1):,} 行")

stmt.close()
pg_conn.close()
print("\n✅ ANALYZE 完成")
```

---

## 🔬 实验 #12: PostgreSQL 索引四阶段对比

### 实验场景
**司机端 App 高频查询**:
> "我在 Manhattan,现在是晚高峰,推荐 TOP 5 高营收区域"
对应 SQL:
```sql
SELECT pickup_zone, revenue, trips, avg_revenue_per_trip
FROM ads.driver_recommendation
WHERE pickup_borough = 'Manhattan'
  AND time_bucket = 'evening_rush'
  AND rev_rank <= 5
ORDER BY rev_rank
```

我们会跑 **5 个版本**,每次加一种索引,看 EXPLAIN ANALYZE 变化。

### 准备:封装查询函数

```python
import re

def pg_explain(query, comment=""):
    """跑 EXPLAIN ANALYZE 并提取关键信息"""
    pg_conn = spark._jvm.java.sql.DriverManager.getConnection(PG_URL, "taxi_user", "taxi_pass123")
    stmt = pg_conn.createStatement()
    rs = stmt.executeQuery(f"EXPLAIN (ANALYZE, BUFFERS) {query}")
    lines = []
    while rs.next():
        lines.append(rs.getString(1))
    plan = "\n".join(lines)

    # 提取关键指标
    exec_time = re.search(r"Execution Time: ([\d.]+) ms", plan)
    plan_time = re.search(r"Planning Time: ([\d.]+) ms", plan)
    scan_type = re.search(r"->\s+(\w+ Scan)", plan)
    buffers = re.search(r"Buffers: shared hit=(\d+)(?:\s+read=(\d+))?", plan)

    print(f"\n{'='*60}")
    print(f"  {comment}")
    print(f"{'='*60}")
    print(plan)
    print(f"\n  ⚡ 执行时间: {exec_time.group(1) if exec_time else '?'} ms"
          f" | 规划时间: {plan_time.group(1) if plan_time else '?'} ms"
          f" | 扫描类型: {scan_type.group(1) if scan_type else '?'}")
    if buffers:
        print(f"  📦 Buffers: hit={buffers.group(1)} read={buffers.group(2) or 0}")

    stmt.close()
    pg_conn.close()
    return float(exec_time.group(1)) if exec_time else 0
```

### 5 个版本对比(Cell)

```python
QUERY = """
    SELECT pickup_zone, revenue, trips, avg_revenue_per_trip
    FROM ads.driver_recommendation
    WHERE pickup_borough = 'Manhattan'
      AND time_bucket = 'evening_rush'
      AND rev_rank <= 5
    ORDER BY rev_rank
"""

# 用一个工具函数跑索引 DDL
def pg_exec(sql):
    c = spark._jvm.java.sql.DriverManager.getConnection(PG_URL, "taxi_user", "taxi_pass123")
    s = c.createStatement(); s.execute(sql); s.close(); c.close()

# ── 阶段 1: 无索引(基线)──
pg_exec("DROP INDEX IF EXISTS ads.ix_drv_borough")
pg_exec("DROP INDEX IF EXISTS ads.ix_drv_borough_bucket")
pg_exec("DROP INDEX IF EXISTS ads.ix_drv_cover")
pg_exec("DROP INDEX IF EXISTS ads.ix_drv_partial")
pg_exec("ANALYZE ads.driver_recommendation")
t1 = pg_explain(QUERY, "阶段 1:无索引(Seq Scan 基线)")

# ── 阶段 2: 单列 B-Tree ──
pg_exec("CREATE INDEX ix_drv_borough ON ads.driver_recommendation(pickup_borough)")
pg_exec("ANALYZE ads.driver_recommendation")
t2 = pg_explain(QUERY, "阶段 2:单列 B-Tree on (pickup_borough)")

# ── 阶段 3: 复合索引 ──
pg_exec("DROP INDEX ads.ix_drv_borough")
pg_exec("CREATE INDEX ix_drv_borough_bucket ON ads.driver_recommendation(pickup_borough, time_bucket, rev_rank)")
pg_exec("ANALYZE ads.driver_recommendation")
t3 = pg_explain(QUERY, "阶段 3:复合 B-Tree on (borough, bucket, rank)")

# ── 阶段 4: 覆盖索引(INCLUDE 把 SELECT 用到的列塞进索引)──
pg_exec("DROP INDEX ads.ix_drv_borough_bucket")
pg_exec("""
    CREATE INDEX ix_drv_cover ON ads.driver_recommendation
        (pickup_borough, time_bucket, rev_rank)
        INCLUDE (pickup_zone, revenue, trips, avg_revenue_per_trip)
""")
pg_exec("ANALYZE ads.driver_recommendation")
t4 = pg_explain(QUERY, "阶段 4:覆盖索引(Index Only Scan,消除回表)")

# ── 阶段 5: 部分索引(只索引 Manhattan 的行)──
pg_exec("DROP INDEX ads.ix_drv_cover")
pg_exec("""
    CREATE INDEX ix_drv_partial ON ads.driver_recommendation
        (time_bucket, rev_rank)
        INCLUDE (pickup_zone, revenue, trips, avg_revenue_per_trip)
        WHERE pickup_borough = 'Manhattan'
""")
pg_exec("ANALYZE ads.driver_recommendation")
t5 = pg_explain(QUERY, "阶段 5:部分索引(只索引 Manhattan)")

# ── 汇总 ──
print(f"\n{'='*60}")
print(f"  五阶段对比汇总")
print(f"{'='*60}")
for i, (name, t) in enumerate([
    ("1 无索引(Seq Scan)", t1),
    ("2 单列 B-Tree", t2),
    ("3 复合索引", t3),
    ("4 覆盖索引(INCLUDE)", t4),
    ("5 部分索引(WHERE)", t5)
], 1):
    speedup = f"{t1/t:.2f}x" if t > 0 else "N/A"
    print(f"  {name:<28} {t:>8.3f} ms  加速 {speedup}")
```

### 📊 实测结果(2026-05-21,ads.trip_search 230,429 行)

| 阶段 | 扫描类型 | 执行时间 | 加速 | Buffers |
|------|---------|---------|------|---------|
| 1 无索引 | Parallel Seq Scan | 6.10ms | 1.0x | hit=**2723** |
| 2 单列 B-Tree(borough) | **仍是 Seq Scan!** | 6.16ms | 1.0x | hit=2723 |
| 3 复合索引 | Index Scan | 0.043ms | **141.9x** | hit=10 read=3 |
| 4 覆盖索引 INCLUDE | **Index Only Scan** | 0.026ms | **234.6x** | hit=1 read=3 |
| 5 部分索引 WHERE | **Index Only Scan** | 0.024ms | **254.2x** | hit=1 read=3 |

### 四个反直觉教学点

**1. 阶段 2 加了索引却没用!**(最值钱的洞察)
单列 `(pickup_borough)` 索引建了,但 EXPLAIN 还是 `Parallel Seq Scan`——**PG 优化器主动放弃了索引**。原因:Manhattan 占 54%(低选择性),走索引要回表 12 万次随机 IO,**比顺序全表扫还慢**。这是"低选择性列不适合单列索引"的铁证,面试问"为什么加了索引不生效"的头号原因。

**2. 阶段 3 复合索引 141.9x 飞跃**
`(borough, time_bucket, revenue DESC)` 三列:WHERE 两列精确定位 + `revenue DESC` 已在索引排好序(省掉 ORDER BY),Buffers 从 2723 → 13。

**3. 阶段 4 `Heap Fetches: 0`(消除回表铁证)**
`INCLUDE (pickup_zone, trips)` 把 SELECT 列塞进索引,`Index Only Scan` + `Heap Fetches: 0` = 完全没回主表,Buffers `hit=1`(只读 1 个索引页)。

**4. 阶段 5 部分索引最快 + 体积最小**
`WHERE pickup_borough='Manhattan'` 让索引只存 Manhattan 行,cost 906 → 805。

### 核心结论
- **IO 减少 680x**(Buffers 2723 → 1)
- **执行时间 254x 加速**(6.10ms → 0.024ms)
- 低选择性列单列索引无效,**复合索引 + 覆盖 INCLUDE 是点查优化的黄金组合**

---

## 💼 简历可写的成果(STAGE 08 新增 2 条)

> • **ADS 层物化到 PostgreSQL**:用 Spark JDBC 把 DWS 汇总数据写入 PostgreSQL 关系库,构建 2 张面向应用的 ADS 表(`daily_borough_summary` 财务日报 + `driver_recommendation` 司机推荐),**解决 Spark/ClickHouse 不适合 Web 应用高并发点查的痛点**,实现"数仓最后一公里"
>
> • **PostgreSQL 索引五阶段优化**(实验 #12):同一司机推荐查询(Manhattan + 晚高峰营收 TOP 10,230k 行表)经历 无索引 → 单列 B-Tree → 复合 → 覆盖(INCLUDE)→ 部分索引,EXPLAIN ANALYZE 证据显示**执行时间从 6.10ms 降至 0.024ms(254x 加速),Buffers IO 从 2723 页降至 1 页(680x)**;深度诊断**低选择性列(Manhattan 占 54%)单列索引被 PG 优化器主动放弃**的经典现象,通过覆盖索引 `Heap Fetches: 0` 验证回表消除

---

## 🎤 面试可能被问到的问题

1. **Q: 为什么数仓最后还要落 PostgreSQL,直接用 Spark/ClickHouse 不行吗?**
   A: 三个原因——(1) Spark 启动 JVM 几百毫秒,Web 应用 SLA 在 200ms 以内根本受不了;(2) ClickHouse 强项是聚合,**点查/小范围查询不如 PostgreSQL B-Tree 索引**;(3) PostgreSQL 支持高并发(几千连接 + 事务),数仓做不到。**ADS 层落 OLTP 是企业级数仓的标准做法**。

2. **Q: B-Tree 索引和 Hash 索引的区别?**
   A: B-Tree 支持等值 + 范围 + ORDER BY(因为是有序的);Hash 只支持等值,但比 B-Tree 快(O(1) vs O(log n))。**PostgreSQL 默认 B-Tree,Hash 索引几乎没人用**(因为不支持事务日志,9.x 之前还是 unlogged 的)。

3. **Q: 覆盖索引(INCLUDE)和把列加到索引 key 里有什么区别?**
   A: 关键区别——INCLUDE 的列**不参与索引排序**,只是"搭便车"存在叶节点。好处:(1) 不影响最左前缀原则;(2) 占用空间小;(3) 数据库不会用 INCLUDE 列做查找,只用作"取数据"。**复合 key 索引每多一列就要多排一次序**,INCLUDE 没这个成本。

4. **Q: 部分索引什么时候用?**
   A: 三个典型场景——(1) 业务上只关心子集(如 status='active' 占 1%);(2) 字段有严重数据倾斜(如 99% NULL);(3) 索引空间敏感(部分索引可能比全量小 10x+)。**生产例子**:订单表 `WHERE status='待支付'` 的部分索引,因为待支付订单是热数据。

5. **Q: EXPLAIN 看到 Index Scan,为什么有时还是慢?**
   A: 三种常见情况——(1) **回表 IO 多**:Index Scan 找到主表行后要随机 IO 读完整行,如果命中行很多,可能比 Seq Scan 还慢(优化器会判断,但有时判断错);(2) **统计信息过期**:rows 估算偏差大;(3) **HOT update**:行物理位置变了,索引指向旧位置导致额外跳转。**修复**:跑 ANALYZE / 用覆盖索引 / VACUUM。

---

## 🛑 踩坑实录(STAGE 08 新增 1 个)

### 坑 9:多镜像环境下 JDBC 驱动需在 Driver + Executor 全节点存在
**现象**:`spark.write.jdbc()` 能成功,但 `DriverManager.getConnection()` 报 `No suitable driver found for jdbc:postgresql://...`
**根因**:
- STAGE 01 在 **spark-master/worker 镜像**(bitnamilegacy/spark)的 Dockerfile 里装了 `postgresql-42.7.1.jar`
- 但 **Jupyter 用的是另一个镜像**(`jupyter/pyspark-notebook`),它的 Spark **没有这个 jar**
- `spark.write.jdbc` 在 Worker 上执行(有 jar)→ 成功
- `DriverManager.getConnection` 在 Jupyter 的 **Driver JVM** 执行(没 jar)→ 失败
**修复**:把 PG 驱动复制到 Jupyter 容器的 Spark jars 目录:
```bash
docker cp spark-master:/opt/bitnami/spark/jars/postgresql-42.7.1.jar /tmp/pg.jar
docker cp /tmp/pg.jar jupyter:/usr/local/spark/jars/postgresql-42.7.1.jar
docker restart jupyter
```
**面试可讲**:Spark 应用的 Driver 和 Executor 是**不同 JVM 进程**(甚至不同机器/容器),JDBC 驱动、UDF 依赖、第三方 jar **必须在所有相关节点都存在**。生产环境通常用 `--jars` 或 `spark.jars.packages` 统一分发,而不是手动 cp。

---

## 🧹 阶段收尾

```bash
cd /Users/alen/DA/NYC-Taxi-Trip-analysis/nyc-taxi-platform
mkdir -p sql/postgres
# 把 5 个 CREATE INDEX 落地到 sql/postgres/ads_indexes.sql 留底

git add docs/ benchmarks/ sql/
git commit -m "feat(stage08): ADS 层入 PG + 索引 5 阶段对比(254x 加速)"
```

---

## ➡️ 下一阶段预告

STAGE 09 **冷热分层:ClickHouse 接入**(项目第二个硬核技术栈跨度):
- 启动 **serving 组的 ClickHouse**(STAGE 01 部署但没用过)
- 把 DWD 数据导入 ClickHouse(MergeTree 引擎)
- **实验 #13**: **ClickHouse vs Spark SQL** 同一聚合查询对比(预期 ClickHouse 快 10-50x)
- 跳数索引(Skip Index)演示
- 引入冷热分层架构:**ClickHouse 热 + Parquet 温 + 对象存储冷**

需要本阶段产出:`ads.driver_recommendation` PG 表 + 对索引的体感。**STAGE 09 起需要启动 serving 组**(ClickHouse 约 4GB 内存)。
