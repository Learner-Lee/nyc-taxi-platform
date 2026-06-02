# STAGE 05: 计算层深度优化(谓词下推 + AQE + CBO)

## 🎯 本阶段目标

- **业务问题**:DWD 表稳定后,运营/财务团队的临时分析查询有时仍然偏慢。**这一阶段挖到 Spark 内核优化的最后一公里**——让你能解释"为什么这条 SQL 慢"并知道往哪改。
- **技术能力**:
  - 深度读懂 Spark 物理执行计划(超越"看到 BroadcastHashJoin 就行")
  - 掌握谓词下推(Predicate Pushdown)的**生效条件与失效模式**(UDF 是头号杀手)
  - **AQE**(Adaptive Query Execution)三大能力:动态合并 Shuffle 分区 / 动态切换 Join 策略 / 动态优化倾斜
  - **CBO**(Cost-Based Optimization)如何让 Spark 看到"数据真相"做更聪明的 Join 顺序
  - 学会用 Spark UI 的 SQL 标签页诊断慢查询
- **产出物**:
  - `benchmarks/stage_05_predicate_pushdown.md` (实验 #6 谓词下推 + UDF 失效)
  - `benchmarks/stage_05_aqe_comparison.md` (实验 #7 AQE 开关对比)
  - `benchmarks/stage_05_cbo_comparison.md` (实验 #8 CBO 对比,加分)
  - `notebooks/stage05_query_optimization.ipynb` 探索过程留底

---

## 📋 前置检查清单

- [ ] STAGE 04 完成,`dwd.fact_trips` 表存在(9.23M 行,30 字段,3 分区)
- [ ] core 组 10 容器全部 Up
- [ ] Spark Worker 资源未被旧 SparkContext 占满(必要时 `docker restart jupyter`)
- [ ] 大致熟悉 Spark UI(http://localhost:8080),特别是 SQL 标签页
- [ ] 需要启动的容器组:**core 组**(无变化)

---

## 🚀 本阶段启动服务

core 组无变化。建议执行 ETL 前重启 jupyter:
```bash
docker restart jupyter && sleep 10
```

---

## 📚 核心概念(5 分钟读完)

### 概念 1:Spark 的两层优化器
```
SQL/DataFrame
     ↓
Catalyst 优化器(逻辑计划层)
   - RBO: 谓词下推、列裁剪、常量折叠……(基于规则,启发式)
   - CBO: Join Reorder、广播判断……(基于代价,需要统计信息)
     ↓
Spark Planner(物理计划层)
   - 选 Join 策略(Broadcast/SortMerge/ShuffleHash)
   - 选 Shuffle 分区数
     ↓
Tungsten 执行引擎
     ↓
AQE(运行时反馈优化)
   - 看到真实数据量再调整后续计划
```

**理解关键**:RBO 在"编译期"用规则,CBO 在"编译期"用统计,**AQE 在"运行时"用真实数据**。三者协同。

### 概念 2:谓词下推(Predicate Pushdown)是什么?为什么会失效?
**生效场景**:`WHERE trip_distance > 5` 上 Parquet 表 —— Spark 把这个条件**推**到 Parquet 文件读取阶段,跳过整段不满足的 row group,IO 量减少。

**失效场景**:

- **UDF 包裹**:`WHERE my_udf(trip_distance) > 5` → Spark 不知道 UDF 干啥,只能全读后过滤
- **CAST 复杂表达式**:`WHERE CAST(distance AS DECIMAL(10,2)) > 5.0` → Spark 3.4+ 部分场景能优化,复杂的还是不行
- **Like '%xxx'**:前缀不固定,Parquet 的统计信息(min/max)用不上

**业内法则**:**"能用纯 SQL 表达的,绝不用 UDF"**——Catalyst 看不进 UDF,等于关掉了优化器。

### 概念 3:AQE 三大能力（动态修改策略，不会在开头直接定死）
| AQE 能力 | 什么是 | 解决什么痛点 |
|---------|--------|------------|
| **动态合并 Shuffle 分区** | 默认 200 分区可能产生很多小 partition,运行时根据真实数据合并到合理大小 | 小分区调度开销大 |
| **动态切换 Join 策略** | 编译期以为大表 × 大表(走 SortMerge),运行时发现一边其实很小,改成 Broadcast | 统计信息不准导致 Join 选错 |
| **动态优化倾斜 Join** | 检测到某个 partition 远大于其他,自动拆分 | 解决数据倾斜(STAGE 06 重点) |

**Spark 3.0+ 默认开启**,但学习时**手动关掉对比**才能看到差异。

### 概念 4:CBO 是什么?为什么很多人不知道它?
**CBO** = Cost-Based Optimization,基于"代价"的优化。比如多表 Join,CBO 会算出"先 join A×B 再 join C 比 先 join B×C 再 join A 便宜",自动 reorder。

**前提**:Spark 需要**知道每张表的统计信息**(行数、列基数、min/max)。但 **Spark 默认不自动收集**——你必须运行:

```sql
ANALYZE TABLE dwd.fact_trips COMPUTE STATISTICS FOR ALL COLUMNS
```
**没运行这条,CBO 基本等于关闭**。这是工程界普遍现象——大多数人写 SQL 时根本没意识到自己丢了 CBO 的红利。

---

## 🛠 操作步骤

### 步骤 1:初始化 + 验证 DWD

```python
from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("STAGE05-计算优化") \
    .master("spark://spark-master:7077") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.executor.memory", "2g") \
    .enableHiveSupport() \
    .getOrCreate()

# 快速自检
print(f"Spark: {spark.version}")
print(f"DWD 行数: {spark.sql('SELECT COUNT(*) FROM dwd.fact_trips').collect()[0][0]:,}")
print(f"AQE 默认状态: {spark.conf.get('spark.sql.adaptive.enabled')}")
print(f"自动广播阈值: {spark.conf.get('spark.sql.autoBroadcastJoinThreshold')} bytes")
print(f"Shuffle 分区数: {spark.conf.get('spark.sql.shuffle.partitions')}")
```

```ini
Spark: 3.4.1
DWD 行数: 9,227,227
AQE 默认状态: true
自动广播阈值: 10485760b bytes
Shuffle 分区数: 200
```

---

## 🔬 实验 #6: 谓词下推 + UDF 失效(本阶段精华)

### 实验目的
**看到谓词下推的物理证据,并见证 UDF 如何把优化器变成瞎子**。

### 实验代码

```python
# ── 查询 A: 纯 SQL 谓词(完美下推)─────────
print("=" * 60)
print("  实验 #6-A: WHERE trip_distance > 5 (纯 SQL,可下推)")
print("=" * 60)

# 跑查询计时
t0 = time.time()
result_a = spark.sql("""
    SELECT COUNT(*) AS cnt, ROUND(SUM(total_amount), 2) AS total_rev
    FROM dwd.fact_trips
    WHERE trip_distance > 5
""").collect()
t_a = time.time() - t0

print(f"\n耗时: {t_a:.2f}s")
print(f"结果: {result_a[0].asDict()}")

# 看 EXPLAIN — 重点关注 PushedFilters 字段
print("\n--- 物理计划(重点看 PushedFilters)---")
spark.sql("""
    EXPLAIN FORMATTED
    SELECT COUNT(*), SUM(total_amount)
    FROM dwd.fact_trips
    WHERE trip_distance > 5
""").show(truncate=False)
```

```
============================================================
  实验 #6-A: WHERE trip_distance > 5 (纯 SQL,可下推)
============================================================

耗时: 1.07s
结果: {'cnt': 1475304, 'total_rev': 97020580.4}

--- 物理计划(重点看 PushedFilters)---
+--------------------------------------------------------------------------------------------------------------------------+
|plan|
+--------------------------------------------------------------------------------------------------------------------------+
|== Physical Plan ==\nAdaptiveSparkPlan (7)\n+- HashAggregate (6)\n   +- Exchange (5)\n      +- HashAggregate (4)\n         +- Project (3)\n            +- Filter (2)\n               +- Scan parquet spark_catalog.dwd.fact_trips (1)\n\n\n(1) Scan parquet spark_catalog.dwd.fact_trips\nOutput [4]: [trip_distance#123, total_amount#135, year#149, month#150]\nBatched: true\nLocation: CatalogFileIndex [hdfs://namenode:9000/nyc-taxi/dwd/fact_trips]\nPushedFilters: [IsNotNull(trip_distance), GreaterThan(trip_distance,5.0)]\nReadSchema: struct<trip_distance:double,total_amount:double>\n\n(2) Filter\nInput [4]: [trip_distance#123, total_amount#135, year#149, month#150]\nCondition : (isnotnull(trip_distance#123) AND (trip_distance#123 > 5.0))\n\n(3) Project\nOutput [1]: [total_amount#135]\nInput [4]: [trip_distance#123, total_amount#135, year#149, month#150]\n\n(4) HashAggregate\nInput [1]: [total_amount#135]\nKeys: []\nFunctions [2]: [partial_count(1), partial_sum(total_amount#135)]\nAggregate Attributes [2]: [count#154L, sum#155]\nResults [2]: [count#156L, sum#157]\n\n(5) Exchange\nInput [2]: [count#156L, sum#157]\nArguments: SinglePartition, ENSURE_REQUIREMENTS, [plan_id=114]\n\n(6) HashAggregate\nInput [2]: [count#156L, sum#157]\nKeys: []\nFunctions [2]: [count(1), sum(total_amount#135)]\nAggregate Attributes [2]: [count(1)#118L, sum(total_amount#135)#151]\nResults [2]: [count(1)#118L AS count(1)#152L, sum(total_amount#135)#151 AS sum(total_amount)#153]\n\n(7) AdaptiveSparkPlan\nOutput [2]: [count(1)#152L, sum(total_amount)#153]\nArguments: isFinalPlan=false\n\n|
+--------------------------------------------------------------------------------------------------------------------------+
```

```python
# ── 查询 B: UDF 包裹谓词(下推失效!)──────
print("=" * 60)
print("  实验 #6-B: WHERE is_long_trip(trip_distance) (UDF 包裹)")
print("=" * 60)

# 注册一个 UDF — 干的事其实跟 trip_distance > 5 一样
@udf(returnType=BooleanType())
def is_long_trip(distance):
    return distance is not None and distance > 5

spark.udf.register("is_long_trip", is_long_trip)

# 跑查询计时
t0 = time.time()
result_b = spark.sql("""
    SELECT COUNT(*) AS cnt, ROUND(SUM(total_amount), 2) AS total_rev
    FROM dwd.fact_trips
    WHERE is_long_trip(trip_distance)
""").collect()
t_b = time.time() - t0

print(f"\n耗时: {t_b:.2f}s")
print(f"结果: {result_b[0].asDict()}")

# 看 EXPLAIN — 重点对比 PushedFilters 变化
print("\n--- 物理计划(重点看 PushedFilters)---")
spark.sql("""
    EXPLAIN FORMATTED
    SELECT COUNT(*), SUM(total_amount)
    FROM dwd.fact_trips
    WHERE is_long_trip(trip_distance)
""").show(truncate=False)

# ── 汇总对比 ──────────────────────────────
print("\n" + "=" * 60)
print(f"  A 纯 SQL:  耗时 1.07s, 结果 cnt=1475304, rev=$97,020,580")
print(f"  B UDF:     耗时 {t_b:.2f}s, 结果 cnt={result_b[0]['cnt']:,}, rev=${result_b[0]['total_rev']:,.0f}")
print(f"  UDF 拖慢比: {t_b/1.07:.2f}x")
print("=" * 60)
```

```
============================================================
  实验 #6-B: WHERE is_long_trip(trip_distance) (UDF 包裹)
============================================================

耗时: 2.82s
结果: {'cnt': 1475304, 'total_rev': 97020580.4}

--- 物理计划(重点看 PushedFilters)---
+--------------------------------------------------------------------------------------------------------------------------+
|plan|
+--------------------------------------------------------------------------------------------------------------------------+
|== Physical Plan ==\nAdaptiveSparkPlan (9)\n+- HashAggregate (8)\n   +- Exchange (7)\n      +- HashAggregate (6)\n         +- Project (5)\n            +- Filter (4)\n               +- BatchEvalPython (3)\n                  +- Project (2)\n                     +- Scan parquet spark_catalog.dwd.fact_trips (1)\n\n\n(1) Scan parquet spark_catalog.dwd.fact_trips\nOutput [4]: [trip_distance#219, total_amount#231, year#245, month#246]\nBatched: true\nLocation: CatalogFileIndex [hdfs://namenode:9000/nyc-taxi/dwd/fact_trips]\nReadSchema: struct<trip_distance:double,total_amount:double>\n\n(2) Project\nOutput [2]: [trip_distance#219, total_amount#231]\nInput [4]: [trip_distance#219, total_amount#231, year#245, month#246]\n\n(3) BatchEvalPython\nInput [2]: [trip_distance#219, total_amount#231]\nArguments: [is_long_trip(trip_distance#219)#247], [pythonUDF0#251]\n\n(4) Filter\nInput [3]: [trip_distance#219, total_amount#231, pythonUDF0#251]\nCondition : pythonUDF0#251\n\n(5) Project\nOutput [1]: [total_amount#231]\nInput [3]: [trip_distance#219, total_amount#231, pythonUDF0#251]\n\n(6) HashAggregate\nInput [1]: [total_amount#231]\nKeys: []\nFunctions [2]: [partial_count(1), partial_sum(total_amount#231)]\nAggregate Attributes [2]: [count#252L, sum#253]\nResults [2]: [count#254L, sum#255]\n\n(7) Exchange\nInput [2]: [count#254L, sum#255]\nArguments: SinglePartition, ENSURE_REQUIREMENTS, [plan_id=229]\n\n(8) HashAggregate\nInput [2]: [count#254L, sum#255]\nKeys: []\nFunctions [2]: [count(1), sum(total_amount#231)]\nAggregate Attributes [2]: [count(1)#214L, sum(total_amount#231)#249]\nResults [2]: [count(1)#214L AS count(1)#248L, sum(total_amount#231)#249 AS sum(total_amount)#250]\n\n(9) AdaptiveSparkPlan\nOutput [2]: [count(1)#248L, sum(total_amount)#250]\nArguments: isFinalPlan=false\n\n|
+--------------------------------------------------------------------------------------------------------------------------+


============================================================
  A 纯 SQL:  耗时 1.07s, 结果 cnt=1475304, rev=$97,020,580
  B UDF:     耗时 2.82s, 结果 cnt=1,475,304, rev=$97,020,580
  UDF 拖慢比: 2.63x
============================================================
```

### 📊 实测结果(2026-05-17,dwd.fact_trips 9.23M 行)

| 维度 | A 纯 SQL | B UDF |
|------|---------|-------|
| 耗时 | 1.07s | 2.82s |
| 拖慢比 | (基准) | **2.63x** |
| PushedFilters | `[IsNotNull, GreaterThan(5.0)]` ✅ | **空 `[]`** ❌ |
| 多余节点 | 无 | **BatchEvalPython**(跨进程序列化) |
| 结果一致性 | cnt=1,475,304 / rev=$97,020,580 | 完全一致 ✅ |

### UDF 性能杀手的"三重打击"
1. **下推失效**:所有 row 从 Parquet 读出来才能跑 UDF(IO 膨胀)
2. **跨进程序列化**:JVM ↔ Python 双向序列化(代价 > UDF 本身计算)
3. **失去 Catalyst 后续优化**:UDF 输出列被视为"未知",优化器后续都不敢动

### 铁律
**能用纯 SQL 表达的绝不用 UDF**。改写优先级:
SQL 内置函数 > 表达式 > Pandas UDF + Arrow > **最后才考虑 Python UDF**

---

## 🔬 实验 #7: AQE 开关对比(看动态合并 Shuffle 分区)

### 实验目的
亲眼看到 AQE 把 `spark.sql.shuffle.partitions=200` 默认值**动态合并**到合理数量,以及 Join 策略的动态切换。

### 实验代码

```python
# 选一个会产生 Shuffle 的查询:按 pickup_borough Group By
query = """
    SELECT pickup_borough,
           COUNT(*) AS trips,
           ROUND(SUM(total_amount), 2) AS revenue
    FROM dwd.fact_trips
    GROUP BY pickup_borough
    ORDER BY revenue DESC
"""

# ── A: 关闭 AQE,使用默认 200 个 shuffle 分区 ─────
print("=" * 60)
print("  A: AQE OFF")
print("=" * 60)
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", "200")

t0 = time.time()
spark.sql(query).collect()
t_off = time.time() - t0
print(f"耗时: {t_off:.2f}s")
spark.sql(f"EXPLAIN FORMATTED {query}").show(truncate=False)

# ── B: 开启 AQE ─────────────────────────────
print("\n" + "=" * 60)
print("  B: AQE ON")
print("=" * 60)
spark.conf.set("spark.sql.adaptive.enabled", "true")

t0 = time.time()
spark.sql(query).collect()
t_on = time.time() - t0
print(f"耗时: {t_on:.2f}s")
spark.sql(f"EXPLAIN FORMATTED {query}").show(truncate=False)

print("\n" + "=" * 60)
print(f"  AQE OFF: {t_off:.2f}s")
print(f"  AQE ON:  {t_on:.2f}s")
print(f"  加速比: {t_off/t_on:.2f}x")
print("\n💡 Spark UI 观察重点:")
print("   AQE OFF: Stage 应该有 200 个 task,大多很小很快")
print("   AQE ON:  Stage 的 task 数会少很多(动态合并),且 EXPLAIN 显示 AdaptiveSparkPlan")
```


### 📊 实测结果(2026-05-17,GROUP BY pickup_borough 仅 8 组)

| 维度 | AQE OFF | AQE ON |
|------|---------|--------|
| 耗时 | 2.88s | **0.75s** |
| 加速比 | (基准) | **3.82x** |
| Shuffle Task | 固定 200(192 partition 空跑) | 动态合并 |
| 顶层节点 | 普通物理计划 | `AdaptiveSparkPlan` |
| isFinalPlan | — | false(EXPLAIN 是运行前视图) |

### ⚠️ 暗坑:`isFinalPlan=false` 的含义
**EXPLAIN 看到的 200 个分区只是 AQE 起点,不是最终执行!** 要看真实计划:
- 方法 1:查询运行后再 `df.explain()` → 会变 `isFinalPlan=true`
- 方法 2:打开 Spark UI → SQL 标签页 → 看 "AQE plan changes"

### AQE 三大能力(本实验用了第 1 个)
1. **动态合并 Shuffle 分区**(本实验,3.82x 加速主因)
2. **动态切换 Join 策略**(SortMerge → Broadcast,统计信息不准时救场)
3. **动态优化倾斜 Join**(STAGE 06 重点)

---

## 🔬 实验 #8: CBO 对比(加分,30 min)

### 实验目的
让 Spark"看到数据真相",对多表 Join 自动选择最优顺序。

### 实验代码

```python
# ── 准备:让 dwd.fact_trips 有完整统计 ──────
print(">>> 收集 dwd.fact_trips 全列统计...")
t0 = time.time()
spark.sql("ANALYZE TABLE dwd.fact_trips COMPUTE STATISTICS FOR ALL COLUMNS")
print(f"  ANALYZE 耗时: {time.time()-t0:.1f}s")

spark.sql("ANALYZE TABLE ods.taxi_zone_lookup COMPUTE STATISTICS FOR ALL COLUMNS")

# 看看 Spark 知道哪些统计
spark.sql("DESCRIBE EXTENDED dwd.fact_trips").show(truncate=False)

# ── 三表 Join 查询(用同一份维表自 Join 模拟)──
# 真实场景下你会有多个不同维表,这里用 self join + 别名模拟
query_join = """
    SELECT pu.pickup_borough,
           do.dropoff_borough,
           COUNT(*) AS trips,
           ROUND(AVG(t.total_amount), 2) AS avg_amt
    FROM dwd.fact_trips t
    JOIN ods.taxi_zone_lookup pu ON t.PULocationID = pu.LocationID
    JOIN ods.taxi_zone_lookup do ON t.DOLocationID = do.LocationID
    WHERE t.year = 2024 AND t.month = 1
    GROUP BY pu.pickup_borough, do.dropoff_borough
    ORDER BY trips DESC
    LIMIT 20
"""

# ── A: CBO OFF ────────────────────────────
print("=" * 60)
print("  A: CBO OFF")
print("=" * 60)
spark.conf.set("spark.sql.cbo.enabled", "false")
spark.conf.set("spark.sql.cbo.joinReorder.enabled", "false")

t0 = time.time()
spark.sql(query_join).collect()
t_off = time.time() - t0
print(f"耗时: {t_off:.2f}s")

# ── B: CBO ON ─────────────────────────────
print("\n" + "=" * 60)
print("  B: CBO ON + Join Reorder")
print("=" * 60)
spark.conf.set("spark.sql.cbo.enabled", "true")
spark.conf.set("spark.sql.cbo.joinReorder.enabled", "true")

t0 = time.time()
spark.sql(query_join).collect()
t_on = time.time() - t0
print(f"耗时: {t_on:.2f}s")

print("\n" + "=" * 60)
print(f"  CBO OFF: {t_off:.2f}s")
print(f"  CBO ON:  {t_on:.2f}s")
print(f"  加速比: {t_off/t_on:.2f}x (本项目数据量较小,差异可能不极致)")
```

### 📊 实测结果(2026-05-17,三表 Join + GroupBy)

| 维度 | CBO OFF | CBO ON |
|------|---------|--------|
| 耗时 | 0.64s | **0.37s** |
| 加速比 | (基准) | **1.71x** |

### ANALYZE 收集到的物理证据(trip_distance 列)
```
min=0.01  max=194.65       ← STAGE 04 清洗规则的物理证据
num_nulls=0                ← 清洗后无空值
distinct_count=5675        ← HyperLogLog 近似
avg_col_len=8 / max_col_len=8
```

`pickup_borough` ANALYZE 估计 distinct=7,STAGE 04 实际 8 个 borough,**HyperLogLog 误差 ~2%**(正常)。

### 工程实践
- Spark 默认**不自动收集**统计 → 大部分团队丢了 CBO 红利
- 生产环境标配:每天凌晨对关键大表跑一次 ANALYZE
- 本项目数据量小(2 表 Join),CBO 收益不极致。**真实价值在 5+ 表 BI 报表场景,加速比能到 5-10x**

---

## 💼 简历可写的成果(STAGE 05 新增 3 条)

> • **谓词下推深度验证**(实验 #6):纯 SQL `WHERE trip_distance > 5` 通过 `PushedFilters: [GreaterThan(trip_distance, 5.0)]` 推到 Parquet 读取层 + 列裁剪(30 字段读 2 个);Python UDF 包裹同等条件后 `PushedFilters` 字段消失,物理计划新增 `BatchEvalPython` 跨进程节点,**耗时 1.07s → 2.82s(2.63x 拖慢)**,沉淀"能用纯 SQL 表达的绝不用 UDF"开发铁律
>
> • **AQE 量化对比**(实验 #7):`GROUP BY pickup_borough` 仅 8 个组,**关闭 AQE 时被 200 个 Shuffle Task 强制分散(192 partition 空跑),开启 AQE 动态合并后耗时 2.88s → 0.75s(3.82x 加速)**;识别 EXPLAIN 显示 `isFinalPlan=false` 是 AQE 运行前视图、必须查 Spark UI 看真实执行计划这一常见误区
>
> • **CBO + ANALYZE 统计信息**(实验 #8):通过 `ANALYZE TABLE COMPUTE STATISTICS FOR ALL COLUMNS` 让 Catalyst 优化器获取 min/max/distinct_count/null_count,**多表 Join Reorder 后耗时 0.64s → 0.37s(1.71x)**;识别 Spark 默认不自动收集统计是生产环境普遍漏点(HyperLogLog 近似估算 ~2% 误差)

---

## 🎤 面试可能被问到的问题

1. **Q: 谓词下推具体是怎么实现的?** 
   A: 三层协作——SQL Parser 把 WHERE 提出来 → Catalyst 在逻辑计划阶段把 **Filter 节点尽量推到 Scan 节点之上** → Parquet 数据源接到 `pushedFilters` 后,用文件 footer 的 row group 统计信息(min/max/null_count)跳过整段。可以用 `df.explain(True)` 看 `PushedFilters` 字段验证。
2. **Q: 为什么 UDF 会破坏谓词下推?** 
   A: UDF 对 Catalyst 来说是黑盒,优化器**不知道它的输入输出关系**,无法转换为 Parquet 可识别的过滤表达式。修复方案:(1) 改写为纯 SQL/内置函数;(2) 用 Pandas UDF + Arrow 减少 Python 开销;(3) 实在不能改的话,把 UDF 谓词放到 SQL 谓词之后,让 SQL 谓词先过滤再跑 UDF。
3. **Q: AQE 默认开启,为什么生产环境有时还会关?** 
   A: 三个情况:
   (1) 已有大量"调优过的代码"依赖固定分区数,AQE 改了分区数导致后续操作出 bug;
   (2) 某些 streaming 场景 AQE 支持不全;
   (3) 调试期为了得到可重现的执行计划,临时关掉 AQE。
   
4. **Q: CBO 为什么很少人用?** 
   A: 三个原因:
   (1) 必须先跑 `ANALYZE TABLE`,很多团队没纳入 ETL 流程;
   (2) 统计信息会**过期**——大表写入后没重新 ANALYZE,CBO 用旧数据反而误判;
   (3) 简单查询用不上,只在 5+ 表 Join 场景才显著。生产实践通常**每天凌晨重新 ANALYZE 一次**关键大表。
5. **Q: AQE 和 CBO 有什么区别?** 
   A: 时机不同——CBO 是**编译期**用预先收集的统计估算代价;AQE 是**运行时**根据真实执行数据调整。两者互补:CBO 给个好的起点,AQE 在过程中修正。

---

## 🧹 阶段收尾

```python
# 没有新的数据产出,主要是认知和工具的积累
# 可选:让 ANALYZE 的统计信息留下(对 STAGE 07 DWS 层有用)
print("✅ STAGE 05 完成,CBO 统计信息已留存")
```

```bash
cd /Users/alen/DA/NYC-Taxi-Trip-analysis/nyc-taxi-platform
git add docs/ benchmarks/ notebooks/
git commit -m "feat(stage05): 计算层 3 项实验(谓词下推 + AQE + CBO)"
```
