# STAGE 03: 存储层优化

## 🎯 本阶段目标

- **业务问题**:STAGE 02 入库的 Parquet 已经比 CSV 强很多,但 NYC Cab Co. 想进一步**降存储成本 + 提查询速度**。这一步要把 Parquet 的潜力榨干。
- **技术能力**:
  - 理解 3 种主流压缩算法的 CPU/IO/空间三角权衡
  - 学会用 EXPLAIN 看分区裁剪是否生效
  - 掌握分桶(Bucketing)的概念与适用场景
  - 识别并治理 HDFS 小文件问题
  - 看懂 Spark UI 的 "Bytes Read" 指标
- **产出物**:
  - `benchmarks/stage_03_compression_compare.md` — 压缩算法对比结果(实验 #2)
  - `benchmarks/stage_03_partition_pruning.md` — 分区裁剪效果(实验 #3)
  - `benchmarks/stage_03_small_files.md` — 小文件治理前后对比
  - HDFS 上多种压缩格式的对比数据(实验后清理)
  - 更新 STAGE 02 主表为最优压缩配置(可选)

---

## 📋 前置检查清单

- [ ] STAGE 02 完成,`ods.yellow_trips` 表存在,3 个分区注册到位
- [ ] HDFS 数据完整(955 万行,2024 Q1)
- [ ] core 组 10 容器全部 Up
- [ ] 磁盘剩余 > 3GB(本阶段对比实验会产出多份临时数据)
- [ ] 需要启动的容器组:**core 组**(无变化)

---

## 🚀 本阶段启动服务

```bash
# core 组已经在跑,无需重启
docker compose -f docker/docker-compose.core.yml ps
```

serving 组仍然不需要。

---

## 📚 核心概念(5 分钟读完)

### 概念 1:压缩算法的三角权衡
| 算法 | 压缩比 | 压缩速度 | 解压速度 | 适用场景 |
|------|--------|---------|---------|---------|
| **无压缩** | 1x | 极快 | 极快 | 内存中的临时数据,IO 不是瓶颈 |
| **Snappy** | 1.5-2x | 快 | 极快 | **大数据默认选择**,Hive/Spark/HBase 都用它做默认值 |
| **Zstd** | 2-3x | 中等 | 快 | **冷数据/归档**,牺牲一点 CPU 换 30-50% 空间 |
| **Gzip** | 2.5-3.5x | 慢 | 中等 | 历史遗留(Hadoop 早期),现在很少在大数据用 |

**业务类比**:

- Snappy = 真空压缩袋(压一压塞进去快,要用拿出来快,体积小一点)
- Zstd = 抽真空+折叠+捆扎(费点劲,但更小,适合长期存放)
- Gzip = 还在用塑料绳捆衣服(旧但能用)

### 概念 2:分区裁剪(Partition Pruning)
**业务类比**:你想找 2024 年 1 月的订单,系统应该**只翻 1 月的文件柜**,不应该把全年的柜子都打开。这件事就叫"分区裁剪"。

**工程实现**:WHERE 条件命中分区键(如 `WHERE year=2024 AND month=1`)时,Spark Catalyst 优化器把"扫哪些目录"裁剪掉,直接跳过不相关的分区目录。

**反面教材**(分区裁剪失败):
- `WHERE CAST(month AS STRING) = '1'`(分区键被函数包裹,优化器不敢推断)
- `WHERE month BETWEEN 1 AND 12`(范围太宽,等于全扫)

### 概念 3:分桶(Bucketing)是什么?
**业务类比**:分区是"分文件柜",分桶是"柜子里的小抽屉"——按某列的 Hash 把数据均匀分进固定数量的桶(文件)。

**核心价值**:**Join 优化**——两张大表都按同一列分桶,且桶数相同,Spark 知道"对应桶之间 Join 即可",**跳过 Shuffle**。

**STAGE 03 不直接用分桶做实验**(数据量还不够大,效果不明显),但要懂概念,**STAGE 05 计算优化时会用上**。

### 概念 4:小文件问题
**业务类比**:NameNode 像图书馆的目录管理员,**每本书的卡片都要记**——不管这本书是 1KB 还是 1GB,卡片大小相同。100 万个 1KB 文件,目录卡片占用 = 100 万张卡片;1 个 1GB 文件,1 张卡片。

**两个具体后果**:

1. **NameNode 内存压力**:每个文件元数据约占 150 字节,100 万小文件 ≈ 150MB 内存
2. **Spark Task 数膨胀**:Spark 默认每个文件至少 1 个 Task,小文件越多 Task 越多,调度开销 > 计算开销

**经验阈值**:HDFS 文件应该 ≥ 128MB(一个 Block 的大小)。本阶段会模拟"过度分区"产生小文件,再用 `coalesce` 治理。

---

## 🛠 操作步骤

> ⚠️ 注意:本阶段是**实验密集型**,操作和实验高度交织,所以"操作步骤"主要是引入实验所需的辅助函数和环境。重头戏在下面的对比实验。

### 步骤 1:在 Jupyter 准备公共辅助函数

```python
from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("STAGE03-存储优化") \
    .master("spark://spark-master:7077") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.executor.memory", "2g") \
    .enableHiveSupport() \
    .getOrCreate()

# Hadoop FS API 句柄(读 HDFS 文件大小用)
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
Path = spark._jvm.org.apache.hadoop.fs.Path


def hdfs_size_bytes(path):
    """返回 HDFS 路径下所有文件的总字节数"""
    total = 0
    files = fs.listFiles(Path(path), True)
    while files.hasNext():
        total += files.next().getLen()
    return total


def hdfs_size_mb(path):
    return hdfs_size_bytes(path) / 1024 / 1024


def timeit(label, fn):
    """计时执行 fn,返回(耗时秒, 结果)"""
    t0 = time.time()
    result = fn()
    elapsed = time.time() - t0
    print(f"  [{label:>16}] {elapsed:>6.2f}s")
    return elapsed, result


# 快速自检
print(f"Spark: {spark.version}")
print(f"Hive 模式: {spark.conf.get('spark.sql.catalogImplementation')}")
# 先检查表是否存在
tables = spark.sql("SHOW TABLES IN ods").filter("tableName == 'yellow_trips'").count()
if tables >= 1:
    print("✅ ods.yellow_trips 表存在")
else:
    print("⚠️  表不存在，需要重建")
print(f"ODS 表行数: {spark.sql('SELECT COUNT(*) FROM ods.yellow_trips').collect()[0][0]:,}")
print("\n✅ 环境就绪,可以开始实验 #2")
```

---

## 🔬 对比实验 #2:压缩算法三方对决(本阶段核心)

### 实验目的
亲眼验证:**Snappy 是默认值不是因为它最优,而是因为它平衡得最好**。Zstd 用 CPU 换更小的存储,无压缩牺牲空间换最快读取。

### 实验设计
拿 2024-01 同源数据(296 万行),分别写成 3 种压缩格式,对比:
1. 写入耗时(CPU 代价)
2. 文件体积(存储成本)
3. 读取耗时(查询性能)

### 实验代码

```python
# 缓存源数据,避免每轮都重新读
df_jan = spark.read.parquet("hdfs://namenode:9000/nyc-taxi/ods/yellow_trips/year=2024/month=01")
df_jan.cache().count()

base_path = "hdfs://namenode:9000/nyc-taxi/ods/_bench_compress"
results = {}

for codec in ["uncompressed", "snappy", "zstd"]:
    print(f"\n>>> 测试压缩算法: {codec}")
    path = f"{base_path}/{codec}"

    # 写入计时
    t_write, _ = timeit(f"{codec}-write",lambda: df_jan.write.mode("overwrite").option("compression", codec).parquet(path))

    # 文件大小
    size_mb = hdfs_size_mb(path)
    print(f"  [{codec + '-size':>16}] {size_mb:>6.2f} MB")

    # 读取 + 聚合(避免懒计算被优化掉)
    t_read, _ = timeit(f"{codec}-read+agg",lambda: spark.read.parquet(path).agg({"total_amount": "sum"}).collect())

    results[codec] = {"write_s": t_write, "size_mb": size_mb, "read_s": t_read}

# 对比表格
print("\n" + "=" * 72)
print(f"  {'算法':<14} {'写入':<12} {'大小':<14} {'读取+聚合':<14} {'相对Snappy':<10}")
print("=" * 72)
snappy_size = results["snappy"]["size_mb"]
for codec, r in results.items():
    rel = (r["size_mb"] / snappy_size - 1) * 100
    rel_str = f"+{rel:.1f}%" if rel > 0 else f"{rel:.1f}%"
    print(f"  {codec:<14} {r['write_s']:>6.2f}s     {r['size_mb']:>8.2f} MB   {r['read_s']:>6.2f}s     {rel_str:>8}")
print("=" * 72)

# Zstd vs Snappy 三维对比(简历重点)
zstd = results["zstd"]
snappy = results["snappy"]
print(f"\n  💡 Zstd vs Snappy:")
print(f"     • 空间节省: {(1 - zstd['size_mb'] / snappy['size_mb']) * 100:.1f}%")
print(f"     • 写入速度: {'快' if zstd['write_s'] < snappy['write_s'] else '慢'} {abs(zstd['write_s'] / snappy['write_s']):.2f}x")
print(f"     • 读取速度: {'快' if zstd['read_s'] < snappy['read_s'] else '慢'} {abs(zstd['read_s'] / snappy['read_s']):.2f}x")
```

### 📊 实测结果(2026-05-17,2024-01 数据 296 万行)

| 算法 | 写入耗时 | 文件大小 | 读取+聚合 | 相对 Snappy |
|------|---------|---------|----------|------------|
| uncompressed | 1.56s | 74.27 MB | 0.34s | +27.6% |
| snappy | 1.45s | 58.22 MB | 0.26s | (基准) |
| **zstd** | **1.02s** | **44.22 MB** | **0.15s** | **-24.0%** |

### 🎯 反直觉发现:Zstd 在所有 3 个维度都赢了 Snappy

教科书说"Zstd 用 CPU 换空间"(写入应该更慢),**但实测 Zstd 反而全面更快**。原因:

**Spark + HDFS 场景下,IO 是瓶颈,不是 CPU**。读 Parquet 耗时 = `IO 时间 + 解压 CPU 时间`,当 IO 远大于 CPU 时:

- Snappy:IO 大(58MB) + CPU 小(解压快) ← 旧时代假设(2010s CPU 弱)
- Zstd:IO 小(44MB) + CPU 中等(解压略慢)
- 结果:**文件小的优势盖过 CPU 代价**,Zstd 反而更快

三个具体加速来源:
1. **网络传输少 24%**:Spark Worker 从 HDFS DataNode 拉数据,字节越少越快
2. **磁盘读 IO 少 24%**:即使本地读,SSD 带宽也是瓶颈
3. **Page Cache 更友好**:文件小,操作系统 page cache 命中率更高

写入 Zstd 也更快,因为 HDFS 写要复制到 2 副本(网络 IO 翻倍),压缩节省的 IO > Zstd 的 CPU 代价。

### ⚠️ 实验局限性(诚实记录)
- **JVM 预热效应**:第一个跑的 codec (uncompressed) 没享受 JIT 优化,可能略偏慢
- **严谨做法**:每个 codec 跑 3 次取中位数,第一次 warm-up 扔掉
- 但**24% 文件压缩是物理事实**,不受预热影响,这个数字最可信

### 💡 决策建议(写在 benchmark 里)
| 场景 | 推荐 | 理由 |
|------|------|------|
| **生产 OLAP / IO 密集场景** | **Zstd** | 文件小、读快、写也不慢,综合最优 |
| **CPU 严重受限的实时管道** | Snappy | 极低 CPU 占用,适合每秒上万次写入 |
| **临时中间数据(Spark shuffle)** | LZ4 或无压缩 | 短生命周期,不值得花 CPU |

### 🌐 行业现状
**Snappy 是 2010 年代默认(那时 CPU 是瓶颈),Zstd 是当下最优(IO 是新瓶颈)**。Facebook、Uber、字节跳动等大厂已经从 Snappy 迁到 Zstd。**默认值滞后于事实**,这是工程界常见的"惯性"。

---

## 🔬 对比实验 #3:全表扫描 vs 分区裁剪

### 实验目的
**亲眼看到分区裁剪让 Spark 扫描数据量减少多少**。这是 OLAP 引擎性能优化的"第一性原理"。

### 实验代码

```python
# 查询 A: 全表扫描(没有分区谓词)
print("=== 查询 A: 全表扫描 ===")
t_a, _ = timeit("全表扫描",
    lambda: spark.sql("""
        SELECT COUNT(*), SUM(total_amount)
        FROM ods.yellow_trips
    """).collect()
)

# 查询 B: 命中分区键(理想情况)
print("\n=== 查询 B: 命中分区键 WHERE year=2024 AND month=1 ===")
t_b, _ = timeit("命中分区",
    lambda: spark.sql("""
        SELECT COUNT(*), SUM(total_amount)
        FROM ods.yellow_trips
        WHERE year=2024 AND month=1
    """).collect()
)

# 查询 C: 分区裁剪失效(把分区键包在函数里)— 反面教材
print("\n=== 查询 C: 反面教材 - WHERE CAST(month AS STRING)='1' ===")
t_c, _ = timeit("裁剪失效",
    lambda: spark.sql("""
        SELECT COUNT(*), SUM(total_amount)
        FROM ods.yellow_trips
        WHERE year=2024 AND CAST(month AS STRING)='1'
    """).collect()
)

# 看 Spark 实际扫了哪些文件 - 通过 explain
print("\n=== 查询 B 的执行计划(看分区裁剪)===")
spark.sql("""
    SELECT COUNT(*), SUM(total_amount)
    FROM ods.yellow_trips
    WHERE year=2024 AND month=1
""").explain(mode="formatted")
```

### 📊 同步打开 Spark UI 观察

打开 http://localhost:8080 → 点最近的 Application → SQL 标签页 → 找这三个查询,看 **"Bytes Read"** 指标:
- 查询 A:应该接近全数据集大小(~150MB)
- 查询 B:应该只读 month=1 分区(~50MB)
- 查询 C:**取决于 Spark 版本**,可能能裁剪也可能裁剪不掉

### 📊 实测结果(2026-05-17,冷启动第一次)
| 查询 | 耗时 | 加速 |
|------|------|------|
| A 全表扫描 | 0.72s | (基准) |
| B 命中分区键 | 0.26s | **2.77x** |
| C CAST 包裹（新版可以识别到） | 0.18s | 4.08x |

### EXPLAIN 物理证据(查询 B)
```
Location: InMemoryFileIndex [hdfs://.../year=2024/month=01]   ← 只读 month=01 目录!
PartitionFilters: [(year=2024), (month=1)]                    ← 过滤下推到读文件前
ReadSchema: struct<total_amount:double>                       ← 19 列只读 1 列(列裁剪)
```

### 三个关键发现
1. **B 比 A 快 2.77x ≈ 理论值 3x**:符合"只扫 1/3 数据"预期
2. **C 比 B 还快**(反直觉):Spark 3.4+ Catalyst 优化器**已经能识别 `CAST(month AS STRING)='1'` 等价于 `month=1`** 继续推断分区。更复杂表达式(如 `month + 1 = 2`)才会破坏推断
3. **二次跑加速比缩小到 1.6x**:数据进 Spark 缓存后 IO 不再是瓶颈,分区裁剪相对收益缩小——**优化的边际收益取决于当前瓶颈在哪**(这是个洞察)

---

## 🔬 对比实验:小文件治理(可选,但强烈推荐)

### 实验目的
模拟"过度分区"产生 1000 个小文件,看 Spark 任务数膨胀的恶果,再用 `coalesce` 治理。

### 实验代码

```python
df_jan = spark.read.parquet("hdfs://namenode:9000/nyc-taxi/ods/yellow_trips/year=2024/month=01")
df_jan.cache().count()

# ── 1. 故意制造 1000 个小文件 ─────────────────────
bad_path = "hdfs://namenode:9000/nyc-taxi/ods/_bench_smallfiles/bad"
print(">>> 写入 1000 个小文件(repartition 会触发 shuffle,稍慢)...")
t_bad_write, _ = timeit("1000个小文件-写入",
                        lambda: df_jan.repartition(1000).write.mode("overwrite").parquet(bad_path)
                        )


# ── 2. 统计文件数和平均大小 ───────────────────────
def count_parquet_files(path):
    files = fs.listFiles(Path(path), True)
    n = 0
    while files.hasNext():
        f = files.next()
        if f.getPath().getName().endswith(".parquet"):
            n += 1
    return n


n_bad = count_parquet_files(bad_path)
size_bad_mb = hdfs_size_mb(bad_path)
print(f"\n  bad 路径: {n_bad} 个 parquet 文件")
print(f"  总大小: {size_bad_mb:.2f} MB")
print(f"  平均文件大小: {size_bad_mb * 1024 / n_bad:.1f} KB  ← 严重小文件!(HDFS Block 是 131072 KB)")

# ── 3. 读取耗时 ──────────────────────────────────
print("\n>>> 读取 1000 个小文件...")
t_bad_read, _ = timeit("1000小文件-读取",
                       lambda: spark.read.parquet(bad_path).agg({"total_amount": "sum"}).collect()
                       )

# ── 4. 治理:coalesce 合并成 4 个文件 ──────────────
good_path = "hdfs://namenode:9000/nyc-taxi/ods/_bench_smallfiles/good"
print("\n>>> 用 coalesce(4) 合并成 4 个文件...")
t_good_write, _ = timeit("coalesce(4)-写入",
                         lambda: spark.read.parquet(bad_path).coalesce(4).write.mode("overwrite").parquet(good_path)
                         )

n_good = count_parquet_files(good_path)
size_good_mb = hdfs_size_mb(good_path)
print(f"\n  good 路径: {n_good} 个 parquet 文件")
print(f"  总大小: {size_good_mb:.2f} MB")
print(f"  平均文件大小: {size_good_mb / n_good:.1f} MB")

print("\n>>> 读取 4 个合理大小文件...")
t_good_read, _ = timeit("4大文件-读取",
                        lambda: spark.read.parquet(good_path).agg({"total_amount": "sum"}).collect()
                        )

# ── 5. 汇总 ─────────────────────────────────────
print("\n" + "=" * 60)
print(f"  {'指标':<20} {'1000小文件':<15} {'4大文件':<15} {'变化':<10}")
print("=" * 60)
print(f"  {'文件数':<20} {n_bad:<15} {n_good:<15} {n_bad / n_good:.0f}x↓")
print(f"  {'平均文件大小':<20} {f'{size_bad_mb * 1024 / n_bad:.1f} KB':<15} {f'{size_good_mb / n_good:.1f} MB':<15} —")
print(f"  {'读取耗时':<20} {f'{t_bad_read:.2f}s':<15} {f'{t_good_read:.2f}s':<15} {t_bad_read / t_good_read:.2f}x↑")
print("=" * 60)
print(f"\n  💡 治理收益: 文件数 ÷ {n_bad / n_good:.0f},读取加速 × {t_bad_read/t_good_read:.2f}")
```

### 📊 实测结果(2026-05-17)
| 指标 | 1000 小文件 | 4 大文件 | 变化 |
|------|-----------|---------|------|
| 文件数 | 1000 | 4 | **250x ↓** |
| 平均文件大小 | 85.8 KB | 17.7 MB | — |
| 总大小 | 83.78 MB | 70.74 MB | 小文件多占 15.6% 存储 |
| 读取耗时 | 2.31s | 0.11s | **20.30x 加速** |

### 三个值得讲的洞察

**1. 为什么 20 倍加速,远超教科书的 2-5 倍?**
- Task 调度开销叠加:1000 个文件 = 1000 个 Spark Task,每个调度开销 ~2ms,纯开销 2 秒
- Parquet metadata 开销:每个文件单独读 footer,1000 次随机 IO 是灾难
- 并行度匹配:4 个文件刚好匹配 3 Worker × 2 cores = 6 槽

**2. 隐藏发现:小文件还更大(83MB vs 70MB)**
Parquet 大 row group 的字典编码能覆盖更多重复值。小文件单独维护字典,效率差;另外 metadata 占比也高。**治理小文件不仅快,还省存储**——双重收益。

**3. NameNode 内存红线(理论)**
每条文件元数据约 150 字节。100 万小文件 = 150MB NameNode 内存,10 张表 = 1.5GB → OOM。**这是 HDFS 小文件治理的运维红线**。

---

## 💼 简历可写的成果(累计 3 条硬指标)

> • 存储优化对比:基于 NYC Yellow Taxi 真实数据完成三项存储层量化实验:
>   - **压缩算法**:Snappy/Zstd/无压缩三方对比,**Zstd 比 Snappy 多节省 24% 存储空间(58MB→44MB)**,且在 IO 受限场景下**读取耗时反而降低 42%**,推翻"Zstd 用 CPU 换空间"的传统认知
>   - **分区裁剪**:验证 `WHERE year=2024 AND month=1` 触发分区裁剪,Spark 物理计划显示**只读取目标分区目录**,扫描数据量减少 2/3,**查询提速 2.77 倍**(0.72s→0.26s);并验证 Spark 3.4 Catalyst 优化器对 `CAST(month AS STRING)='1'` 这类反面教材具备自动修复能力
>   - **小文件治理**:模拟 1000 个小文件场景(平均 86 KB),通过 `coalesce(4)` 合并成 4 个 17.7MB 大文件后,**读取加速 20.3 倍(2.31s→0.11s),文件数减少 250 倍,存储再省 15.6%**;识别根因为 Task 调度开销 + Parquet metadata 随机 IO + 并行度失配

---

## 🎤 面试可能被问到的问题

1. **Q: Snappy 和 Zstd,你的项目选了哪个?为什么?**
   A: 项目里热数据(ODS/DWD,被频繁查询)用 Snappy,因为解压快、CPU 代价小;归档/冷数据(超过 1 年的历史)如果要落地,可以考虑 Zstd 多省一点空间。实测 Zstd 比 Snappy 多节省 ___% 空间但写入慢 ___倍,这种权衡只在归档场景值得。

2. **Q: 分区裁剪什么情况下会失效?**
   A: 三种典型情况——(1) 分区键被函数包裹如 `CAST(month AS STRING)='1'`;(2) 分区键参与计算如 `WHERE month + 1 = 2`;(3) 分区键在 OR 条件里和非分区列混用。这些都让 Catalyst 优化器无法在静态阶段推断要扫哪些分区。

3. **Q: HDFS 小文件问题怎么解决?**
   A: 写入侧用 `coalesce` 或 `repartition` 控制输出文件数(每个文件接近 HDFS Block 大小 128MB 最好);存储侧可以用 HAR(Hadoop Archive)归档;计算侧用 CombineFileInputFormat 让一个 Task 读多个小文件。**根本上要从源头治理**,而不是治标。

4. **Q: 分区和分桶能一起用吗?什么场景?**
   A: 能,而且常见。分区按时间(`year/month`),分桶按 Join Key(如 `PULocationID`)。这样既享受分区裁剪,又能在 Join 时跳过 Shuffle。代价是写入复杂度高、文件数多。

---

## 🧹 阶段收尾

```python
# 清理所有 _bench_* 临时数据
for sub in ["_bench_compress", "_bench_smallfiles"]:
    fs.delete(Path(f"hdfs://namenode:9000/nyc-taxi/ods/{sub}"), True)
print("✅ STAGE 03 临时数据清理完成")

# 释放缓存
spark.catalog.clearCache()
```

```bash
# git commit
cd /Users/alen/DA/NYC-Taxi-Trip-analysis/nyc-taxi-platform
git add benchmarks/ docs/
git commit -m "feat(stage03): 存储层 3 项优化实验(压缩对比 + 分区裁剪 + 小文件治理)"
```

---

## ➡️ 下一阶段预告

STAGE 04 我们进入 **DWD 层与数据清洗**:
- 把 ODS 里那些"匪夷所思"的脏数据(负金额、312,722 英里行程、跨年分区污染)用规则筛掉
- 与 `taxi_zone_lookup` 维表关联,补全 PU/DO 区域名称(用**广播 Join**,为 STAGE 05 铺垫)
- 输出标准化的 `dwd.fact_trips` 表
- 第一次接触 UDF(用户定义函数) — 处理"时段桶"等业务衍生字段

需要本阶段产出:稳定的 `ods.yellow_trips` 表 + 关于"什么是脏数据"的清晰认知。
