# NYC Taxi 大数据平台 · 复习速查手册

> 一份文档复习整个项目。面试前刷它。每块"是什么 + 为什么 + 原理 + 我的实测数据"。
> 配套:量化总账 [`benchmarks/_summary.md`](../benchmarks/_summary.md) · 总览 [`PROJECT_OVERVIEW.md`](PROJECT_OVERVIEW.md)

---

## 0. 一分钟项目自我介绍(背诵版)

> "我独立搭建了一个企业级大数据全栈平台,处理 NYC 出租车 955 万行数据。用 Docker Compose 部署了 HDFS + Spark 集群 + Hive + ClickHouse + PostgreSQL + Airflow + Superset 共 10 个组件,完整实现 ODS→DWD→DWS→ADS 四层数仓。做了 8 大类共 19 项量化优化,比如 CSV 转 Parquet 查询快 12 倍、PG 覆盖索引让点查快 254 倍、ClickHouse 比 Spark 聚合快 9.6 倍、数据倾斜用加盐法和 AQE 解决。还为运营、财务、司机三类用户分别做了 Superset 看板和 Streamlit 应用,用 Airflow 把 ETL 串成自动化 Pipeline 并验证了幂等性。过程中踩了 13 个真实工程坑,都沉淀成了文档。"

**面试官追问"最难的地方"** → 讲 **数据倾斜的反直觉发现** 或 **ClickHouse 导入的 3 个坑**(见第 7、8 节)。

---

## 1. 架构与数据流(一张图记住)

```
NYC TLC Parquet → HDFS → Spark
  → ODS(原样)→ DWD(清洗+维度)→ DWS(预聚合)→ ADS(面向应用)
                                                    ├→ PostgreSQL(点查:司机端/财务)
                                                    └→ ClickHouse(聚合:运营看板)
  Airflow 调度全链路 · Hive Metastore 管元数据
  消费:Superset 看板 / Streamlit 司机端 / Jupyter 探索
```

**四层数仓职责**(必背):

| 层 | 职责 | 一句话 | 表 |
|----|------|--------|-----|
| ODS | 原始层 | 原样保留,可追溯 | `ods.yellow_trips`(外部表) |
| DWD | 明细层 | 清洗 + 维度关联 + 衍生字段 | `dwd.fact_trips` |
| DWS | 汇总层 | 预聚合(日/小时粒度)【可以先进行汇总，然后ADS可以负责增长率和排名】 | `dws.daily_zone_revenue` |
| ADS | 应用层 | 面向具体业务,落 OLTP/OLAP | `ads.*`(PG) |

**为什么分层**:数据血缘清晰 / 复用性(DWD 算一次,DWS/ADS 都用)/ 稳定性(上游变只影响 ODS)。

---

## 2. 19 项量化指标速查(简历核心弹药)

| 类别 | 优化项 | 前 → 后 | 倍数 | 原理一句话 |
|------|--------|---------|------|-----------|
| 存储 | CSV→Parquet 查询 | 3.00s→0.25s | **12.2x** | 列裁剪 + 谓词下推,只读需要的列/行组 |
| 存储 | Parquet 压缩 | 308→58MB | **5.3x** | 列式存储 + Snappy,同列数据局部性好 |
| 存储 | Zstd vs Snappy | — | 空间-24%/读+42% | IO 是瓶颈,文件小盖过 CPU 代价 |
| 存储 | 分区裁剪 | 0.72s→0.26s | **2.77x** | WHERE 命中分区键,跳过无关目录 |
| 存储 | 小文件治理 | 2.31s→0.11s | **20.3x** | 1000→4 文件,省 Task 调度 + metadata IO |
| 计算 | Broadcast Join | 0.87s→0.23s | **3.75x** | 小表广播,主表零 Shuffle |
| 计算 | AQE 动态合并 | 2.88s→0.75s | **3.82x** | 200 空分区合并成几个,省 Task |
| 计算 | CBO Join Reorder | 0.64s→0.37s | **1.71x** | ANALYZE 统计后选最优 Join 顺序 |
| 计算 | UDF 拖慢(反面) | 1.07s→2.82s | 慢 2.63x | UDF 破坏谓词下推 + 跨进程序列化 |
| 倾斜 | 加盐法(最长Task) | 855→138ms | **6.2x** | 热 key 加随机后缀,拆成 16 份并行 |
| 倾斜 | AQE Skew Join | 2.17s→0.75s | **2.89x** | 自动拆分大 partition,零 SQL 修改 |
| 查询 | DWS 预聚合行数 | 9.23M→20k | **452x** | 提前算好,下游扫几 MB 而非几百 MB |
| 查询 | 窗口函数 vs 自连接 | 0.54s→0.32s | **1.70x** | LAG 单次扫描,省掉 Join 的 2 次 Shuffle |
| 查询 | HyperLogLog | 2.28s→0.71s | **3.20x** | 近似去重,误差 2.33% 换速度 |
| OLAP | ClickHouse vs Spark | 1.805s→0.187s | **9.6x** | 无 JVM 启动 + 向量化 + 稀疏索引 |
| 索引 | PG 覆盖索引 | 6.10ms→0.024ms | **254x** | Index Only Scan 消除回表,IO 680x↓ |

---

## 3. 核心概念速查(每个:是什么+为什么+原理)

### 存储格式

#### **Parquet(列式存储)**

- 是什么:按列存储的文件格式
- 为什么快:① **列裁剪**(SELECT 几列只读几列)② **谓词下推**(文件头记录每个 row group 的 min/max,WHERE 跳过整段)
- 对比 CSV:CSV 行式,要看一列必须读整行

#### **压缩算法选型**

| 算法 | 压缩比 | 速度 | 用 |
|------|--------|------|----|
| Snappy | 中 | 极快 | 大数据默认(历史最优) |
| Zstd | 高 | 中 | **IO 瓶颈场景最优**(当下推荐) |
| Gzip | 高 | 慢 | 旧,少用 |
- **反直觉**:实测 Zstd 全面超 Snappy,因为 Spark+HDFS 是 IO 瓶颈不是 CPU 瓶颈,文件小省的 IO > 多花的 CPU

#### **分区 vs 分桶**

先记住一句话：**分区改变的是“目录结构”（搞文件夹），分桶改变的是“文件本身”（拆分数据文件）。**

1. **分区 (Partitioning) —— “分类建文件夹”**

假设你是学校档案管理员，要把全校 10 万名学生的档案存进电脑。

- **做法**：你按照“年级”来建文件夹。2024级一个文件夹、2025级一个文件夹、2026级一个文件夹。
- **底层对应**：在 HDFS（分布式文件系统）中，**分区直接对应着不同的子目录**。
- **痛点**：如果按照“身份证号”来分区，由于每个人身份证都不同，电脑会创建 10 万个文件夹。在分布式系统里，**文件夹太多会让系统崩溃**（NameNode 内存吃不消）。

2. **分桶 (Bucketing) —— “把数据均匀分堆”**

为了解决上面“高基数特征（比如用户ID、商品ID）无法分区”的问题，**分桶**登场了。

分桶不再建文件夹，而是在**同一个文件夹内部**，把数据规整地拆分成**固定数量的文件**。

- **做法**：比如你决定建 **4个桶**（文件）。当来了一个新学生的数据，系统怎么知道它该去哪个桶？系统会用一个固定的数学公式：

  $$\text{桶号} = \text{hash}(\text{分桶字段}) \pmod {\text{总桶数}}$$

  比如：

  - 学生 A 的 ID 是 1001，`hash(1001) % 4 = 1` ----> 丢进 `000001_0` 这个文件。
  - 学生 B 的 ID 是 1002，`hash(1002) % 4 = 2` ---->  丢进 `000002_0` 这个文件。
  - 学生 C 的 ID 是 1005，`hash(1005) % 4 = 1` ---->  同样丢进 `000001_0` 这个文件。

- **底层对应**：无论你有多少数据，**桶的数量在建表时就是固定好的**。4个桶就永远只有4个文件，绝不会因为数据量大就疯狂暴增文件夹。

**分区 VS 分桶 深度对比**

| **特性**     | **分区 (Partitioning)**                  | **分桶 (Bucketing)**                     |
| ------------ | ---------------------------------------- | ---------------------------------------- |
| **物理表现** | 表现为 **HDFS 子目录** (文件夹)          | 表现为 **HDFS 子文件** (数据文件)        |
| **数量控制** | 数量不固定，由字段的实际动态值决定       | **数量固定**，由你建表时手动指定         |
| **适合字段** | 低基数字段（如：日期、国家、省份、年级） | 高基数字段（如：用户ID、商品ID、订单号） |
| **核心目的** | 过滤掉无关目录，**避免全表扫描**         | 优化**大表 Join**、数据抽样、防数据倾斜  |

#### **HDFS 小文件问题**

- 危害:① NameNode 内存(每个文件 ~150 字节元数据)② Spark Task 数膨胀(每文件至少 1 task)
- 解决:`coalesce(N)` 合并、HAR 归档、CombineFileInputFormat
- 经验:文件应 ≥ 128MB(一个 Block)



### 计算优化

#### **谓词下推(Predicate Pushdown)**

- 是什么:把 WHERE 条件"推"到读文件阶段,早过滤
- 失效杀手:**UDF**(Catalyst 看不进 UDF,PushedFilters 变空)
- 验证:EXPLAIN 看 `PushedFilters: [...]` 字段

#### **Broadcast Join vs Shuffle Join**

- Join 本质:让相同 key 相遇
- Shuffle Join:两表都按 key 重分区(搬数据),大表慢
- Broadcast Join:小表(<10MB)复制给每个 Executor,大表不动
- 物理计划:`BroadcastHashJoin` vs `SortMergeJoin`
- 限制:小表太大会 OOM;只能广播 Join 的一侧

#### **AQE(自适应查询执行)** ★面试高频

- 核心思想:**别全靠编译期猜,跑一段看真实数据再调整后面计划**(像实时导航)
- 三大能力:
  1. 动态合并 Shuffle 分区(200 空分区→几个)
  2. 动态切换 Join 策略(以为大表用 Shuffle,实际小就改 Broadcast)
  3. 动态优化倾斜 Join(检测到大 partition 自动拆)
- Spark 3.0+ 默认开,学习时手动关对比才看得到效果
- 暗坑:EXPLAIN 显示 `isFinalPlan=false`,是运行前视图,真实计划要看 Spark UI

#### **CBO(基于代价优化)**

**有了 CBO**：CBO 掐指一算，发现 A 和 C 都是极小的表。它会自动把顺序改成 `(A Join C) Join B`。A 和 C 先关联，出来的中间结果可能只有 50 行，再拿这 50 行去跟 B 表做关联，轻松搞定！

##### 为什么非要执行 `ANALYZE TABLE`

Spark 默认是不自动收集统计信息的。**因为 CBO 是一个“瞎眼精算师”，没有数据，它就无法算账。**

如果不运行 `ANALYZE TABLE...`，Spark 只知道文件的物理大小（比如 1GB），但它不知道：

- 这 1GB 里到底是 100 行（每行数据很大），还是 100 万行（每行数据很小）？
- 这个字段的倾斜度怎么样？有多少是 NULL 值？

> ⚠️ **大厂避坑指南**：为什么 Spark 不默认开启自动收集？ 因为**收集统计信息本身是非常昂贵的**！它需要把整张表全量扫描一遍，去数行数、算直方图。如果你的表每天都在高频更新，天天自动去算这个，集群算力就被这玩意儿吃光了。所以，Spark 把这个权力交给了开发者，由你在离线调度批处理完、或者数据导入后，**手动触发**一次。

##### CBO VS AQE：终极区别

- **CBO 是“出发前的路线规划”**：你出门前，看了一眼昨天的天气预报和历史堵车数据（**预收集的统计信息**），规划了一条路线。
- **AQE 是“开到半路上的高德地图实时导航”**：你开到高速上，前面突然发生车祸堵死了（**运行时产生了动态数据倾斜**），高德地图立刻弹窗：“检测到前方严重拥堵，已为您切换新路线”（**动态调整执行计划**）。

##### 强强联合

| **场景**                | **只有 CBO**                 | **只有 AQE**                     | **CBO + AQE 同时开启（完美状态）**                        |
| ----------------------- | ---------------------------- | -------------------------------- | --------------------------------------------------------- |
| **多表关联顺序**        | 极好（根据统计信息自动排好） | 较差（只能按你写的顺序死板执行） | **极好**（初始顺序就是对的，底子打得好）                  |
| **突发数据倾斜**        | 无法处理，直接 OOM 挂掉      | 可以动态把倾斜的 Key 拆分处理    | **完美**（不仅关联顺序最省资源，遇到倾斜还能动态化解）    |
| **小分区横行 (小文件)** | 无法处理，产生大量碎任务     | 动态把小分区合并成大分区         | **完美**（减少了 Task 数量，极大节省了集群 CPU 调度资源） |





### 数据倾斜 ★面试高频

**是什么**: 某 key 占大部分数据(90%), Join 时全挤到一个 task, 这个 task 累死其他闲死(木桶效应)
**诊断**: Spark UI 看 task duration 的 Max/Median 比【>5x 倾斜】【>50x 严重】
**三种解法**:

| **方案**           | **谁来干**            | **适用场景**                                              | **缺点/局限性**                                              |
| ------------------ | --------------------- | --------------------------------------------------------- | ------------------------------------------------------------ |
| **AQE 自动优化**   | **Spark 引擎** (首选) | 90% 的普通 Join 倾斜场景                                  | 1. 只能优化 **Join**，如果是 `Group By` 导致的倾斜无能为力。 2. 必须等 Shuffle 结束才能介入。 |
| **加盐 (Salting)** | **程序员** (手动)     | 1. `Group By` 严重倾斜 2. AQE 没兜住的极端大表 Join       | 代码变得很臃肿，增加了维护成本，小表会膨胀。                 |
| **UNION 处理**     | **程序员** (手动)     | 明确知道就是某个固定热点（如 `NULL` 或 `ID:0`）导致的倾斜 | 大表会被扫描两次，若没有缓存，性能会打折扣。                 |

**Q: 真实生产里你怎么处理"未知倾斜"?**
A: 三步走——
(1) 先开 AQE Skew Join,大部分场景自动解决;
(2) 仍有问题就跑 `df.groupBy("key").count().orderBy(desc("count")).show()` 找出热 key;
(3) 对热 key 单独 union 处理或者加盐。
**核心原则:先让 AQE 兜底,不行再人工干预**。





### OLAP / 索引

**ClickHouse 为什么快**

- ① 无 JVM 启动(常驻 C++ 进程)② 向量化执行(SIMD 批处理)③ MergeTree 数据按 ORDER BY 物理排序,数据局部性好
- 稀疏索引:每 8192 行一个标记(不是每行),空间极小
- 适合聚合,不适合点查

**PostgreSQL 4 类索引**
| 类型 | 写法 | 用 |
|------|------|----|
| B-Tree | `(col)` | 等值/范围默认 |
| 复合 | `(a,b,c)` | 多列 WHERE,最左前缀 |
| 覆盖 | `(a) INCLUDE (b,c)` | 消除回表(Index Only Scan) |
| 部分 | `(a) WHERE status='x'` | 只索引子集,省空间 |
- **回表**:索引只有索引列,SELECT 其他列要回主表读(额外随机 IO)
- **覆盖索引**:INCLUDE 把 SELECT 列塞进索引,`Heap Fetches: 0` 不回表
- EXPLAIN 看:Seq Scan(全表) / Index Scan / **Index Only Scan**(最优)

**OLAP vs OLTP(何时用哪个)**
| | OLTP(PG) | OLAP(CH/Spark) |
|--|----------|----------------|
| 查询 | 点查/小范围 | 全表聚合 |
| 并发 | 高(几千连接) | 低 |
| 延迟 | ms | 秒 |
| 用 | 司机端 App、订单状态 | 运营看板、数据分析 |

### 查询优化

**窗口函数 vs 自连接**
- 算"日环比"用 `LAG(rev) OVER (PARTITION BY zone ORDER BY day)`
- 比自连接快:单次扫描排序 vs Join 两次 Shuffle
- 其他:`RANK/ROW_NUMBER/LEAD/移动平均`

**HyperLogLog 近似计算**
- `APPROX_COUNT_DISTINCT()` 替代 `COUNT(DISTINCT)`
- 固定空间(几 KB)估算 distinct,默认误差 5%(实测 2.33%)
- 适合 UV / 独立设备数,误差可接受时优先用

### 工程化

**幂等性(Airflow 灵魂)**
- 定义:任务跑 1 次和 100 次结果一样(重跑不翻倍)
- 怎么做:全链路 `INSERT OVERWRITE`(覆盖)而非 `INSERT INTO`(追加)
- 实测:重跑 DAG 两次,行数/订单数完全一致

**Lambda 架构**(实时扩展会用)
- 批处理层(准但慢)+ 速度层(快但近似)+ 服务层(合并)
- 对比 Kappa:只有流(Netflix 推崇)

---

## 4. 20 道高频面试题(背诵版答案要点)

1. **Parquet 为什么比 CSV 快?** 列式存储→列裁剪(只读需要的列)+ 谓词下推(row group min/max 跳过)
2. **Snappy/Zstd 怎么选?** Snappy 快 CPU 省(历史默认),Zstd 压缩比高(IO 瓶颈场景反而更快),冷数据用 Zstd
3. **HDFS 小文件问题?** NameNode 内存压力 + Task 膨胀;coalesce 合并 / HAR / 源头治理
4. **分区 vs 分桶?** 分区裁剪跳目录,分桶消除 Join Shuffle
5. **谓词下推原理 + UDF 为什么破坏它?** Catalyst 推 Filter 到 Scan;UDF 是黑盒优化器看不进,PushedFilters 变空
6. **Broadcast vs Shuffle Join?** 小表广播大表不动 vs 两表按 key 重分区;<10MB 自动广播
7. **AQE 解决什么?** 编译期统计不准,运行时根据真实数据动态调整:分区合并/Join 切换/倾斜处理
8. **数据倾斜怎么发现怎么处理?** Spark UI 看 Max/Median;加盐法 / AQE Skew Join
9. **CBO vs RBO?** RBO 规则(启发式),CBO 代价(需 ANALYZE 统计);CBO 编译期,AQE 运行时
10. **四层数仓职责?** ODS 原样 / DWD 清洗+维度 / DWS 预聚合 / ADS 面向应用;为复用、血缘、稳定
11. **拉链表/SCD2?**(项目没做,补)记录维度历史变化,加 start_date/end_date
12. **维度建模 vs 范式?** 数仓选维度建模(星型),查询性能优先,反范式宽表
13. **数据质量怎么做?** DWD 清洗规则 + 数据剖析量化(空值率/异常值)+ 监控
14. **ClickHouse 为什么快/MergeTree?** 无 JVM + 向量化 + 稀疏索引 + 按 ORDER BY 物理排序
15. **跳数索引 vs B-Tree?** 跳数定位到块(跳过不需扫的块),B-Tree 定位到行
16. **PG 覆盖/复合/部分索引?** 覆盖消除回表,复合最左前缀,部分只索引子集
17. **OLAP vs OLTP?** 读写模式/存储结构不同;点查用 PG,聚合用 CH
18. **Lambda 架构?** 批层+速度层+服务层;Kappa 只有流
19. **Airflow 幂等性?** OVERWRITE 而非 INTO;重跑不翻倍
20. **数据血缘?** 四层分层 + Hive Metastore 元数据

---

## 5. 业务洞察(讲故事用,体现业务感)

| 洞察 | 数据 | 业务建议 |
|------|------|---------|
| 早晚高峰不对称 | 早高峰订单仅晚高峰 53% | 晚高峰重点排班 |
| 机场金矿 | 机场行程 9.5%,客单价 $77 vs 市区 $22(3.5x) | 机场专车产品 |
| 黑色周日 | 2024-03-31 Manhattan 营收-25~35%,仅 Penn Station +30.9% | 异常归因(天气/活动) |
| 打车密度 | Manhattan 54% vs Queens 93% | Manhattan 保供给,Queens 引流 |
| 总营收 | 2024 Q1 $2.53 亿 | CEO 头号数字 |

---

## 6. 13 个工程坑(现象→根因→修复)

| # | 坑 | 现象 | 根因 | 修复 |
|---|-----|------|------|------|
| 1 | bitnami 镜像下架 | pull not found | 仓库迁移 | 用 bitnamilegacy/spark |
| 2 | ARM 跑 amd64 | platform mismatch | M 芯片 + amd64 镜像 | `platform: linux/amd64` |
| 3 | NameNode 没格式化 | InconsistentFSState | 镜像不自动 format | bash 脚本检测 current/ |
| 4 | PG15 认证冲突 | auth type 10 not supported | SCRAM vs 旧 JDBC | 强制 md5 |
| 5 | 镜像无 curl | healthcheck 误报 | 精简镜像 | 改 ps 探测进程 |
| 6 | Hive 入口硬编码 | relation already exists | SKIP_SCHEMA_INIT 被写死 | override entrypoint |
| 7 | 维表 CSV 解析失效 | header/quote 没处理 | Hive ROW FORMAT 弱 | Spark 读→转 Parquet |
| 8 | 上游 schema 变更 | INT64 vs DOUBLE | TLC 改字段类型 | 按真实 schema 建表 |
| 9 | JDBC 驱动缺失 | No suitable driver | Jupyter 镜像没 jar | cp 到 Jupyter |
| 10 | file:// 路径不一致 | Mkdirs failed | driver/executor 路径不同 | 写 HDFS |
| 11 | CH user_files 沙箱 | DATABASE_ACCESS_DENIED | file() 安全限制 | HTTP POST Parquet |
| 12 | Superset 连 CH | dialect 不识别 | 没装驱动 | pip clickhouse-connect |
| 13 | Superset 时间列 | 图表空白 | 没标 temporal | 标记 + 改时间筛选 |

---

## 7. 反直觉洞察专区(面试加分炸弹)

**① partial aggregate 让 GroupBy 倾斜自愈**
- 现象:GROUP BY + SUM/COUNT 跑倾斜数据,shuffle 只有 0.02MB,不倾斜
- 原理:Spark map 端预聚合,287MB 数据 shuffle 前压成几 KB
- 结论:**可结合聚合(SUM/COUNT)天然抗倾斜,真正倾斜的是 Join + 不可结合聚合(collect_list/percentile)**
- 怎么讲:"我本以为 COUNT(DISTINCT) 会倾斜,实测 Max/Median 才 5.2x,后来发现是 map 端预聚合/预去重,改用 Join 才复现 122x 真倾斜"

**② Zstd 反超 Snappy**
- 教科书:Zstd 用 CPU 换空间(写更慢)
- 实测:Zstd 写/读/空间全赢
- 原理:Spark+HDFS 是 IO 瓶颈,文件小省的 IO > 多花的 CPU
- 行业:Facebook/Uber/字节都从 Snappy 迁 Zstd

**③ 低选择性列单列索引失效**
- 现象:给 Manhattan(占 54%)建单列索引,EXPLAIN 还是 Seq Scan
- 原理:走索引要回表 12 万次随机 IO,比顺序全表扫还慢,优化器主动放弃
- 结论:低选择性列单列索引无效,要复合索引 + 覆盖

**④ AQE Skew Join 完爆手写加盐**
- AQE 0.75s 打败朴素 2.17s 和加盐 1.91s,且 SQL 零修改
- 原理:运行时动态拆分比静态启发式聪明

---

## 8. 一句话杀手锏(面试官问"最难/最有成就感")

> "最有意思的是数据倾斜实验——我本来想用 GROUP BY 复现倾斜,但 Spark UI 显示根本不倾斜。折腾半天才发现是 Spark 的 partial aggregate 在 map 端把数据预聚合了,287MB 压成 0.02MB。这让我真正理解了**不是所有操作都会倾斜,要看是否触发大量 shuffle**——真正的倾斜重灾区是 Join 和 collect_list 这种不可结合的聚合。最后我改用 Join 场景才复现出 Max/Median 122 倍的真倾斜,再用加盐法降到 23 倍,AQE Skew Join 更是零代码修改做到 2.89 倍加速。这个'假设被证伪再修正'的过程,比直接成功学到的更多。"
