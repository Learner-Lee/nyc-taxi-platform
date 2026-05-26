# NYC 出租车智能运营平台

企业级大数据全栈项目 | NYC Yellow Taxi 2022-2024 | 约 3-5 亿条记录

## 快速导航

- [项目总览](docs/PROJECT_OVERVIEW.md) — 架构图、业务背景、简历/面试价值
- [资源规划](docs/RESOURCE_PLAN.md) — 内存预算、分组启停、OOM 应急
- [学习路径](docs/LEARNING_PATH.md) — 阶段依赖、核心概念、延伸阅读
- [STAGE 01](docs/STAGE_01_环境搭建.md) — 环境搭建与 HDFS 入门（含 6 个踩坑实录）
- [STAGE 02](docs/STAGE_02_数据接入与ODS层.md) — 数据接入与 ODS 层（含 2 个踩坑实录 + CSV vs Parquet 对比实验）
- [STAGE 03](docs/STAGE_03_存储层优化.md) — 存储层优化（Zstd / 分区裁剪 / 小文件治理 三项量化实验）
- [STAGE 04](docs/STAGE_04_DWD层与数据清洗.md) — DWD 层与数据清洗（96.57% 可信率 + Broadcast Join 3.75x 加速 + 业务洞察）
- [STAGE 05](docs/STAGE_05_计算层深度优化.md) — 计算层深度优化（谓词下推 / AQE 3.82x / CBO 1.71x）
- [STAGE 06](docs/STAGE_06_数据倾斜实战.md) — 数据倾斜实战（朴素 122x → 加盐 23x → AQE Skew Join 2.89x 加速）
- [STAGE 07](docs/STAGE_07_DWS层与查询优化.md) — DWS 层与查询优化（行数缩减 452x / 窗口函数 1.70x / HyperLogLog 3.20x）
- [STAGE 08](docs/STAGE_08_ADS层与PG索引.md) — ADS 层 + PostgreSQL 索引（5 阶段对比 254x 加速 / IO 680x↓）
- [STAGE 09](docs/STAGE_09_ClickHouse冷热分层.md) — 冷热分层 ClickHouse（CH vs Spark 聚合 9.6x 加速）
- [STAGE 10](docs/STAGE_10_Airflow调度编排.md) — Airflow 调度编排（ETL DAG + 幂等性验证）
- [STAGE 11](docs/STAGE_11_Superset看板.md) — Superset 双看板（运营 CH + 财务 PG + 过滤器联动）

## 启动

```bash
# 启动核心服务（常驻）
docker compose -f docker/docker-compose.core.yml up -d

# 启动服务层（按需，STAGE 09+）
docker compose -f docker/docker-compose.serving.yml up -d
```

## Web UI 入口

| 服务 | URL | 用途 |
|------|-----|------|
| HDFS NameNode | http://localhost:9870 | 文件浏览、DataNode 健康 |
| Spark Master | http://localhost:8080 | Worker 状态、Job 监控 |
| Jupyter | http://localhost:8888 | 数据探索 Notebook |
| PostgreSQL | localhost:5432 | ADS + Hive Metastore 后端 |

## 当前进度

- [x] **STAGE 00**: 文档体系搭建（PROJECT_OVERVIEW / RESOURCE_PLAN / LEARNING_PATH）
- [x] **STAGE 01**: 环境搭建与 HDFS 入门 — 10 容器健康运行，踩 6 坑后稳定
- [x] **STAGE 02**: 数据接入与 ODS 层 — 955 万行入库，踩 2 坑（CSV 解析、上游 schema 变更），CSV vs Parquet 实验完成（**12.2x 加速 / 5.3x 压缩**）
- [x] **STAGE 03**: 存储层优化 — 三项实验完成:**Zstd 全面胜出**(24% 空间节省 + 42% 读取加速)、**分区裁剪 2.77x 加速**、**小文件治理 20.3x 加速 + 15.6% 存储节省**
- [x] **STAGE 04**: DWD 层与数据清洗 — 9.55M→9.23M (**96.57% 可信率**) + 11 业务衍生字段 + **Broadcast Join 3.75x 加速** + 业务洞察(早高峰仅晚高峰 53% / 机场客单价 3.5x)
- [x] **STAGE 05**: 计算层深度优化 — 三项实验完成:**谓词下推 UDF 拖慢 2.63x**(PushedFilters 失效 + BatchEvalPython)、**AQE 加速 3.82x**(200→动态合并)、**CBO Join Reorder 1.71x**(ANALYZE 统计)
- [x] **STAGE 06**: 数据倾斜实战 — Join 三方案对比:**朴素 Max/Median 122.1x → 加盐法 23x(6.2x↓)→ AQE Skew Join 0.75s(2.89x 加速,零 SQL 修改)** + 反直觉洞察(partial aggregate 让 GroupBy 倾斜自愈)
- [x] **STAGE 07**: DWS 层与查询优化 — DWS 物化(**行数缩减 452x / 体积压缩 85x**)+ 窗口函数 **1.70x** + HyperLogLog **3.20x(误差 2.33%)** + 业务洞察(黑色周日 / 打车密度指标)
- [x] **STAGE 08**: ADS 层 + PostgreSQL 索引 — ADS 入 PG + 索引 5 阶段对比:**Seq Scan 6.10ms → 部分索引 0.024ms(254x 加速,IO 680x↓)** + 低选择性列单列索引被优化器放弃的洞察
- [x] **STAGE 09**: 冷热分层 ClickHouse — 920 万行入 MergeTree + **CH vs Spark 聚合 9.6x 加速**(首查就快 3.4x,无 JVM 开销)+ 踩通 3 种导入方式
- [x] **STAGE 10**: Airflow 调度编排 — 3-task ETL DAG + 失败重试 + **幂等性验证(重跑 2 次数据零翻倍)** + 复用 PG 元数据库
- [x] **STAGE 11**: Superset 看板 — 运营(ClickHouse)+ 财务(PostgreSQL)双看板,7 图表 + 过滤器联动,三大洞察可视化(机场金矿/早高峰弱/黑色周日)
- [ ] STAGE 12: Streamlit 司机应用
- [ ] STAGE 07: DWS 层与查询优化
- [ ] STAGE 08: ADS 层 + PostgreSQL 索引
- [ ] STAGE 09: 冷热分层：ClickHouse 接入
- [ ] STAGE 10: Airflow 调度编排
- [ ] STAGE 11: Superset 看板
- [ ] STAGE 12: Streamlit 司机应用
- [ ] STAGE 13: 项目复盘与简历输出

## 当前集群资源（STAGE 01 验证结果）

- **Spark 集群**：1 Master + 3 Worker，总计 6 cores / 12 GB
- **HDFS 集群**：1 NameNode + 2 DataNode，副本数 2
- **元数据**：Hive Metastore + PostgreSQL（md5 认证）
- **内存占用**：约 15 GB（core 组）
