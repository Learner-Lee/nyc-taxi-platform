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
- [ ] STAGE 05: 计算层优化
- [ ] STAGE 06: 数据倾斜实战
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
