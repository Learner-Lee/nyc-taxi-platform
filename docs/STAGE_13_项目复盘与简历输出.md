# STAGE 13: 项目复盘与简历输出

## 🎯 本阶段目标
把 12 个阶段的成果**整理成一条能讲的故事线 + 一份能投的简历 + 一个能展示的 GitHub README**,完成"做完项目"到"拿项目找工作"的最后一跃。

---

## 一、一句话讲清楚这个项目

> 基于 Docker Compose 独立搭建了覆盖 **存储→计算→数仓→OLAP→调度→BI→应用** 全链路的企业级大数据平台,处理 NYC 出租车 955 万行数据,完成 ODS→DWD→DWS→ADS 四层数仓,做了 **8 大类共 19 项量化优化**(最高 254x 加速),为运营/财务/司机/数据/调度五类用户交付了看板和应用。

---

## 二、完整故事线(13 阶段串成一条线)

```
【打地基】
S01 环境搭建 → 10 容器分布式集群(HDFS+Spark+Hive+PG+Jupyter)

【数据进来】
S02 ODS 接入 → 955 万行入库,CSV vs Parquet 验证列式存储(12.2x)

【把存储榨干】
S03 存储优化 → Zstd 压缩 / 分区裁剪 / 小文件治理(三项实验)

【把数据洗干净】
S04 DWD 清洗 → 8 条规则 96.57% 可信率 + Broadcast Join + 业务衍生字段

【把计算调到极致】
S05 计算优化 → 谓词下推 / AQE / CBO(理解 Spark 内核)
S06 数据倾斜 → 加盐法 + AQE Skew Join(最硬核一章)

【把查询做快】
S07 DWS 预聚合 → 窗口函数 / HyperLogLog(452x 行数缩减)

【对外提供服务】
S08 ADS + PG 索引 → 司机端点查 254x 加速(覆盖索引)
S09 ClickHouse → 运营聚合 9.6x(冷热分层)

【自动化 + 可视化 + 产品化】
S10 Airflow → ETL DAG + 幂等性
S11 Superset → 运营/财务双看板
S12 Streamlit → 司机端推荐应用

【收官】
S13 复盘 → 简历 + README + 面试准备
```

**讲述要点**:每个阶段都是"先看到痛点 → 引入技术 → 量化验证"。面试时挑 2-3 个你最有感觉的(如 S06 倾斜、S09 ClickHouse)深入讲。

---

## 三、能力 Checklist(对照 LEARNING_PATH 自测)

### 存储与格式 ★★★
- [x] 列式 vs 行式存储的根本区别(实测 12.2x)
- [x] 按场景选压缩算法(实测 Zstd 反超 Snappy 的 IO 瓶颈洞察)
- [x] 分区键设计 + 分区裁剪(EXPLAIN 物理证据)
- [x] HDFS 小文件治理(20.3x + 存储节省)

### Spark 计算 ★★★
- [x] 读懂 Physical Plan(PushedFilters / SortMergeJoin / BroadcastHashJoin)
- [x] Spark UI 定位瓶颈(task duration Max/Median)
- [x] 数据倾斜诊断 + 解决(加盐 + AQE Skew Join)
- [x] AQE 三大能力 + CBO 统计

### 数仓设计 ★★★
- [x] ODS/DWD/DWS/ADS 四层职责
- [x] 维度建模 + Broadcast Join
- [x] 清洗规则 + 数据质量量化
- [x] 幂等性(全链路 OVERWRITE)

### OLAP 与索引 ★★★
- [x] ClickHouse MergeTree 存储结构(稀疏索引)
- [x] PostgreSQL EXPLAIN ANALYZE 解读
- [x] 4 类索引选型(B-Tree/复合/覆盖/部分)
- [x] OLAP vs OLTP 何时用哪个

### 工程化 ★★
- [x] Docker Compose 多容器部署 + 分组启停
- [x] Airflow 幂等 DAG
- [x] Streamlit 数据应用
- [x] Superset 多源看板
- [x] 13 个真实工程坑的诊断与修复

---

## 四、20 道面试题(答案要点散在各 STAGE,这里是索引)

| # | 题 | 答案在 |
|---|----|----|
| 1 | Parquet vs CSV 为什么快 | S02 |
| 2 | Snappy/Zstd/Gzip 怎么选 | S03 |
| 3 | HDFS 小文件问题 | S03 |
| 4 | 分区 vs 分桶 | S03 |
| 5 | 谓词下推原理 + UDF 为什么破坏它 | S05 |
| 6 | Broadcast vs Shuffle Join | S04 |
| 7 | AQE 解决什么问题 | S05 |
| 8 | 数据倾斜怎么发现怎么处理 | S06 |
| 9 | CBO vs RBO | S05 |
| 10 | ODS/DWD/DWS/ADS 职责 | S04/S07 |
| 11 | 拉链表/SCD2 | (扩展) |
| 12 | 维度建模 vs 范式 | S04 |
| 13 | 数据质量怎么做 | S04 |
| 14 | ClickHouse 为什么快/MergeTree | S09 |
| 15 | 跳数索引 vs B-Tree | S09 |
| 16 | PG 覆盖/复合/部分索引 | S08 |
| 17 | OLAP vs OLTP | S08/S09 |
| 18 | Lambda 架构/批流一体 | (扩展) |
| 19 | Airflow DAG/幂等性 | S10 |
| 20 | 数据血缘 | S04(分层) |

> 11、18 这两题项目没直接覆盖,面试前补一下理论(拉链表 SCD2、Lambda/Kappa 架构)。

---

## 五、项目可以继续扩展的方向(面试被问"还能做什么")

1. **数据量扩展**:2024 Q1 → 2022-2024 全量(3-5 亿行),验证架构横向扩展
2. **实时链路**:加 Kafka + Spark Streaming,做 Lambda 架构(批 + 流)
3. **数据质量平台化**:Great Expectations 自动化数据校验
4. **数据湖**:Parquet → Iceberg/Hudi,支持 ACID + 时间旅行
5. **拉链表**:对维表做 SCD2 缓慢变化维
6. **CI/CD**:DAG 代码 + dbt 模型纳入 git + 自动测试

---

## 六、收官 checklist

- [x] 13 个 STAGE 文档完整
- [x] benchmarks/ 量化指标总账(`_summary.md`)
- [x] 看板截图(`superset/dashboards/`)
- [x] 司机应用截图(`streamlit/screenshots/`)
- [x] PROJECT_OVERVIEW 简历终稿(实测数据版)
- [x] README 终稿(GitHub 门面)
- [ ] git 全量提交 + push 到 GitHub(你来做)

---

## 七、最后的话

这个项目的价值不在"用了多少组件",而在:
1. **每个技术都对应业务痛点**(没有为炫技而炫技)
2. **每个优化都有量化数据**(经得起面试追问)
3. **13 个工程坑**(区分"跑过教程"和"真正搭过")
4. **完整故事线**(从数据进来到给司机用,闭环)

**面试时的杀手锏**:当面试官问"你这个项目最难的地方",讲 **S06 数据倾斜**(partial aggregate 自愈的反直觉发现)或 **S09 ClickHouse 导入踩的 3 个坑**(file:// 分布式陷阱 / user_files 沙箱)——这些是真正做过的人才讲得出的细节。
