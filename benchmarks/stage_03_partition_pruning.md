# 实验 #3:分区裁剪威力

> 执行日期:2026-05-17 | 数据:NYC Yellow Taxi 2024 Q1,9,554,778 行(3 个分区)

## 实测结果(冷启动第一次)
| 查询 | SQL | 耗时 | 加速 |
|------|-----|------|------|
| A 全表扫描 | `SELECT COUNT(*), SUM(total_amount) FROM ods.yellow_trips` | 0.72s | 基准 |
| B 命中分区键 | `... WHERE year=2024 AND month=1` | 0.26s | **2.77x** |
| C CAST 包裹 | `... WHERE year=2024 AND CAST(month AS STRING)='1'` | 0.18s | 4.08x |

## EXPLAIN 物理证据
Location: InMemoryFileIndex [hdfs://.../year=2024/month=01]  ← 只读 month=01 目录
PartitionFilters: [(year=2024), (month=1)]                   ← 过滤下推到读文件前
ReadSchema: struct<total_amount:double>                       ← 19 列只读 1 列(列裁剪)

## 关键发现
- B 比 A 快 2.77x,符合"只扫 1/3 数据"的理论预期
- Spark 3.4+ Catalyst 优化器能识别 CAST 仍然下推,但更复杂表达式会破坏推断
- 二次跑数据全部进 Spark cache 后,IO 不再是瓶颈,分区裁剪相对收益缩小到 1.6x——**优化的边际收益取决于当前瓶颈在哪**
