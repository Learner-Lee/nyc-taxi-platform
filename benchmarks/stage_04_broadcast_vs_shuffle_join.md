# 实验 #5: Broadcast Join vs Shuffle Join

> 执行日期:2026-05-17 | 数据:2024-01 yellow_trips(2.96M 行)× taxi_zone_lookup(265 行)

## 实测结果
| 维度 | Shuffle Join (SortMergeJoin) | Broadcast Join | 加速 |
|------|------------------------------|----------------|------|
| 耗时 | 0.87s | **0.23s** | **3.75x** |
| 主表 Exchange | hashpartitioning(200 分区) | 无 | — |
| 维表 Exchange | hashpartitioning(200 分区) | BroadcastExchange | — |
| Sort 操作 | 两侧都 Sort | 无 | — |

## EXPLAIN 物理证据
- A(Shuffle): `SortMergeJoin [PULocationID], [LocationID], LeftOuter` + 两次 `Exchange hashpartitioning`
- B(Broadcast): `BroadcastHashJoin ... BuildRight` + 主表零 Exchange

## 决策原则
- 维表 < 10MB(默认 `spark.sql.autoBroadcastJoinThreshold`)→ 让 Spark 自动判断
- 维表稍大但仍能放进 Driver/Executor 内存 → 显式 `/*+ BROADCAST(z) */`
- 维表 > 100MB 或类型不匹配 → 老老实实 Shuffle Join
