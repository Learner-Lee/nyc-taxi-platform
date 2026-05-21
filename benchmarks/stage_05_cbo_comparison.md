# 实验 #8: CBO + ANALYZE 统计

## 实测(三表 Join + GroupBy)
| 维度 | CBO OFF | CBO ON |
|------|---------|--------|
| 耗时 | 0.64s | 0.37s |
| 加速比 | (基准) | **1.71x** |

## ANALYZE 收集到的统计(trip_distance)
- min=0.01 max=194.65(STAGE 04 清洗规则的物理证据)
- num_nulls=0
- distinct_count=5675
- avg_col_len/max_col_len 都有

## 工程实践
- 默认 Spark 不自动收集统计 → 大部分团队丢了 CBO 红利
- 每天凌晨 ANALYZE 关键大表是生产标配
- HyperLogLog 近似算法有 ~2% 误差(本项目 pickup_borough 实际 8,ANALYZE 估 7)
