# 实验 #7: AQE 开关对比

## 实测(GROUP BY pickup_borough 仅 8 组)
| 维度 | AQE OFF | AQE ON |
|------|---------|--------|
| 耗时 | 2.88s | 0.75s |
| 加速比 | (基准) | **3.82x** |
| Shuffle Task | 固定 200(192 空跑) | 动态合并 |
| 顶层节点 | 普通物理计划 | `AdaptiveSparkPlan` |
| isFinalPlan | — | false(EXPLAIN 是运行前视图)|

## AQE 三大能力
1. 动态合并 Shuffle 分区(本实验主要受益)
2. 动态切换 Join 策略(SortMerge→Broadcast)
3. 动态优化倾斜 Join(STAGE 06 重点)

## 暗坑
EXPLAIN 显示的 200 是 AQE 起点,实际运行后会变。要看真实执行计划,必须:
- 查询运行后再 explain
- 或者打开 Spark UI 看 AQE plan changes
