# 实验 #6: 谓词下推 + UDF 失效

## 实测(2026-05-17, dwd.fact_trips 9.23M 行)
| 维度 | A 纯 SQL | B UDF |
|------|---------|-------|
| 耗时 | 1.07s | 2.82s |
| 拖慢比 | (基准) | **2.63x** |
| PushedFilters | `[IsNotNull, GreaterThan(5.0)]` | **空 []** |
| BatchEvalPython 节点 | 无 | 有(跨进程序列化) |
| 结果一致 | ✅ | ✅ cnt=1475304, rev=$97M |

## 原理
1. UDF 对 Catalyst 是黑盒,无法转换为 Parquet 可识别的过滤
2. 跨进程序列化(JVM ↔ Python)是 UDF 性能的根本瓶颈
3. UDF 输出列被 Catalyst 视为"未知",后续优化器都不敢动它

## 铁律
**能用纯 SQL 表达的绝不用 UDF**。改写优先级:SQL 内置函数 > 表达式 > Pandas UDF + Arrow > 最后才考虑 Python UDF
