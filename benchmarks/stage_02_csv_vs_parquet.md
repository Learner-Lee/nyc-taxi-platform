# 实验 #1:CSV vs Parquet 量化对比

> 执行日期:2026-05-17 | 数据:NYC Yellow Taxi 2024-01,2,964,624 行

## 实验环境
- Spark 3.4.1 standalone,3 Worker × 2 cores × 4GB
- HDFS 3.3.6,2 DataNode,副本数 2
- 同源数据分别落地 CSV 和 Parquet,用 Spark SQL 跑相同查询

## 量化结果

| 维度 | CSV | Parquet | 加速/压缩比 |
|------|-----|---------|------------|
| 文件总大小 | 308.06 MB | 58.22 MB | **5.29x 压缩,节省 81.1% 空间** |
| 写入耗时 | 3.0s | 1.5s | 2.0x |
| `SELECT COUNT(*)` | 0.59s | 0.16s | **3.7x** |
| `WHERE trip_distance>5 + SUM(total_amount)` | 3.00s | 0.25s | **12.2x** |

## 原理解释

### 为什么 Parquet 体积小 5.3 倍?
1. **列式存储**:相同列的值聚集存储,数据局部性高,通用压缩算法(Snappy)效果更好
2. **类型感知**:Parquet 知道字段类型,数字字段用 dictionary encoding / RLE 编码,空间利用率远高于 CSV 文本
3. **二进制 vs 文本**:Parquet 二进制存储,无需 CSV 的字符分隔符和引号转义

### 为什么 COUNT(*) 加速 3.7 倍?
Parquet **文件 footer 直接记录每个 row group 的行数**,COUNT(*) 无需扫描数据,只读 metadata。CSV 必须逐行扫描计数。

### 为什么 WHERE+SUM 加速 12.2 倍?
两个优化叠加:
1. **列裁剪 (Column Pruning)**:只读 `trip_distance` 和 `total_amount` 两列,跳过其他 17 列。CSV 必须读整行才能拿到这两个字段
2. **谓词下推 (Predicate Pushdown)**:Parquet 每个 row group 的 footer 记录 `trip_distance` 的 min/max,Spark 在读文件前就跳过不满足条件的 row group

### 浮点精度差异(29946845.159999676 vs 29946845.16000478)
Parquet vectorized reader 用 SIMD 批量累加,浮点加法顺序不可交换,导致 1e-6 级别误差。业务可忽略,但反映并行计算的非确定性。

## 简历可写的成果   

> 基于 296 万行 NYC Yellow Taxi 数据,完成 CSV vs Parquet 量化对比实验:
> - 文件体积压缩 **5.3 倍**(308MB → 58MB,节省 81% 存储成本)
> - 复杂查询(WHERE 过滤 + 聚合)耗时加速 **12.2 倍**(3.0s → 0.25s)
> - 验证列裁剪与谓词下推的实际效果,为后续存储优化建立量化基线

## 后续验证(STAGE 03 实验 #2 伏笔)
当前 Parquet 默认 Snappy 压缩。Zstd 通常能再压 1.5-2x,但写入慢。STAGE 03 会做 Snappy vs Zstd vs 无压缩三方对比。
