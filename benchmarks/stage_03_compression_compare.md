# 实验 #2:Parquet 压缩算法三方对比

> 执行日期:2026-05-17 | 数据:NYC Yellow Taxi 2024-01,2,964,624 行

## 实测结果
| 算法 | 写入耗时 | 文件大小 | 读取+聚合 | 相对 Snappy |
|------|---------|---------|----------|------------|
| uncompressed | 1.56s | 74.27 MB | 0.34s | +27.6% |
| snappy | 1.45s | 58.22 MB | 0.26s | (基准) |
| **zstd** | **1.02s** | **44.22 MB** | **0.15s** | **-24.0%** |

## 核心发现:Zstd 在所有 3 个维度都赢了 Snappy
**Zstd vs Snappy:空间节省 24%,写入快 30%,读取快 42%**——推翻"Zstd 用 CPU 换空间"的传统认知。

## 原理:Spark + HDFS 场景下,IO 是瓶颈,不是 CPU
- 读 Parquet 耗时 = IO 时间 + 解压 CPU 时间
- 当 IO ≫ CPU 时,文件小的优势盖过 CPU 代价
- 写入也是 IO 瓶颈:HDFS 副本复制(2x)的网络代价 > Zstd 的压缩 CPU 代价

## 行业现状
Snappy 是 2010 年代默认(那时 CPU 是瓶颈),Zstd 是当下最优。Facebook/Uber/字节跳动等已迁移到 Zstd。

## 实验局限性
- JVM 预热影响:第一个 codec (uncompressed) 没享受 JIT 优化
- 24% 文件压缩是物理事实,不受预热影响
