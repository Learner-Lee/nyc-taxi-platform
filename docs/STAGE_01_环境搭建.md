# STAGE 01: 环境搭建与 HDFS 入门

## 🎯 本阶段目标

- **业务问题**：没有存储层，后续一切都是空谈。这一步是整个平台的地基。
- **技术能力**：掌握 Docker Compose 多容器编排、HDFS 基本操作、Spark 集群验证
- **产出物**：
  - `docker/docker-compose.core.yml` — core 组配置文件
  - `docker/.env` — 统一环境变量
  - `docker/hadoop/` — HDFS 配置
  - `docker/spark/` — Spark 配置
  - HDFS Web UI 可访问（localhost:9870）
  - Spark Master Web UI 可访问（localhost:8080）
  - Jupyter 可访问（localhost:8888）

---

## 📋 前置检查清单

- [ ] Docker 已安装（`docker --version` 有输出）
- [ ] Docker Compose 已安装（`docker compose version` 有输出）
- [ ] 磁盘剩余 > 20GB（本阶段容器镜像约需 8-10GB）
- [ ] 端口未被占用：9870、9000、8080、7077、18080、8888、5432
- [ ] 需要启动的容器组：**core 组**
- [ ] 当前内存占用应低于 8GB（留足 15GB 给容器）

---

## 🚀 本阶段启动服务

```bash
# 本阶段只启动 core 组，serving 组不需要
docker compose -f docker/docker-compose.core.yml up -d

# 验证启动状态
docker compose -f docker/docker-compose.core.yml ps
```

预计启动后内存占用：**~14-15GB**

---

## 📚 核心概念（3 分钟读完）

### 概念 1：HDFS 为什么存在？
**业务类比**：你有 100TB 的出租车数据，单台机器最多 8TB。HDFS 就像一个**跨越多台服务器的超大硬盘**，文件被切成 128MB 的块（Block），分散存在多台机器上。我们用 2 台 DataNode 模拟这个分布式存储。

**为什么 NameNode 那么重要**：它是整个文件系统的"目录"，只记录"哪个文件、哪些块、块在哪台机器上"。NameNode 挂了，数据还在 DataNode 上，但你找不到了。生产上 NameNode 必须高可用（HA），学习环境用单 NameNode 够了。

### 概念 2：Spark 集群是怎么工作的？
**业务类比**：Master 是工头，负责分配任务；Worker 是工人，实际干活。你提交一个 Spark Job，Master 把它分成多个 Task，分发给 3 个 Worker 并行执行。我们配 3 个 Worker × 4GB，总计 12GB 计算内存。

### 概念 3：为什么用 Docker Compose？
在你的机器上模拟 7 个"服务器"（NameNode + 2 DataNode + Spark Master + 3 Worker）。没有 Docker，你需要在 7 台真实机器上手动安装配置 Hadoop + Spark。Docker Compose 用一个 YAML 文件描述这一切，一条命令启动，**环境完全可复现**。

---

## 🛠 操作步骤

### 步骤 1：创建统一环境变量文件

**🤔 为什么这么做**：所有容器共享同一套配置，改一处全局生效，避免版本号、密码散落在多个文件里。

**⌨️ 操作**：文件已为你生成在 `docker/.env`

**✅ 预期效果**：`cat docker/.env` 能看到变量内容

---

### 步骤 2：启动 core 组

**🤔 为什么这么做**：第一次启动会拉取镜像（约 5-8GB），需要耐心等待。

**⌨️ 操作**：
```bash
cd /storage/Study_file/NYC-Taxi-Trip-analysis/nyc-taxi-platform

# 启动（第一次约 5-10 分钟，主要在拉取镜像）
docker compose -f docker/docker-compose.core.yml up -d

# 实时查看启动日志
docker compose -f docker/docker-compose.core.yml logs -f --tail=50
```

**✅ 预期效果**：
```
NAME                STATUS
namenode            Up 2 minutes
datanode1           Up 2 minutes
datanode2           Up 2 minutes
spark-master        Up 2 minutes
spark-worker-1      Up 2 minutes
spark-worker-2      Up 2 minutes
spark-worker-3      Up 2 minutes
hive-metastore      Up 2 minutes
postgres            Up 2 minutes
jupyter             Up 2 minutes
```

**🐛 如果出错**：
- `port is already allocated` → 检查 `lsof -i :9870`，关掉占用端口的进程
- 容器一直 `Restarting` → `docker logs namenode` 查看详情，贴给我
- 内存不足 → 检查 `free -h`，确认可用内存 > 15GB

---

### 步骤 3：初始化 HDFS 目录结构

**🤔 为什么这么做**：在新文件系统上建好规范目录，后续各层数据各归其位。

**⌨️ 操作**：
```bash
docker exec -it namenode bash

hdfs dfs -mkdir -p /nyc-taxi/raw
hdfs dfs -mkdir -p /nyc-taxi/ods
hdfs dfs -mkdir -p /nyc-taxi/dwd
hdfs dfs -mkdir -p /nyc-taxi/dws
hdfs dfs -mkdir -p /nyc-taxi/ads
hdfs dfs -mkdir -p /user/hive/warehouse
hdfs dfs -chmod -R 777 /nyc-taxi
hdfs dfs -chmod -R 777 /user/hive/warehouse
hdfs dfs -ls /

exit
```

**✅ 预期效果**：
```
Found 2 items
drwxrwxrwx   - root supergroup  0 ... /nyc-taxi
drwxrwxrwx   - root supergroup  0 ... /user
```

**🐛 如果出错**：
- `Safe mode` → 运行 `hdfs dfsadmin -safemode leave`，HDFS 刚启动时处于保护模式

---

### 步骤 4：验证 Spark 集群

**🤔 为什么这么做**：确认 3 个 Worker 都注册到 Master，才能正常提交 Job。

**⌨️ 操作**：
```bash
curl -s http://localhost:8080/json/ | python3 -c "
import json, sys
d = json.load(sys.stdin)
workers = d.get('workers', [])
print(f'Workers 数量: {len(workers)}')
for w in workers:
    print(f'  {w[\"id\"]}: {w[\"state\"]}, Memory={w[\"memory\"]}MB')
"
```

**✅ 预期效果**：看到 3 个 ALIVE 状态的 Worker，每个 4096MB

---

### 步骤 5：Jupyter 里跑第一个 Spark 程序（端到端验证）

**🤔 为什么这么做**：验证完整链路——Jupyter → Spark → HDFS，都通才算 STAGE 01 真正完成。

**⌨️ 操作**：浏览器打开 `http://localhost:8888`，新建 Python 3 Notebook，粘贴执行：

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("STAGE01-验证") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

df = spark.createDataFrame(
    [("NYC Cab Co.", 2024, "Ready to go!")],
    ["company", "year", "status"]
)
df.write.mode("overwrite").parquet("hdfs://namenode:9000/nyc-taxi/raw/test")

df2 = spark.read.parquet("hdfs://namenode:9000/nyc-taxi/raw/test")
df2.show()

print(f"Spark 版本: {spark.version}")
print("✅ STAGE 01 验证通过！")
spark.stop()
```

**✅ 预期效果**：
```
+------------+----+------------+
|     company|year|      status|
+------------+----+------------+
|NYC Cab Co. |2024|Ready to go!|
+------------+----+------------+
Spark 版本: 3.4.x
✅ STAGE 01 验证通过！
```

**🐛 如果出错**：
- `Connection to spark-master:7077 refused` → Spark Master 没启动，检查 `docker ps`
- OOM 报错 → 把 `executor.memory` 从 `2g` 改为 `1g` 重试

---

## 🔬 认知实验：感受 HDFS 分块存储

> 本阶段无量化性能对比，做一次认知实验理解 HDFS 的核心机制

**实验目的**：亲眼看到 HDFS 如何把大文件切成块、副本怎么分布

```bash
docker exec -it namenode bash

# 创建 300MB 测试文件
dd if=/dev/urandom of=/tmp/test_300mb.bin bs=1M count=300

# 上传到 HDFS
hdfs dfs -put /tmp/test_300mb.bin /nyc-taxi/raw/

# 查看分块信息
hdfs fsck /nyc-taxi/raw/test_300mb.bin -files -blocks -locations
```

**👀 观察重点**：
1. 300MB 文件被切成几块？（128MB 一块，应该是 3 块）
2. 每块有几个副本？（默认 3 个，但我们只有 2 个 DataNode，所以是 2 个）
3. 块分布在哪些 DataNode 上？

**🧠 原理**：这就是 HDFS 高可用的底层机制——即使一台 DataNode 挂了，另一台还有副本。生产环境通常 3 副本 + 3 DataNode。

```bash
# 清理测试文件
hdfs dfs -rm /nyc-taxi/raw/test_300mb.bin
rm /tmp/test_300mb.bin
exit
```

---

## 💼 简历可写的成果

> 使用 Docker Compose 在单机模拟部署 10 节点分布式集群（HDFS 1NameNode+2DataNode、Spark 1Master+3Worker、Hive Metastore、PostgreSQL、Jupyter），通过 core/serving 分组启停策略将内存占用控制在 15GB 以内，实现开发环境与生产架构拓扑一致。

---

## 🎤 面试可能被问到的问题

1. **Q: HDFS 的 NameNode 和 DataNode 各自存什么？**  
   A: NameNode 只存元数据（文件名、块列表、块在哪台机器），不存真实数据；DataNode 存真实数据块。NameNode 是内存密集型，DataNode 是磁盘密集型。

2. **Q: HDFS 块大小默认 128MB，为什么不设 1MB？**  
   A: 块越小，元数据越多（NameNode 内存压力大），每个块都需要 RPC 建连，小块意味着大量网络开销。128MB 是平衡元数据量和传输效率的经验值。

3. **Q: Docker Compose 和 Kubernetes 的关系？**  
   A: Compose 是单机多容器编排，适合开发/学习；K8s 是跨机器集群编排，适合生产。核心概念一致（Service/Volume/Network），Compose 是 K8s 的简化版入门路径。

---

## 🧹 阶段收尾

- **保持 core 组运行**（STAGE 02 继续需要）
- 清理测试数据：`docker exec namenode hdfs dfs -rm -r /nyc-taxi/raw/test`
- 建议提交一次 git：`git add docker/ && git commit -m "feat: STAGE01 core 组环境搭建完成"`

---

## ➡️ 下一阶段预告

STAGE 02 我们会下载 NYC Yellow Taxi 2024 Q1 数据（3 个月约 1.5GB），完成：
- 原始 Parquet 数据上传到 HDFS ODS 层
- 用 PySpark 做首次数据探索（字段含义、数据分布）
- **核心实验 #1**：CSV vs Parquet 扫描速度对比（第一次亲眼见证列式存储的威力）

需要本阶段产出：HDFS `/nyc-taxi/ods` 目录就绪、Spark 集群可提交 Job。
