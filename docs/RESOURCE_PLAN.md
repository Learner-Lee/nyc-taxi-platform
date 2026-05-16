# RESOURCE_PLAN — 资源规划与分组启停策略

> ⚠️ **这份文档救命用的** — M5 32GB 同时跑全栈必然 OOM，读完再动手

---

## 一、各组件内存占用预估

| 组件 | 容器名 | 内存下限 | 推荐配置 | 说明 |
|------|--------|---------|---------|------|
| HDFS NameNode | namenode | 512MB | 1GB | 管理元数据，稳定后约 400MB |
| HDFS DataNode × 2 | datanode1/2 | 512MB × 2 | 768MB × 2 | 数据节点，读写时会上升 |
| Spark Master | spark-master | 512MB | 512MB | 调度器，很轻 |
| Spark Worker × 3 | spark-worker-1/2/3 | 4GB × 3 | 4GB × 3 | **大户**，核心计算资源 |
| Hive Metastore | hive-metastore | 512MB | 768MB | 元数据服务 + Derby/Postgres |
| PostgreSQL | postgres | 256MB | 512MB | 轻量，ADS 层 + Metastore 后端 |
| Jupyter | jupyter | 512MB | 1GB | Notebook 内核 |
| **core 组合计** | | **~14GB** | **~15GB** | 含操作系统约占 16-17GB |
| ClickHouse | clickhouse | 2GB | 4GB | 冷启动后内存会增长 |
| Airflow (含 Worker) | airflow-* | 1.5GB | 2GB | Scheduler + Worker + Redis |
| Superset | superset | 512MB | 1GB | 含 Gunicorn 多进程 |
| Streamlit | streamlit | 256MB | 512MB | 很轻 |
| **serving 组合计** | | **~4.5GB** | **~7.5GB** | |
| **全栈合计** | | **~18.5GB** | **~22.5GB** | 距 24GB 上限尚有约 1.5GB 余量 |

> **OS 与 Docker Daemon 自身约占 1-1.5GB**，所以全栈极限状态下内存非常紧张。

---

## 二、Docker Desktop 内存设置

```
Docker Desktop → Settings → Resources → Memory: 24 GB
```

留 8GB 给 macOS 系统（M5 32GB 机器的合理配比）。

**验证方式**（在宿主机运行）：
```bash
docker stats --no-stream | head -20
```

---

## 三、分组启停策略

### 3.1 core 组（常驻，每次工作前启动）

```bash
# 启动
docker compose -f docker/docker-compose.core.yml up -d

# 关闭（收工时）
docker compose -f docker/docker-compose.core.yml down
```

**包含服务**：
- HDFS NameNode
- HDFS DataNode × 2
- Spark Master + Worker × 3
- Hive Metastore
- PostgreSQL
- Jupyter

**启动耗时**：约 60-90 秒（等 Spark Worker 注册完成）

**验证 core 组健康**：
```bash
# 检查所有容器状态
docker compose -f docker/docker-compose.core.yml ps

# 确认 Spark Workers 已注册
curl -s http://localhost:8080/json/ | python3 -c "
import json, sys
d = json.load(sys.stdin)
print(f'Workers: {len(d[\"workers\"])}, Status: {[w[\"state\"] for w in d[\"workers\"]]}')
"
```

---

### 3.2 serving 组（按需，用完即停）

```bash
# 启动（需要看板/调度时）
docker compose -f docker/docker-compose.serving.yml up -d

# 用完关闭（释放 ~7GB 内存）
docker compose -f docker/docker-compose.serving.yml down
```

**包含服务**：ClickHouse + Airflow + Superset + Streamlit

> **原则**：做 STAGE 01-08 只需 core 组。STAGE 09 起才需要 serving 组中的 ClickHouse。STAGE 10 才需要 Airflow，以此类推。

---

## 四、各阶段启动矩阵

| 阶段 | core 组 | ClickHouse | Airflow | Superset | Streamlit | 预计内存 |
|------|---------|-----------|--------|---------|----------|---------|
| STAGE 01 环境搭建 | ✅ | ❌ | ❌ | ❌ | ❌ | ~15GB |
| STAGE 02 ODS 接入 | ✅ | ❌ | ❌ | ❌ | ❌ | ~15GB |
| STAGE 03 存储优化 | ✅ | ❌ | ❌ | ❌ | ❌ | ~15GB |
| STAGE 04 DWD 清洗 | ✅ | ❌ | ❌ | ❌ | ❌ | ~16GB |
| STAGE 05 计算优化 | ✅ | ❌ | ❌ | ❌ | ❌ | ~17GB |
| STAGE 06 数据倾斜 | ✅ | ❌ | ❌ | ❌ | ❌ | ~17GB |
| STAGE 07 DWS+查询 | ✅ | ❌ | ❌ | ❌ | ❌ | ~17GB |
| STAGE 08 ADS+PG索引 | ✅ | ❌ | ❌ | ❌ | ❌ | ~16GB |
| STAGE 09 ClickHouse | ✅ | ✅ | ❌ | ❌ | ❌ | ~20GB |
| STAGE 10 Airflow | ✅ | ✅ | ✅ | ❌ | ❌ | ~22GB |
| STAGE 11 Superset | ✅ | ✅ | ✅ | ✅ | ❌ | ~23GB |
| STAGE 12 Streamlit | ✅ | ✅ | ❌ | ❌ | ✅ | ~21GB |
| STAGE 13 复盘 | ✅ | ❌ | ❌ | ❌ | ❌ | ~15GB |

> **关键原则**：STAGE 09+ 启动 ClickHouse 时，Spark Worker 各减少 1GB（从 4GB 改为 3GB）以腾出空间。

---

## 五、Spark Worker 内存动态配置

### 常规模式（STAGE 01-08，无 ClickHouse）

```yaml
# docker/docker-compose.core.yml 中的 Worker 配置
environment:
  - SPARK_WORKER_MEMORY=4g
  - SPARK_WORKER_CORES=2
```

### 节约模式（STAGE 09+，有 ClickHouse）

```yaml
environment:
  - SPARK_WORKER_MEMORY=3g   # 从 4g 降到 3g，三个 Worker 共节省 3GB
  - SPARK_WORKER_CORES=2
```

切换方式：
```bash
# 修改 .env 中的 SPARK_WORKER_MEMORY，然后重建 Worker
docker compose -f docker/docker-compose.core.yml up -d --no-deps spark-worker-1 spark-worker-2 spark-worker-3
```

---

## 六、OOM 应急方案

当你看到容器被杀死（`docker ps` 中某容器消失）或 Spark 任务 OOM 报错时：

### 应急步骤 1：查清楚谁在吃内存
```bash
docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}" | sort -k3 -rh
```

### 应急步骤 2：暂停不需要的服务
```bash
# 暂停（保留数据，恢复快）
docker compose -f docker/docker-compose.serving.yml pause clickhouse

# 或直接关停 serving 组
docker compose -f docker/docker-compose.serving.yml down
```

### 应急步骤 3：降低 Spark 并行度

在 Spark 代码中临时加入：
```python
spark.conf.set("spark.sql.shuffle.partitions", "50")   # 默认 200，降低并行度
spark.conf.set("spark.executor.memory", "2g")           # 降低单 Executor 内存
```

### 应急步骤 4：极限降配（单 Worker 模式）

修改 `docker/docker-compose.core.yml`，只保留 1 个 Worker：
```bash
docker compose -f docker/docker-compose.core.yml up -d --scale spark-worker=1
```

此时可用计算内存约 4GB，只能处理小数据集（2024 Q1 的 3 个月数据）。

---

## 七、关键端口速查

| 服务 | Web UI | 说明 |
|------|--------|------|
| HDFS NameNode | http://localhost:9870 | 文件系统浏览、DataNode 健康 |
| Spark Master | http://localhost:8080 | Worker 状态、Job 历史 |
| Spark History | http://localhost:18080 | 已完成 Job 的详细执行计划 |
| Jupyter | http://localhost:8888 | Notebook 入口（无密码）|
| ClickHouse | http://localhost:8123 | HTTP 接口，Play UI |
| Airflow | http://localhost:8081 | DAG 管理（admin/admin）|
| Superset | http://localhost:8088 | BI 看板（admin/admin）|
| Streamlit | http://localhost:8501 | 司机端应用 |
| PostgreSQL | localhost:5432 | psql / DBeaver 连接 |

---

## 八、磁盘空间规划

| 目录 | 内容 | 预计大小 |
|------|------|---------|
| `data/raw/` | 原始 Parquet（NYC TLC 下载）| 50-80GB |
| HDFS（容器 volume）| ODS + DWD + DWS 数据 | 80-120GB |
| ClickHouse data | 热数据（DWD 约 1 年）| 20-30GB |
| PostgreSQL data | ADS 聚合结果 | < 5GB |
| **合计** | | **~200-235GB** |

> **建议**：确保磁盘剩余 > 250GB 再开始。用 `df -h /` 检查。

---

## 九、快速健康检查脚本

```bash
#!/bin/bash
# 保存为 scripts/health_check.sh

echo "=== Docker 容器状态 ==="
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E "NAME|hdfs|spark|hive|postgres|jupyter|click|airflow|superset|streamlit"

echo ""
echo "=== 内存使用 ==="
docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}"

echo ""
echo "=== HDFS 状态 ==="
docker exec namenode hdfs dfsadmin -report 2>/dev/null | grep -E "Live|Dead|DFS Used|DFS Remaining"

echo ""
echo "=== Spark Workers ==="
curl -s http://localhost:8080/json/ 2>/dev/null | python3 -c "
import json, sys
d = json.load(sys.stdin)
for w in d.get('workers', []):
    print(f'  {w[\"id\"]}: {w[\"state\"]}, Memory: {w[\"memoryused\"]}/{w[\"memory\"]}MB')
" || echo "  Spark Master 未启动"
```
