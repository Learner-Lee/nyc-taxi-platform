#!/bin/bash
# 全栈健康检查脚本
# 用法: bash scripts/health_check.sh

echo "======================================"
echo " NYC Taxi Platform — 健康检查"
echo "======================================"

echo ""
echo "【容器状态】"
docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "NAME|namenode|datanode|spark|hive|postgres|jupyter|click|airflow|superset|streamlit"

echo ""
echo "【内存占用 Top 10】"
docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}" 2>/dev/null | head -12

echo ""
echo "【HDFS 存活节点】"
docker exec namenode hdfs dfsadmin -report 2>/dev/null | grep -E "Live datanodes|Dead datanodes|DFS Used|DFS Remaining" || echo "  NameNode 未启动"

echo ""
echo "【Spark Workers】"
curl -s http://localhost:8080/json/ 2>/dev/null | python3 -c "
import json, sys
d = json.load(sys.stdin)
workers = d.get('workers', [])
print(f'  总数: {len(workers)} 个')
for w in workers:
    used = w.get('memoryused', 0)
    total = w.get('memory', 0)
    print(f'  {w[\"id\"][:30]}... state={w[\"state\"]}, Mem={used}/{total}MB')
" 2>/dev/null || echo "  Spark Master 未启动"

echo ""
echo "【Web UI 地址】"
echo "  HDFS      : http://localhost:9870"
echo "  Spark     : http://localhost:8080"
echo "  History   : http://localhost:18080"
echo "  Jupyter   : http://localhost:8888"
echo "  ClickHouse: http://localhost:8123/play (serving 组)"
echo "  Airflow   : http://localhost:8081     (serving 组)"
echo "  Superset  : http://localhost:8088     (serving 组)"
echo "  Streamlit : http://localhost:8501     (serving 组)"
echo "======================================"
