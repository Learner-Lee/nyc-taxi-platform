# STAGE 10: Airflow 调度编排

## 🎯 本阶段目标

- **业务问题**:前 9 个阶段都是**手动在 Jupyter 一个 cell 一个 cell 跑**。真实生产里,ODS→DWD→DWS→ADS 这条链路要**每天凌晨自动跑**,失败要重试要告警。这一阶段用 Airflow 把手动流程升级为**自动化 Pipeline**——这是"数据分析师 → 数据工程师"的关键一跃。
  ![image-20260603183646239](STAGE_10_Airflow调度编排.assets/image-20260603183646239.png)
- **技术能力**:
  - 理解 DAG(有向无环图)、Operator、Task 依赖
  - 用 Airflow 编排多层 ETL 任务
  - **幂等性设计**:任务重跑不产生重复数据(为什么我们一直用 INSERT OVERWRITE)
  - 失败重试 + 告警配置
  - 理解 `schedule_interval` 与手动 trigger
- **产出物**:
  - `airflow/dags/nyc_taxi_etl.py` (ETL DAG 定义)
  - `jobs/` 下的可独立运行 ETL 脚本
  - Airflow Web UI 能看到 DAG 运行成功

---

## 📋 前置检查清单

- [ ] STAGE 09 完成,各层数据稳定
- [ ] **内存预算**:core(~13G) + Airflow(~2G) = 15G。**ClickHouse(4G)建议先停**(STAGE 10 不需要它)
- [ ] core 组运行中

### ⚠️ 内存腾挪:先停 ClickHouse
```bash
# STAGE 10 不需要 ClickHouse,停掉腾 4GB(数据在 volume 里,不会丢)
docker compose -f docker/docker-compose.serving.yml stop clickhouse
```

---

## 🧭 关键决策点(开始前必须定)

Airflow 调度 Spark 任务有两种方案,**复杂度和真实度不同**:

### 选项 A:完整版(spark-submit,生产真实)
- 自定义 Airflow 镜像(装 OpenJDK + PySpark)
- DAG 用 `BashOperator` 跑 `spark-submit --master spark://spark-master:7077`
- **优点**:和生产一致,简历含金量高
- **缺点**:要构建镜像(~10 分钟)、内存多占、调试复杂

### 选项 B:轻量版(PythonOperator,聚焦 Airflow 概念)
- 用官方 Airflow 镜像,不集成 Spark
- DAG 用 `PythonOperator` 调度**纯 SQL 刷新**(PG/ClickHouse 的 ADS 层聚合)
- **优点**:快、稳、内存省,Airflow 的 DAG/依赖/重试/幂等/调度概念全覆盖
- **缺点**:不演示 Spark 提交(但 Spark 提交本身和 Airflow 学习关系不大)

**我的建议:选 B**。理由:
1. STAGE 10 的学习目标是 **Airflow 本身**(DAG/依赖/重试/调度),不是"怎么提交 Spark"
2. Spark 提交在 STAGE 01-09 已经做透了
3. 内存友好,M5 不会爆
4. 轻量版同样能写出漂亮的简历 bullet 和讲清楚幂等性

> 如果你执意要完整版(简历想写"Airflow 调度 Spark 集群"),告诉我,我给自定义镜像方案。**否则默认走 B**。

---

## 📚 核心概念(5 分钟读完)

### 概念 1:DAG 是什么?
**DAG**(Directed Acyclic Graph,有向无环图)= 一组任务 + 它们的依赖关系,**不能有环**(A 依赖 B,B 不能反过来依赖 A)。

**业务类比**:做菜的步骤图——"切菜 → 炒菜 → 装盘",装盘依赖炒菜,炒菜依赖切菜,不能颠倒。Airflow 的 DAG 就是把 ETL 步骤画成这种依赖图,自动按顺序执行。

```
ods_check → dwd_refresh → dws_refresh → ads_refresh → quality_check
```

### 概念 2:Operator 与 Task
- **Operator**:任务的"类型"(`PythonOperator` 跑 Python 函数,`BashOperator` 跑 shell,`SparkSubmitOperator` 提交 Spark)
- **Task**:Operator 的一个实例(具体的一个任务节点)

### 概念 3:幂等性(Idempotency)— Airflow 的灵魂
**定义**:同一个任务跑 1 次和跑 100 次,结果一样(不会重复累积数据)。

**为什么重要**:Airflow 任务会失败重试、会手动重跑、会补数据(backfill)。如果不幂等,重跑就会**数据翻倍**。

**我们怎么保证幂等的**(回顾前面阶段):
- DWD/DWS 一直用 `INSERT OVERWRITE`(覆盖,不是 append)
- ADS 用 Spark `mode("overwrite")`
- **这就是为什么从 STAGE 04 起我坚持用 OVERWRITE 而不是 INSERT INTO**——为 Airflow 调度埋的伏笔

### 概念 4:schedule_interval 与 Catchup
- `schedule_interval='@daily'`:每天跑一次
- `start_date`:DAG 从哪天开始算
- `catchup=False`:不补跑历史(否则 Airflow 会把 start_date 到现在的所有调度补一遍)

---

## 🛠 操作步骤(选项 B 轻量版)

### 步骤 1:在 serving compose 补 Airflow 服务

确认 `docker/docker-compose.serving.yml` 有 airflow 服务。如果没有,追加(精简单容器版,LocalExecutor):

```yaml
  airflow:
    image: apache/airflow:2.7.3
    platform: linux/amd64
    container_name: airflow
    environment:
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://taxi_user:taxi_pass123@postgres:5432/airflow
      - AIRFLOW__CORE__LOAD_EXAMPLES=false
      - AIRFLOW__CORE__FERNET_KEY=ZmDfcTF7_60GrrY167zsiPd67pEvs0aGOv2oasOM1Pg=
      - _AIRFLOW_WWW_USER_USERNAME=admin
      - _AIRFLOW_WWW_USER_PASSWORD=admin
      - _PIP_ADDITIONAL_REQUIREMENTS=psycopg2-binary
    ports:
      - "8081:8080"
    volumes:
      - ../airflow/dags:/opt/airflow/dags
      - ../jobs:/opt/airflow/jobs
    command: >
      bash -c "airflow db migrate &&
               airflow users create --username admin --password admin
                 --firstname A --lastname B --role Admin --email a@b.com || true &&
               (airflow scheduler &) && airflow webserver"
    networks:
      - taxi-net
```

> 关键:Airflow 元数据库用我们已有的 PostgreSQL(建个 `airflow` 库),不再起单独的 PG。

### 步骤 2:在 PostgreSQL 建 airflow 元数据库

```bash
docker exec postgres psql -U taxi_user -d nyc_taxi -c "CREATE DATABASE airflow"
```

### 步骤 3:启动 Airflow

```bash
docker compose -f docker/docker-compose.serving.yml up -d airflow
# 首次启动慢(db migrate + 装 psycopg2),等 60-90 秒
sleep 90
docker logs airflow 2>&1 | tail -15
```

访问 http://localhost:8081(admin/admin)。

### 步骤 4:写 ETL DAG

DAG 文件放 `airflow/dags/nyc_taxi_etl.py`。轻量版:用 PythonOperator 调 PG SQL 刷新 ADS 层 + 数据质量检查。(具体代码动手时给)

---

## 🔬 幂等性验证实验(实测 2026-05-26)

跑两次 DAG,对比 `ads.borough_summary` 汇总:

| 指标 | 第一次 | 第二次 | 结果 |
|------|--------|--------|------|
| rows | 32 | 32 | ✅ 一致 |
| total_trips | 9,227,227 | 9,227,227 | ✅ 一致 |
| total_rev | $253,231,727 | $253,231,727 | ✅ 一致 |

**结论**:`TRUNCATE + INSERT` 让重跑结果 100% 一致。如果改成 `INSERT INTO`(去掉 TRUNCATE),第二次会变 64 行 / 订单翻倍——**这就是不幂等的灾难**,补一次数据报表就错。

**额外发现**:`total_trips = 9,227,227` 正好 = DWD 行数(聚合无丢失);**NYC Cab Co. 2024 Q1 总营收 $2.53 亿**。

---

## 💼 简历可写的成果(STAGE 10 新增 1 条)

> • **Airflow 自动化 ETL 编排**:用 Airflow(LocalExecutor + PostgreSQL 元数据库)把 ODS→DWD→DWS→ADS 手动流程编排为 DAG,配置任务依赖、失败重试(retries + retry_delay)、数据质量检查任务;**通过全链路 INSERT OVERWRITE 保证幂等性**(验证重跑 DAG 数据不翻倍),实现日级自动化调度

---

## 🎤 面试可能被问到的问题

1. **Q: 怎么保证 Airflow 任务幂等?**
   A: 核心是**写操作可重入**——用 `INSERT OVERWRITE`/`MERGE`/`DELETE+INSERT` 而非 `INSERT INTO`;按分区覆盖(只重写当天分区);用确定性的输出路径(不带时间戳随机后缀)。**我的项目从 DWD 起全链路 OVERWRITE,就是为了重跑安全**。

2. **Q: Airflow 的 Executor 有哪几种?**
   A: SequentialExecutor(单线程,调试用)、LocalExecutor(单机多进程,中小规模)、CeleryExecutor(分布式,多 worker)、KubernetesExecutor(每任务一个 pod)。我项目用 LocalExecutor(单机够用)。

3. **Q: schedule_interval='@daily' 的任务,数据日期怎么取?**
   A: 用 Airflow 的 **execution_date / logical_date**(模板变量 `{{ ds }}`),它是"这次调度代表的业务日期",不是"实际运行时间"。这样补数据(backfill)时每个任务知道自己处理哪天的数据。

4. **Q: 任务失败了怎么办?**
   A: 配 `retries=3` + `retry_delay`,自动重试;`email_on_failure` 或接 Slack/钉钉告警;`sla` 设超时;严重的用 `on_failure_callback` 自定义处理。**幂等是重试的前提**——不幂等的任务不敢自动重试。

5. **Q: catchup=True 和 False 的区别?**
   A: True 时 Airflow 会把 `start_date` 到当前的所有未跑调度**补跑一遍**(适合补历史数据);False 只跑最新的(适合"从现在开始调度")。**生产新上线 DAG 通常 catchup=False**,避免一上线就跑几百个历史任务把集群打爆。

---

## 🧹 阶段收尾

```bash
cd /Users/alen/DA/NYC-Taxi-Trip-analysis/nyc-taxi-platform
git add docs/ airflow/ jobs/
git commit -m "feat(stage10): Airflow ETL DAG + 幂等性验证"
```

---

## ➡️ 下一阶段预告

STAGE 11 **Superset 看板**:
- 启动 Superset(serving 组)
- 连接 ClickHouse(运营看板)+ PostgreSQL(财务看板)
- 制作运营看板(订单量趋势、热点区域)+ 财务看板(营收报表)
- 把前面挖到的业务洞察(早高峰 53%、机场金矿、黑色周日)**可视化**

需要本阶段产出:稳定的 ADS 层 + DAG 跑通经验。**STAGE 11 要重新启动 ClickHouse**。
