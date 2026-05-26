-- ============================================================
-- ADS 层索引定义 — ads.trip_search 司机搜索表
-- 实验 #12 验证:5 阶段对比,最终用覆盖索引/部分索引
-- 执行: psql -h localhost -U taxi_user -d nyc_taxi -f ads_indexes.sql
-- ============================================================

-- 清理旧索引(幂等)
DROP INDEX IF EXISTS ads.ix_ts_borough;
DROP INDEX IF EXISTS ads.ix_ts_composite;
DROP INDEX IF EXISTS ads.ix_ts_cover;
DROP INDEX IF EXISTS ads.ix_ts_partial;

-- ── 方案 1: 单列 B-Tree(实测无效,Manhattan 占 54% 低选择性,优化器放弃)──
-- CREATE INDEX ix_ts_borough ON ads.trip_search(pickup_borough);
-- 保留注释作为"反面教材"记录,不实际创建

-- ── 方案 2: 复合索引(实测 141.9x 加速)──
-- (borough, time_bucket, revenue DESC):WHERE 两列精确定位 + revenue 预排序
-- CREATE INDEX ix_ts_composite ON ads.trip_search(pickup_borough, time_bucket, revenue DESC);

-- ── 方案 3: 覆盖索引(实测 234.6x 加速,生产推荐)──
-- INCLUDE 把 SELECT 列塞进索引 → Index Only Scan, Heap Fetches=0 消除回表
CREATE INDEX ix_ts_cover ON ads.trip_search(pickup_borough, time_bucket, revenue DESC)
    INCLUDE (pickup_zone, trips);

-- ── 方案 4: 部分索引(实测 254.2x 加速,Manhattan 专用,体积最小)──
-- 只索引 Manhattan 的行,适合"热点 borough 高频查询"场景
CREATE INDEX ix_ts_partial ON ads.trip_search(time_bucket, revenue DESC)
    INCLUDE (pickup_zone, trips)
    WHERE pickup_borough = 'Manhattan';

-- 收集统计,让优化器准确选择索引
ANALYZE ads.trip_search;
