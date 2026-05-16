-- 初始化脚本：PostgreSQL 首次启动时自动执行
-- 创建 Hive Metastore 数据库 + ADS 业务数据库

-- 创建 Hive Metastore 专用数据库
CREATE DATABASE hive_metastore
    WITH ENCODING='UTF8'
    LC_COLLATE='en_US.UTF-8'
    LC_CTYPE='en_US.UTF-8'
    TEMPLATE=template0;

-- 给 taxi_user 授权
GRANT ALL PRIVILEGES ON DATABASE hive_metastore TO taxi_user;
GRANT ALL PRIVILEGES ON DATABASE nyc_taxi TO taxi_user;

-- 在 nyc_taxi 数据库里预创建 ADS schema（后续阶段使用）
\c nyc_taxi;
CREATE SCHEMA IF NOT EXISTS ads;
CREATE SCHEMA IF NOT EXISTS dim;

COMMENT ON SCHEMA ads IS 'ADS 应用层：面向业务的结果表';
COMMENT ON SCHEMA dim IS '维度层：区域、时间等维表';
