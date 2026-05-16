FROM bitnamilegacy/spark:3.4.1

USER root

# Python 数据生态依赖
RUN pip install --no-cache-dir \
    pandas==2.1.4 \
    pyarrow==14.0.2 \
    numpy==1.26.2 \
    psycopg2-binary==2.9.9

# 用 Docker 原生 ADD 下载 JDBC 驱动（镜像内无 curl/wget）
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar \
    /opt/bitnami/spark/jars/postgresql-42.7.1.jar

RUN chmod 644 /opt/bitnami/spark/jars/postgresql-42.7.1.jar

COPY spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf

USER 1001
