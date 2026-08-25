FROM apache/spark:3.5.9-scala2.12-java17-python3-ubuntu

ARG LIVY_VERSION=0.9.0-incubating
ARG LIVY_ARCHIVE_SHA512=50151be834bba3011fccbb564c3c9d6bf7a08af468c2082604f3c5b40ba95705a4c2e925e835bd58a58abcbeb18f3ab7bf588e01fa4f778ff9e82a4b91b9d2de

USER root

ADD https://downloads.apache.org/incubator/livy/${LIVY_VERSION}/apache-livy-${LIVY_VERSION}_2.12-bin.zip /tmp/apache-livy.zip
RUN echo "${LIVY_ARCHIVE_SHA512}  /tmp/apache-livy.zip" | sha512sum --check - \
    && python3 -m zipfile -e /tmp/apache-livy.zip /opt \
    && mv "/opt/apache-livy-${LIVY_VERSION}_2.12-bin" /opt/livy \
    && rm /tmp/apache-livy.zip \
    && mkdir -p /opt/livy/logs /opt/iceberg-ops/jobs

ADD --checksum=sha256:94b8e36fc329f0293d44ba9e01b784a56e9501affec1842d898144c51f6e486a \
    https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.11.0/iceberg-spark-runtime-3.5_2.12-1.11.0.jar \
    /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.11.0.jar

COPY playground/livy/conf/livy.conf /opt/livy/conf/livy.conf
COPY spark-jobs/iceberg_maintenance.py /opt/iceberg-ops/jobs/iceberg_maintenance.py

RUN chmod 0755 /opt/livy/bin/livy-server \
    && chmod 0644 /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.11.0.jar \
    && chown -R 185:0 /opt/livy /opt/iceberg-ops

ENV LIVY_HOME=/opt/livy \
    LIVY_CONF_DIR=/opt/livy/conf \
    SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=python3 \
    PYTHONUNBUFFERED=1

USER 185
WORKDIR /opt/livy
EXPOSE 8998

ENTRYPOINT ["/opt/livy/bin/livy-server"]
