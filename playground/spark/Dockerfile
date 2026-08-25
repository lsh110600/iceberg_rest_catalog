FROM apache/spark:3.5.9-scala2.12-java17-python3-ubuntu AS base

USER root

ADD --checksum=sha256:94b8e36fc329f0293d44ba9e01b784a56e9501affec1842d898144c51f6e486a \
    https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.11.0/iceberg-spark-runtime-3.5_2.12-1.11.0.jar \
    /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.11.0.jar
RUN chmod 0644 /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.11.0.jar

WORKDIR /opt/playground
COPY requirements.txt .
RUN python3 -m pip install --no-cache-dir -r requirements.txt

COPY --chown=185:0 app app

ENV PYTHONPATH=/opt/playground:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
    PYTHONUNBUFFERED=1

FROM base AS test
USER root
RUN python3 -m pip install --no-cache-dir pytest==8.4.1
COPY --chown=185:0 tests tests
USER 185
ENTRYPOINT []
CMD ["python3", "-m", "pytest", "-q"]

FROM base AS runtime

USER 185
EXPOSE 8090

ENTRYPOINT ["python3", "-m", "uvicorn"]
CMD ["app.main:app", "--host", "0.0.0.0", "--port", "8090"]
