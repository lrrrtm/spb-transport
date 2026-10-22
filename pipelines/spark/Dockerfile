# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:0.10.2 AS uv
FROM apache/spark:3.5.9-scala2.12-java17-python3-ubuntu

USER root
COPY --from=uv /uv /uvx /bin/
ADD --checksum=sha256:fed393dcc3ef70611fe3d4f35ae78a8ea7c55301fd4cdd66c5dc97fcfaa5a00f \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.9/spark-sql-kafka-0-10_2.12-3.5.9.jar /opt/spark/jars/
ADD --checksum=sha256:e82612e6070438c5ea48b08c74f367a6d214a390bdf6dcf3ff316dca592ddda5 \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.9/spark-token-provider-kafka-0-10_2.12-3.5.9.jar /opt/spark/jars/
ADD --checksum=sha256:9d0a9058c8ede79db6e54ae378ed16fe8d1bead2894635a4aa6aa0b7981668c6 \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD --checksum=sha256:ea0505ee7515e58b1ac0e686e4d1a5d9f7d808e251a61bc371aa0595b9963f83 \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/
ADD --checksum=sha256:bec0b24dcb23f9670172724826584802b80ae6cbdaba03bdebdef9327b962f6a \
    https://repo1.maven.org/maven2/com/google/code/findbugs/jsr305/3.0.0/jsr305-3.0.0.jar /opt/spark/jars/
ADD --checksum=sha256:d9ad7f7eec75b256a809b2f7df9b20a5f1d44e2dc5a54082a7f7ead27a8b9ab0 \
    https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-assembly_2.12/3.5.1/spark-cassandra-connector-assembly_2.12-3.5.1.jar /opt/spark/jars/
RUN chmod 0644 /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.9.jar \
    /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.9.jar \
    /opt/spark/jars/kafka-clients-3.4.1.jar \
    /opt/spark/jars/commons-pool2-2.11.1.jar \
    /opt/spark/jars/jsr305-3.0.0.jar \
    /opt/spark/jars/spark-cassandra-connector-assembly_2.12-3.5.1.jar
WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY packages/contracts packages/contracts
COPY apps/collector apps/collector
COPY pipelines/spark pipelines/spark
ENV PATH="/app/.venv/bin:$PATH" \
    UV_PYTHON_INSTALL_DIR="/opt/uv-python" \
    UV_PYTHON_PREFERENCE="only-managed"

ARG UV_INSECURE_HOST
RUN if [ -n "$UV_INSECURE_HOST" ]; then \
      echo "WARNING: uv TLS verification is disabled for: $UV_INSECURE_HOST"; \
      UV_INSECURE_HOST="$UV_INSECURE_HOST" \
        uv sync --python 3.13 --frozen --no-dev \
          --package spb-transport-spark; \
    else \
      uv sync --python 3.13 --frozen --no-dev --package spb-transport-spark; \
    fi

USER spark
