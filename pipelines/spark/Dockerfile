# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:0.10.2 AS uv
FROM apache/spark:4.0.4-python3

USER root
COPY --from=uv /uv /uvx /bin/
ADD --checksum=sha256:b2f7b3a4fbf292b5bf10bd4dd6af301eded9c9985b16239b407401d66dbc8a25 \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.4/spark-sql-kafka-0-10_2.13-4.0.4.jar /opt/spark/jars/
ADD --checksum=sha256:ae9817053af94a21992e948269eba1a50d511333cb51462865983abfbeea7f60 \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.4/spark-token-provider-kafka-0-10_2.13-4.0.4.jar /opt/spark/jars/
ADD --checksum=sha256:7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9 \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar /opt/spark/jars/
ADD --checksum=sha256:6d3bd18df8410f3e31b031aca582cc109342358a62a2759ebd0c4cdf30d06f8b \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar /opt/spark/jars/
ADD --checksum=sha256:bec0b24dcb23f9670172724826584802b80ae6cbdaba03bdebdef9327b962f6a \
    https://repo1.maven.org/maven2/com/google/code/findbugs/jsr305/3.0.0/jsr305-3.0.0.jar /opt/spark/jars/
ADD --checksum=sha256:4eae6e68cf44e9f709970355590ae981883edf6484608d747376a56cbb285432 \
    https://repo1.maven.org/maven2/org/scala-lang/modules/scala-parallel-collections_2.13/1.2.0/scala-parallel-collections_2.13-1.2.0.jar /opt/spark/jars/
RUN chmod 0644 /opt/spark/jars/spark-sql-kafka-0-10_2.13-4.0.4.jar \
    /opt/spark/jars/spark-token-provider-kafka-0-10_2.13-4.0.4.jar \
    /opt/spark/jars/kafka-clients-3.9.1.jar \
    /opt/spark/jars/commons-pool2-2.12.0.jar \
    /opt/spark/jars/jsr305-3.0.0.jar \
    /opt/spark/jars/scala-parallel-collections_2.13-1.2.0.jar
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
