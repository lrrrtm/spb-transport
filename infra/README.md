# Локальная инфраструктура

`compose.yaml` поднимает один Kafka broker в KRaft-режиме, HDFS NameNode/DataNode, Spark master/worker и Cassandra. Это локальная среда разработки, а не production topology.

Hadoop XML находится в `infra/hadoop`. Версии образов и порты задаются в `.env`; проверка конфигурации выполняется через `make compose-check`.

