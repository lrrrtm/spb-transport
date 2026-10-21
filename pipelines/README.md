# Data pipelines

`spark/` содержит Structured Streaming jobs. Raw archive читает только
`gtfs-raw-response.v1`: валидные envelope сохраняются в Parquet/HDFS, а ошибки
контракта, checksum/base64 и protobuf decode направляются в отдельный quarantine dataset.
Pipeline-код не импортирует внутренности FastAPI или frontend.
