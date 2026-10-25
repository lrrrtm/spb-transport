import json
import logging
from datetime import UTC, datetime

import uvicorn

from spb_transport_api.app import create_app
from spb_transport_api.config import ApiSettings


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        error = getattr(record, "error", None)
        if error is not None:
            payload["error"] = error
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def configure_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)
    logging.getLogger("cassandra").setLevel(logging.WARNING)


def main() -> None:
    configure_logging()
    settings = ApiSettings()  # type: ignore[call-arg]  # values come from the environment
    uvicorn.run(create_app(settings), host=settings.host, port=settings.port, log_config=None)


if __name__ == "__main__":
    main()
