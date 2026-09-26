"""JSON structured logging."""

import logging
import sys

import structlog


def configure_logging(service: str) -> structlog.stdlib.BoundLogger:
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
    )
    logger: structlog.stdlib.BoundLogger = structlog.get_logger().bind(service=service)
    return logger
