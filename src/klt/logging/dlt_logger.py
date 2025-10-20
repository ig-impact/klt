"""DLT logging configuration - Intercepts dlt logs and redirects to loguru."""

import logging
import sys

from loguru import logger


class InterceptHandler(logging.Handler):
    """Handler that intercepts standard logging and redirects to loguru."""

    @logger.catch(default=True, onerror=lambda _: sys.exit(1))
    def emit(self, record):
        """Emit a log record through loguru.
        
        Args:
            record: The log record from standard logging
        """
        # Get the corresponding Loguru level if it exists.
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find the caller from where the logged message originated.
        frame, depth = sys._getframe(6), 6
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


# Remove default stderr handler to avoid console pollution
logger.remove()

# Add stderr handler ONLY for non-HTTP logs (DLT logs only)
logger.add(
    sys.stderr,
    filter=lambda record: "resource" not in record["extra"],  # Exclude HTTP logs
    format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
)

# Add global log file for dlt (exclude HTTP logs)
logger.add(
    "dlt_loguru.log",
    filter=lambda record: "resource" not in record["extra"],  # Exclude HTTP logs
    rotation="10 MB",
    retention="30 days",
    compression="zip",
)

# Configure dlt logger to use loguru
logger_dlt = logging.getLogger("dlt")
logger_dlt.addHandler(InterceptHandler())

