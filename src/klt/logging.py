import logging
import sys

from loguru import logger


class InterceptHandler(logging.Handler):
    @logger.catch(default=True, onerror=lambda _: sys.exit(1))
    def emit(self, record):
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


logger_dlt = logging.getLogger("dlt")
logger_dlt.addHandler(InterceptHandler())

logger.add("dlt_loguru.log")
