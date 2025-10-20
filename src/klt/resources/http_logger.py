"""HTTP response logger for Kobo API calls."""
from pathlib import Path
from typing import Callable, Optional

from loguru import logger
from requests import Response

# Track registered sinks to avoid duplicates
_registered_sinks = {}


def create_http_logger(
    resource_name: Optional[str] = None,
    log_file: Optional[str] = None,
) -> Callable[[Response], Response]:
    """
    Factory function to create HTTP response loggers.
    
    This function creates a response logger that can either:
    - Log to a shared file (when resource_name is None)
    - Log to a resource-specific file (when resource_name is provided)
    
    Args:
        resource_name: Name of the resource (e.g., "submission", "asset").
                      If provided, logs will go to a separate file per resource.
        log_file: Custom log file path. If not provided, defaults to:
                 - "http_logs/{resource_name}.log" for resource-specific
                 - Uses the global logger for shared logging
    
    Returns:
        A callable that logs HTTP responses and returns them unmodified.
    
    Examples:
        ```python
        # Shared logging (all resources in same file)
        from .http_logger import create_http_logger
        log_http = create_http_logger()
        
        # Resource-specific logging
        log_submission = create_http_logger(resource_name="submission")
        log_asset = create_http_logger(resource_name="asset")
        
        # Custom file path
        log_custom = create_http_logger(
            resource_name="audit",
            log_file="custom_logs/audit_http.log"
        )
        ```
    """
    # Setup resource-specific logger if needed
    if resource_name:
        # Determine log file path
        if log_file is None:
            log_dir = Path("http_logs")
            log_dir.mkdir(exist_ok=True)
            log_file = str(log_dir / f"{resource_name}.log")
        
        # Add a dedicated sink for this resource (only once)
        if resource_name not in _registered_sinks:
            sink_id = logger.add(
                log_file,
                filter=lambda record: record["extra"].get("resource") == resource_name,
                format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
                rotation="10 MB",
                retention="30 days",
                compression="zip",
                enqueue=True,  # Thread-safe
            )
            _registered_sinks[resource_name] = sink_id
    
    def log_http_response(response: Response, *args, **kwargs) -> Response:
        """
        Log HTTP response details from Kobo API.
        
        Args:
            response: The Response object from requests library
            *args: Additional positional arguments (unused, required by dlt)
            **kwargs: Additional keyword arguments (unused, required by dlt)
        
        Returns:
            Response: The unmodified response object (required by dlt)
        """
        status_code = response.status_code
        method = response.request.method
        url = response.request.url
        reason = response.reason
        
        # Build log message
        prefix = f"{resource_name.upper()} | " if resource_name else "Kobo API | "
        message = f"{prefix}{method} {url} → {status_code} {reason}"
        
        # Determine log level and suffix based on status code
        if 100 <= status_code < 200:
            log_level = "INFO"
            suffix = " (Informational)"
        elif 200 <= status_code < 300:
            log_level = "SUCCESS"
            suffix = ""
        elif 300 <= status_code < 400:
            log_level = "INFO"
            suffix = " (Redirect)"
        elif 400 <= status_code < 500:
            log_level = "WARNING"
            suffix = " (Client Error)"
        else:  # 500+
            log_level = "ERROR"
            suffix = " (Server Error)"
        
        # Log with resource tag if specified
        if resource_name:
            logger.bind(resource=resource_name).opt(depth=1).log(
                log_level, message + suffix
            )
        else:
            logger.opt(depth=1).log(log_level, message + suffix)
        
        return response
    
    return log_http_response


# Default shared logger (backward compatibility)
log_http_response = create_http_logger()

