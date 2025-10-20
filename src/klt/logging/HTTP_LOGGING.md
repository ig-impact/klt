# HTTP Response Logging for Kobo Resources

## Overview

HTTP logging system for tracking all API requests to KoboToolbox server. Supports two operational modes:

1. **Resource-specific logging** (default): Each resource writes to its own log file in `http_logs/`
2. **Shared logging**: All resources write to `dlt_loguru.log` (NOT RECOMMENDED - pollutes DLT logs)

**Important**: HTTP logs are **isolated** from DLT logs. They will NOT appear in `dlt_loguru.log` or terminal output when using resource-specific logging.

## Current Configuration: Resource-Specific Logging

### Log File Structure

```
http_logs/
├── project_view.log
├── asset.log
├── asset_content.log
├── submission.log
└── audit.log
```

### Log Format

```
2025-10-20 15:30:45 | SUCCESS  | SUBMISSION | GET https://kobo.../api/v2/assets/.../data/ → 200 OK
2025-10-20 15:30:46 | WARNING  | ASSET | GET https://kobo.../api/v2/assets → 400 Bad Request (Client Error)
2025-10-20 15:30:47 | ERROR    | AUDIT | POST https://kobo.../attachments → 500 Internal Server Error (Server Error)
```

### Log Levels by HTTP Status Code

| Status Code | Log Level | Description |
|-------------|-----------|-------------|
| 1xx | INFO | Informational responses |
| 2xx | SUCCESS | Successful requests |
| 3xx | INFO | Redirections |
| 4xx | WARNING | Client errors |
| 5xx | ERROR | Server errors |

## Configuration

### Option 1: Resource-Specific Logging (Current)

```python
from klt.logging import create_http_logger

# Create resource-specific logger
log_submission_http = create_http_logger(resource_name="submission")

resource: EndpointResource = {
    "endpoint": {
        "response_actions": [
            log_submission_http,  # Logs to http_logs/submission.log
            {"status_code": 400, "action": "ignore"},
        ],
    },
}
```

### Option 2: Shared Logging (NOT RECOMMENDED)

**Warning**: This mode will mix HTTP logs with DLT logs in `dlt_loguru.log`, making it harder to troubleshoot.

```python
from klt.logging import log_http_response  # Default shared logger

resource: EndpointResource = {
    "endpoint": {
        "response_actions": [
            log_http_response,  # Logs to dlt_loguru.log (mixed with DLT logs)
            {"status_code": 400, "action": "ignore"},
        ],
    },
}
```

### Option 3: Custom Log File Path

```python
from klt.logging import create_http_logger

log_custom = create_http_logger(
    resource_name="submission",
    log_file="custom_logs/my_submission.log"
)
```

## Switching Between Modes

### Switch to Shared Logging

In **all resource files**, replace:

```python
# Before
from klt.logging import create_http_logger
log_submission_http = create_http_logger(resource_name="submission")

# After
from klt.logging import log_http_response
```

And in `response_actions`:

```python
# Before
"response_actions": [log_submission_http, ...]

# After
"response_actions": [log_http_response, ...]
```

### Revert to Resource-Specific Logging

Reverse the changes above.

## Advanced Features

### Automatic Log Rotation

- **Max size**: 10 MB per file
- **Retention**: 30 days
- **Compression**: Archived logs are compressed to `.zip`
- **Thread-safe**: Uses `enqueue=True` for concurrent operations

### Log Filtering

Each logger uses filters to ensure only resource-specific logs are written to corresponding files, preventing cross-contamination.

## Example Output

**http_logs/submission.log**:
```
2025-10-20 10:15:23 | SUCCESS  | SUBMISSION | GET https://kobo.../data/ → 200 OK
2025-10-20 10:15:24 | SUCCESS  | SUBMISSION | GET https://kobo.../data/ → 200 OK
2025-10-20 10:15:25 | WARNING  | SUBMISSION | GET https://kobo.../data/ → 400 Bad Request (Client Error)
```

**http_logs/asset.log**:
```
2025-10-20 10:15:20 | SUCCESS  | ASSET | GET https://kobo.../assets → 200 OK
2025-10-20 10:15:21 | SUCCESS  | ASSET | GET https://kobo.../assets → 200 OK
```

## Resource-Specific Logging Benefits

- **Isolation**: Easy troubleshooting for specific resources
- **Performance**: Smaller files, faster searches
- **Analytics**: Per-resource statistics (error rates, etc.)
- **Debugging**: Focus on single resource without noise
- **Rotation**: Independent size management per resource

## Shared Logging Benefits

- **Global view**: All logs in single file
- **Chronology**: Exact inter-resource request ordering
- **Simplicity**: Single file to monitor

## Log Isolation

### How It Works

The logging system uses **filters** to ensure clean separation:

- **DLT logs**: Go to `dlt_loguru.log` and terminal (stderr)
- **HTTP logs**: Go ONLY to `http_logs/*.log` (not in terminal or `dlt_loguru.log`)

This is achieved using loguru's `extra` field:
- HTTP logs have `extra["resource"] = "submission"` (or other resource name)
- DLT logs don't have this field
- Filters check for the presence of `extra["resource"]` to route logs correctly

### Terminal Output

You will see in your terminal:
- ✅ DLT pipeline logs (extraction, normalization, loading)
- ❌ HTTP response logs (silent - check `http_logs/*.log` files)

This keeps your terminal clean and focused on pipeline operations.

## Troubleshooting

### Empty Log Files

If a log file is empty, verify the resource is loaded in the pipeline:

```python
# In kobotoolbox_pipeline.py
resources = [
    res_project_view(selected=False),
    res_asset_content(parallelized=False),  # Ensure not commented out
    ...
]
```

### Log Duplication

**If HTTP logs appear in both `http_logs/` and `dlt_loguru.log`**:

1. Verify you're using the latest `dlt_logger.py` with filters:
   ```python
   logger.add(
       "dlt_loguru.log",
       filter=lambda record: "resource" not in record["extra"],  # Key line!
   )
   ```

2. Check that `dlt_logger.py` is imported BEFORE creating HTTP loggers:
   ```python
   # In logging/__init__.py
   from .dlt_logger import logger, logger_dlt  # Import first!
   from .http_logger import create_http_logger, log_http_response
   ```

3. Restart your Python process to clear old logger configurations.

### HTTP Logs Not Appearing

If `http_logs/*.log` files are empty:

1. Verify the resource is loaded in the pipeline (see "Empty Log Files" section above)
2. Check that `http_logs/` directory exists (created automatically on first run)
3. Verify the logger is called in `response_actions`:
   ```python
   "response_actions": [log_submission_http, ...]  # Must be first!
   ```


## Module Location

This logging system is located in `src/klt/logging/`:
- `http_logger.py`: HTTP response logging implementation
- `dlt_logger.py`: DLT logging configuration
- `__init__.py`: Public API exports
