# HTTP Response Logging for Kobo Resources

## Overview

HTTP logging system for tracking all API requests to KoboToolbox server. Supports two operational modes:

1. **Resource-specific logging** (default): Each resource writes to its own log file
2. **Shared logging**: All resources write to a single global log file

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
from .http_logger import create_http_logger

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

### Option 2: Shared Logging

```python
from .http_logger import log_http_response  # Default shared logger

resource: EndpointResource = {
    "endpoint": {
        "response_actions": [
            log_http_response,  # Logs to dlt_loguru.log
            {"status_code": 400, "action": "ignore"},
        ],
    },
}
```

### Option 3: Custom Log File Path

```python
from .http_logger import create_http_logger

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
from .http_logger import create_http_logger
log_submission_http = create_http_logger(resource_name="submission")

# After
from .http_logger import log_http_response
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

If logs appear in both `http_logs/` and `dlt_loguru.log`, ensure you're using the latest version of `http_logger.py` with proper sink registration and filtering.
  
