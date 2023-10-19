import os
import logging

logger = logging.getLogger(__name__)

payload_size_limit = 51200
try:
    payload_size_limit = int(os.environ.get("OTEL_PAYLOAD_SIZE_LIMIT", 51200))
except ValueError:
    logger.error(
        "OTEL_PAYLOAD_SIZE_LIMIT is not a number"
    )
    
def get_payload_size_limit() -> int:
    return payload_size_limit
def limit_string_size(s: str) -> str:
    if len(s) > payload_size_limit:
        return s[:payload_size_limit]
    else:
        return s