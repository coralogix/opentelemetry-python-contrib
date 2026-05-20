# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
import os
from typing import Callable
from urllib.parse import urlparse

from opentelemetry.semconv._incubating.attributes import (
    server_attributes as ServerAttributes,
)
from opentelemetry.util.types import AttributeValue

_logger = logging.getLogger(__name__)

_DEFAULT_PAYLOAD_SIZE_LIMIT = 51200
payload_size_limit = _DEFAULT_PAYLOAD_SIZE_LIMIT
try:
    payload_size_limit = int(
        os.environ.get("OTEL_PAYLOAD_SIZE_LIMIT", _DEFAULT_PAYLOAD_SIZE_LIMIT)
    )
except ValueError:
    _logger.error("OTEL_PAYLOAD_SIZE_LIMIT is not a number")
    payload_size_limit = _DEFAULT_PAYLOAD_SIZE_LIMIT


def get_payload_size_limit() -> int:
    return payload_size_limit


def limit_string_size(value: str) -> str:
    if len(value) > payload_size_limit:
        return value[:payload_size_limit]
    return value


def get_server_attributes(endpoint_url: str) -> dict[str, AttributeValue]:
    """Extract server.* attributes from AWS endpoint URL."""
    parsed = urlparse(endpoint_url)
    attributes = {}
    if parsed.hostname:
        attributes[ServerAttributes.SERVER_ADDRESS] = parsed.hostname
        attributes[ServerAttributes.SERVER_PORT] = parsed.port or 443
    return attributes


def _safe_invoke(function: Callable, *args):
    function_name = "<unknown>"
    try:
        function_name = function.__name__
        function(*args)
    except Exception as ex:  # pylint:disable=broad-except
        _logger.error(
            "Error when invoking function '%s'", function_name, exc_info=ex
        )
