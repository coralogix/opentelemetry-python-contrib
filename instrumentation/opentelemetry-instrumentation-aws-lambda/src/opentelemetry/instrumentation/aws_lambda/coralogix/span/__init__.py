"""
This module provides functionality for creating OpenTelemetry spans for AWS Lambda functions.

It contains the base CoralogixSpan class that defines the interface for creating spans for different
AWS Lambda triggers, as well as concrete implementations for various AWS services like:

"""
from typing import Dict, Tuple, Any
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from opentelemetry.context.context import Context
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import Tracer, Span, SpanKind

from opentelemetry.instrumentation.aws_lambda.utils import limit_string_size

from opentelemetry.instrumentation.aws_lambda.coralogix import attributes as cx_attributes

@dataclass
class CoralogixSpan(ABC):
    """
    Base class for creating OpenTelemetry spans for AWS Lambda functions.

    This class defines the interface for creating spans for different AWS Lambda triggers.
    """
    tracer: Tracer
    parent_context: Context
    lambda_event: Dict[str, Any]
    orig_handler_name: str
    generated_span: Span = field(init=False, repr=False)

    @abstractmethod
    def is_applicable(self) -> bool:
        """
        Check if the span is applicable for the given AWS Lambda event.
        """

    @abstractmethod
    def before_run(self) -> Tuple[Span, Context]:
        """
        Create the span and context for the given AWS Lambda event.
        """

    def get_child_span_type(self) -> SpanKind:
        """
        Get the child span type for the given AWS Lambda event.
        """
        return None

    def set_rpc_request_body(self, body: Any) -> None:
        """
        Set the request body for the given AWS Lambda event.
        """
        body = limit_string_size(str(body))
        self.generated_span.set_attribute(cx_attributes.RPC_REQUEST_BODY, body)


    def after_run(self, result: Dict[str, Any]) -> None:
        """
        After the lamda is executed, this method is called to perform any additional actions.
        """
        if not isinstance(result, dict):  # type: ignore[unnecessary-isinstance-call]
            return

        if result.get("statusCode"):
            self.generated_span.set_attribute(
                SpanAttributes.HTTP_STATUS_CODE,
                result.get("statusCode") # type: ignore[unknown-get]
            )

        if result.get("body"):
            self.generated_span.set_attribute(
                cx_attributes.RPC_RESPONSE_BODY,
                limit_string_size(str(result.get("body")))
            )


    @staticmethod
    def get_queue_url(queue_url: str) -> str:
        """
        Extract the queue name from the queue URL.

        Example queue_url: arn:aws:sqs:us-east-1:123456789012:my_queue_name
        """
        if ":" in queue_url:
            splitted_queue_url = queue_url.split(":")
            if len(splitted_queue_url) > 0:
                queue_url = splitted_queue_url[-1]
        return queue_url
