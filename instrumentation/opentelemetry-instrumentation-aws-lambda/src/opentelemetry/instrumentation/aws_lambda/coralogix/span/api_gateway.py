"""
This module provides functionality for creating OpenTelemetry spans for API Gateway events.
"""

from typing import Dict, Tuple, Any

from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import set_span_in_context, Span, SpanKind
from opentelemetry.context.context import Context
from opentelemetry.instrumentation.aws_lambda.utils import limit_string_size
from opentelemetry.instrumentation.aws_lambda.coralogix.span import CoralogixSpan
from opentelemetry.instrumentation.aws_lambda.coralogix import attributes as cx_attributes

class APIGatewaySpan(CoralogixSpan):
    """
    Span for API Gateway events.
    """

    def is_applicable(self) -> bool:
        # If the request came from an API Gateway, extract http attributes from the event
        # https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/instrumentation/aws-lambda.md#api-gateway
        # https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/http.md#http-server-semantic-conventions
        if not self.lambda_event.get("requestContext"):
            return False

        return True

    def before_run(self) -> Tuple[Span, Context]:
        # Get the span name
        span_name = self.orig_handler_name
        path = ""
        if self.lambda_event.get(
            "requestContext"
        ) and self.lambda_event["requestContext"].get("http"):
            span_name = str(self.lambda_event["requestContext"]["http"].get("path"))
            path = span_name
        elif self.lambda_event.get("resource"):
            span_name = str(self.lambda_event.get("resource"))

        # Create the span
        self.generated_span = self.tracer.start_span(
            span_name,
            context=self.parent_context,
            kind=SpanKind.CLIENT
        )
        if self.lambda_event.get("version") == "2.0":
            self.generated_span.set_attribute(cx_attributes.FAAS_TRIGGER_TYPE, "Api Gateway Rest")
        else:
            self.generated_span.set_attribute(cx_attributes.FAAS_TRIGGER_TYPE, "Api Gateway HTTP")

        self.generated_span.set_attribute(SpanAttributes.FAAS_TRIGGER, "http")

        if self.lambda_event.get("headers"):
            for key, value in self.lambda_event["headers"].items():
                header_name = cx_attributes.HTTP_HEADER_PREFIX + key.lower().replace("-", "_")
                self.generated_span.set_attribute(header_name, value)

        if path:
            self.generated_span.set_attribute(
                SpanAttributes.HTTP_URL, path
            )

        if self.lambda_event.get("requestContext") and (
            self.lambda_event["requestContext"].get("domainName") and path
        ):
            domain_name = self.lambda_event["requestContext"].get("domainName")
            self.generated_span.set_attribute(
                SpanAttributes.HTTP_URL, domain_name + path
            )

        return self.generated_span, set_span_in_context(self.generated_span)

    def after_run(self, result: Dict[str, Any]) -> None:
        if not isinstance(result, dict):  # type: ignore[unnecessary-isinstance-call]
            # We need to double check because sometimes
            # it can happen that the result is not a dict
            return

        if result.get("body"):
            body = limit_string_size(str(result.get("body")))
            self.generated_span.set_attribute(cx_attributes.HTTP_RESPONSE_BODY, str(body))
