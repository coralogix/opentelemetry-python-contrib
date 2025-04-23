"""
This module provides functionality for creating OpenTelemetry spans for EventBridge events.
"""

import json
from typing import List, Tuple

from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import set_span_in_context, Span, SpanKind, span, Link
from opentelemetry.context.context import Context
from opentelemetry.propagate import get_global_textmap
from opentelemetry.trace.propagation import get_current_span

from opentelemetry.instrumentation.aws_lambda.coralogix.span import CoralogixSpan
from opentelemetry.instrumentation.aws_lambda.coralogix import attributes as cx_attributes

class EventBridgeSpan(CoralogixSpan):
    """
    Span for EventBridge events.
    """

    def is_applicable(self) -> bool:
        if not self.lambda_event.get("source"):
            return False

        if not isinstance(self.lambda_event.get("source"), str):
            return False

        return True

    def before_run(self) -> Tuple[Span, Context]:
        # Get span name
        span_name = 'EventBridge event'
        if self.lambda_event.get("detail-type"):
            span_name = str(self.lambda_event.get("detail-type"))

        links: List[Link] = []
        if self.lambda_event.get("detail") and self.lambda_event["detail"].get("_context"):
            ctx = get_global_textmap().extract(carrier=self.lambda_event["detail"].get("_context"))
            span_ctx = get_current_span(ctx).get_span_context()
            if span_ctx.span_id != span.INVALID_SPAN_ID:
                links.append(Link(span_ctx))

        # Create the span
        self.generated_span = self.tracer.start_span(
            span_name,
            context=self.parent_context,
            kind=SpanKind.CONSUMER,
            links=links
        )
        self.generated_span.set_attribute(
            SpanAttributes.FAAS_TRIGGER, "pubsub"
        )
        self.generated_span.set_attribute(
            cx_attributes.FAAS_TRIGGER_TYPE, "EventBridge"
        )
        self.generated_span.set_attribute(
            cx_attributes.AWS_EVENT_BRIDGE_TRIGGER_SOURCE,
            str(self.lambda_event.get("source"))
        )

        self.set_rpc_request_body(json.dumps(self.lambda_event))

        return self.generated_span, set_span_in_context(self.generated_span)
