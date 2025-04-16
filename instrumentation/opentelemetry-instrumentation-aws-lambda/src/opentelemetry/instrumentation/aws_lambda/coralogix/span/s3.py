"""
This module provides functionality for creating OpenTelemetry spans for S3 events.
"""

import json
from typing import Tuple, Dict, Any

from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import set_span_in_context, Span, SpanKind
from opentelemetry.context.context import Context

from opentelemetry.instrumentation.aws_lambda.coralogix.span import CoralogixSpan
from opentelemetry.instrumentation.aws_lambda.coralogix import attributes as cx_attributes

class S3Span(CoralogixSpan):
    """
    Span for S3 events.
    """

    def is_applicable(self) -> bool:
        records = self.lambda_event.get("Records")
        if not records:
            return False

        if not records[0].get("eventSource"):
            return False

        return records[0].get("eventSource") == "aws:s3"

    def before_run(self) -> Tuple[Span, Context]:
        # Get span name
        records = self.lambda_event.get("Records")
        first_record: Dict[str, Any] = records[0]  # type: ignore[index]
        span_name = self.orig_handler_name
        if first_record.get("eventName"):  # type: ignore[unknown-get]
            span_name = str(first_record.get("eventName"))  # type: ignore[unknown-get]

        # Create the span
        self.generated_span = self.tracer.start_span(
            span_name,
            context=self.parent_context,
            kind=SpanKind.PRODUCER
        )
        self.generated_span.set_attribute(SpanAttributes.FAAS_TRIGGER, "datasource")
        self.generated_span.set_attribute(cx_attributes.FAAS_TRIGGER_TYPE, "S3")

        if first_record.get("s3"):  # type: ignore[unknown-get]
            event = first_record.get("s3")  # type: ignore[unknown-get]
            self.set_rpc_request_body(json.dumps(event))

        return self.generated_span, set_span_in_context(self.generated_span)
