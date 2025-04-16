"""
This module provides functionality for creating OpenTelemetry spans for Cognito events.
"""
import json
from typing import Tuple

from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import set_span_in_context, Span, SpanKind
from opentelemetry.context.context import Context
from opentelemetry.instrumentation.aws_lambda.coralogix.span import CoralogixSpan
from opentelemetry.instrumentation.aws_lambda.coralogix import attributes as cx_attributes

class CognitoSpan(CoralogixSpan):
    """
    Span for Cognito events.
    """

    def is_applicable(self) -> bool:
        if not self.lambda_event.get("eventType"):
            return False

        if self.lambda_event.get("eventType") != "SyncTrigger":
            return False

        return True

    def before_run(self) -> Tuple[Span, Context]:
        # Get span name
        span_name = "SyncTrigger"

        self.generated_span = self.tracer.start_span(
            span_name,
            context=self.parent_context,
            kind=SpanKind.PRODUCER
        )
        self.generated_span.set_attribute(SpanAttributes.FAAS_TRIGGER, "datasource")
        self.generated_span.set_attribute(cx_attributes.FAAS_TRIGGER_TYPE, "Cognito")

        if self.lambda_event.get("datasetRecords"):
            self.set_rpc_request_body(json.dumps(self.lambda_event["datasetRecords"]))

        return self.generated_span, set_span_in_context(self.generated_span)
