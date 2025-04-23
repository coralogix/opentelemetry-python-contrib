
"""
This module provides functionality for creating OpenTelemetry spans for Kinesis events.
"""

from typing import List, Tuple, Dict, Any
import base64
import json

from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import set_span_in_context, Span, SpanKind
from opentelemetry.context.context import Context
from opentelemetry.trace import Link, span
from opentelemetry.propagate import get_global_textmap
from opentelemetry.trace.propagation import get_current_span

from opentelemetry.instrumentation.aws_lambda.coralogix.span import CoralogixSpan
from opentelemetry.instrumentation.aws_lambda.coralogix import attributes as cx_attributes

class KinesisSpan(CoralogixSpan):
    """
    Span for Kinesis events.
    """

    def is_applicable(self) -> bool:
        records = self.lambda_event.get("Records")
        if not records:
            return False

        if not records[0].get("eventSource"):
            return False

        return records[0].get("eventSource") == "aws:kinesis"

    def get_child_span_type(self) -> SpanKind:
        return SpanKind.INTERNAL

    def before_run(self) -> Tuple[Span, Context]:
        links: List[Link] = []
        queue_url = ""

        records = self.lambda_event.get("Records")

        for record in records:  # type: ignore[index]
            if record.get("kinesis") is None:
                continue

            if queue_url == "":
                queue_url = record.get("eventSourceARN")

            data = record["kinesis"].get("data")
            if data is not None:
                decoded_bytes = base64.b64decode(data)
                decoded_string = decoded_bytes.decode('utf-8')
                data = json.loads(decoded_string)
                ctx = get_global_textmap().extract(carrier=data.get("_context"))
                span_ctx = get_current_span(ctx).get_span_context()
                if span_ctx.span_id != span.INVALID_SPAN_ID:
                    links.append(Link(span_ctx))

        # Get span name
        span_name = self.orig_handler_name

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
            cx_attributes.FAAS_TRIGGER_TYPE, "Kinesis"
        )
        self.generated_span.set_attribute(
            SpanAttributes.MESSAGING_SYSTEM, "aws.kinesis"
        )
        self.generated_span.set_attribute(
            SpanAttributes.MESSAGING_URL, queue_url
        )

        dest = KinesisSpan.get_queue_url(queue_url)
        if dest:
            self.generated_span.set_attribute(SpanAttributes.MESSAGING_DESTINATION, dest)

        first_record: Dict[str, Any] = records[0]  # type: ignore[index]
        if first_record.get("kinesis") and first_record["kinesis"].get("data"):  # type: ignore[unknown-get]
            decoded_bytes = base64.b64decode(first_record["kinesis"].get("data"))  # type: ignore[unknown-get]
            decoded_string = decoded_bytes.decode('utf-8')
            body = json.loads(decoded_string)

            self.set_rpc_request_body(body)

        return self.generated_span, set_span_in_context(self.generated_span)

    def after_run(self, result: Dict[str, Any]) -> None:
        if result.get("ResponseMetadata") and result.get("ResponseMetadata").get("HTTPStatusCode"):  # type: ignore[unknown-get]
            self.generated_span.set_attribute(
                SpanAttributes.HTTP_STATUS_CODE,
                result.get("ResponseMetadata").get("HTTPStatusCode")  # type: ignore[unknown-get]
            )
