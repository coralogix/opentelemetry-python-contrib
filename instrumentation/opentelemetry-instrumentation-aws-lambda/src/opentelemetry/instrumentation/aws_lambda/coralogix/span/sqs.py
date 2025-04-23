"""
This module provides functionality for creating OpenTelemetry spans for SQS events.
"""

from typing import List, Mapping, Optional, Tuple

from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import set_span_in_context, Span, SpanKind, Link, span
from opentelemetry.context.context import Context
from opentelemetry.propagate import get_global_textmap
from opentelemetry.trace.propagation import get_current_span
from opentelemetry.propagators import textmap
from opentelemetry.instrumentation.aws_lambda.utils import limit_string_size

from opentelemetry.instrumentation.aws_lambda.coralogix.span import CoralogixSpan
from opentelemetry.instrumentation.aws_lambda.coralogix import attributes as cx_attributes

class SQSSpan(CoralogixSpan):
    """
    Span for SQS events.
    """

    def is_applicable(self) -> bool:
        records = self.lambda_event.get("Records")
        if not records:
            return False

        if not records[0].get("eventSource"):
            return False
        return "aws:sqs" in records[0].get("eventSource")

    def before_run(self) -> Tuple[Span, Context]:
        links: List[Link] = []
        queue_url = ""

        records = self.lambda_event.get("Records")
        for record in records:  # type: ignore[index]
            if queue_url == "":
                queue_url = record.get("eventSourceARN")

            attributes = record.get("messageAttributes")
            if attributes is not None:
                ctx = get_global_textmap().extract(carrier=attributes, getter=SQSGetter())  # type: ignore[arg-type]
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
        self.generated_span.set_attribute(SpanAttributes.FAAS_TRIGGER, "pubsub")
        self.generated_span.set_attribute(cx_attributes.FAAS_TRIGGER_TYPE, "SQS")
        self.generated_span.set_attribute(SpanAttributes.MESSAGING_SYSTEM, "aws.sqs")
        self.generated_span.set_attribute(SpanAttributes.MESSAGING_URL, queue_url)

        if records:
            first_record = records[0]
            body = limit_string_size(first_record.get("body"))
            self.generated_span.set_attribute(cx_attributes.MESSAGING_MESSAGE, body)
            self.set_rpc_request_body(body)

        dest = SQSSpan.get_queue_url(queue_url)
        if dest:
            self.generated_span.set_attribute(SpanAttributes.MESSAGING_DESTINATION, dest)

        return self.generated_span, set_span_in_context(self.generated_span)


class SQSGetter():
    """
    Getter implementation to retrieve a value from a dictionary.
    """

    def get(
        self, carrier: Mapping[str, textmap.CarrierValT], key: str
    ) -> Optional[List[str]]:
        """Getter implementation to retrieve a value from a dictionary.

        Args:
            carrier: dictionary in which to get value
            key: the key used to get the value
        Returns:
            A list with a single string with the value if it exists, else None.
        """
        val = carrier.get(key, None)
        if val is None:
            return None
        if val.get("stringValue") is not None:  # type: ignore[unknown-get]
            return [val.get("stringValue")]  # type: ignore[unknown-get]
        return None

    def keys(
        self, carrier: Mapping[str, textmap.CarrierValT]
    ) -> List[str]:
        """Keys implementation that returns all keys from a dictionary."""
        return list(carrier.keys())
