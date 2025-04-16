"""
This module provides functionality for creating OpenTelemetry spans for SNS events.
"""

from typing import List, Mapping, Optional, Tuple, Dict, Any

from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import set_span_in_context, Span, SpanKind, Link, span
from opentelemetry.context.context import Context
from opentelemetry.propagate import get_global_textmap
from opentelemetry.trace.propagation import get_current_span
from opentelemetry.propagators import textmap

from opentelemetry.instrumentation.aws_lambda.coralogix.span import CoralogixSpan
from opentelemetry.instrumentation.aws_lambda.coralogix import attributes as cx_attributes

class SNSSpan(CoralogixSpan):
    """
    Span for SNS events.
    """

    def is_applicable(self) -> bool:
        records = self.lambda_event.get("Records")
        if not records:
            return False

        if not records[0].get("EventSource"):
            return False

        return records[0].get("EventSource") == "aws:sns"

    def get_child_span_type(self) -> SpanKind:
        return SpanKind.INTERNAL

    def before_run(self) -> Tuple[Span, Context]:
        links: List[Link] = []
        queue_url = ""
        records = self.lambda_event.get("Records")

        for record in records:  # type: ignore[index]
            if record.get("Sns") is None:
                continue

            if queue_url == "":
                queue_url = record.get("Sns").get("TopicArn")

            attributes = record.get("Sns").get("MessageAttributes")
            if attributes is not None:
                ctx = get_global_textmap().extract(carrier=attributes, getter=SNSGetter())  # type: ignore[arg-type]
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
        self.generated_span.set_attribute(cx_attributes.FAAS_TRIGGER_TYPE, "SNS")
        self.generated_span.set_attribute(SpanAttributes.MESSAGING_SYSTEM, "aws.sns")
        self.generated_span.set_attribute(SpanAttributes.MESSAGING_URL, queue_url)

        dest = SNSSpan.get_queue_url(queue_url)
        if dest:
            self.generated_span.set_attribute(SpanAttributes.MESSAGING_DESTINATION, dest)

        first_record: Dict[str, Any] = records[0]  # type: ignore[index]
        if first_record.get("Sns") and first_record["Sns"].get("Message"):  # type: ignore[unknown-get]
            self.set_rpc_request_body(first_record["Sns"].get("Message"))  # type: ignore[unknown-get]
        return self.generated_span, set_span_in_context(self.generated_span)


class SNSGetter():
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
        if val.get("Value") is not None:  # type: ignore[unknown-get]
            return [val.get("Value")]  # type: ignore[unknown-get]
        return None

    def keys(self, carrier: Mapping[str, textmap.CarrierValT]) -> List[str]:
        """Keys implementation that returns all keys from a dictionary."""
        return list(carrier.keys())
