"""OpenTelemetry bootstrap shared by every Python service.

Traces go out over OTLP/gRPC to the OpenTelemetry Collector (OTEL_EXPORTER_OTLP_ENDPOINT).
If the endpoint is not set, tracing stays a no-op, so the services still run
standalone without a collector.

Kafka has no auto-instrumentation here on purpose: the trace context is
carried in message headers by `inject_kafka_headers` / `extract_kafka_context`,
which keeps one trace spanning webhook -> Kafka -> consumer -> Kafka -> worker -> ERP.
"""
import logging
import os
from collections.abc import Iterable

from opentelemetry import context, propagate, trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

KafkaHeaders = list[tuple[str, bytes]]


def setup_tracing(service_name: str) -> trace.Tracer:
    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
    if endpoint:
        provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=True)))
        trace.set_tracer_provider(provider)
        # Stamp trace/span ids on every log record so a log line can be
        # matched to its trace (use %(otelTraceID)s in the log format).
        from opentelemetry.instrumentation.logging import LoggingInstrumentor

        LoggingInstrumentor().instrument(set_logging_format=False)
    else:
        logging.getLogger(service_name).info("OTEL_EXPORTER_OTLP_ENDPOINT not set — tracing disabled")
    return trace.get_tracer(service_name)


def inject_kafka_headers() -> KafkaHeaders:
    """Headers carrying the current span's context, ready for producer.send(headers=...)."""
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return [(k, v.encode()) for k, v in carrier.items()]


def extract_kafka_context(headers: Iterable[tuple[str, bytes]] | None) -> context.Context:
    carrier = {k: v.decode() for k, v in (headers or [])}
    return propagate.extract(carrier)
