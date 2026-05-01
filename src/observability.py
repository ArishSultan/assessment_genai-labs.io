from __future__ import annotations

import sys
import time
import logging
import functools
import structlog

from typing import Callable, TypeVar, ParamSpec
from pathlib import Path

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter, SimpleSpanProcessor

from prometheus_client import CollectorRegistry, Counter, Histogram, write_to_textfile

try:
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

    _OTLP_AVAILABLE = True
except ImportError:
    _OTLP_AVAILABLE = False

P = ParamSpec("P")
R = TypeVar("R")


class _Metrics:
    def __init__(self) -> None:
        self.registry = CollectorRegistry()

        self.pipeline_requests = Counter(
            "pipeline_requests_total",
            "Pipeline runs by final status.",
            ["status"],
            registry=self.registry,
        )
        self.stage_duration = Histogram(
            "pipeline_stage_duration_seconds",
            "Wall-clock time per pipeline stage.",
            ["stage"],
            buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0],
            registry=self.registry,
        )
        self.llm_tokens = Counter(
            "llm_tokens_total",
            "LLM tokens consumed by type.",
            ["type"],
            registry=self.registry,
        )
        self.llm_calls = Counter(
            "llm_calls_total",
            "LLM API calls made.",
            registry=self.registry,
        )
        self.sql_rows_returned = Histogram(
            "sql_rows_returned",
            "Rows returned per SQL execution.",
            buckets=[0, 1, 5, 10, 25, 50, 100],
            registry=self.registry,
        )
        self._path: Path | None = None

    def flush(self) -> None:
        if self._path:
            write_to_textfile(str(self._path), self.registry)

    def record_llm_stats(self, stats: dict) -> None:
        self.llm_calls.inc(stats.get("llm_calls", 0))
        self.llm_tokens.labels(type="prompt").inc(stats.get("prompt_tokens", 0))
        self.llm_tokens.labels(type="completion").inc(stats.get("completion_tokens", 0))
        self.llm_tokens.labels(type="total").inc(stats.get("total_tokens", 0))


METRICS = _Metrics()


class _OtelContextProcessor:
    def __call__(self, _logger, _method, event_dict: dict) -> dict:
        ctx = trace.get_current_span().get_span_context()
        if ctx and ctx.is_valid:
            event_dict["trace_id"] = format(ctx.trace_id, "032x")
            event_dict["span_id"] = format(ctx.span_id, "016x")
        return event_dict


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)


def _configure_structlog(log_level: str, log_format: str) -> None:
    level = getattr(logging, log_level.upper(), logging.INFO)
    shared = [
        _OtelContextProcessor(),
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]
    renderer = (
        structlog.dev.ConsoleRenderer(colors=True)
        if log_format == "pretty"
        else structlog.processors.JSONRenderer()
    )
    structlog.configure(
        processors=shared + [structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(processor=renderer, foreign_pre_chain=shared)
    )
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)


def _configure_tracing(service_name: str, otlp_endpoint: str | None) -> None:
    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    if otlp_endpoint and _OTLP_AVAILABLE:
        provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True))
        )
    else:
        provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))
    trace.set_tracer_provider(provider)


def _tracer() -> trace.Tracer:
    return trace.get_tracer("analytics_pipeline")


def instrument(stage: str) -> Callable[[Callable[P, R]], Callable[P, R]]:
    def decorator(fn: Callable[P, R]) -> Callable[P, R]:
        log = get_logger(fn.__module__ or __name__)

        @functools.wraps(fn)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            log.debug(f"{stage}.started")
            t0 = time.perf_counter()

            with _tracer().start_as_current_span(stage) as span:
                try:
                    result = fn(*args, **kwargs)
                except Exception as exc:
                    span.record_exception(exc)
                    span.set_status(trace.StatusCode.ERROR, str(exc))
                    log.error(f"{stage}.failed", error=str(exc), exc_info=True)
                    raise

            elapsed_ms = (time.perf_counter() - t0) * 1000
            METRICS.stage_duration.labels(stage=stage).observe(elapsed_ms / 1000)
            log.debug(f"{stage}.finished", duration_ms=round(elapsed_ms, 1))
            return result

        return wrapper

    return decorator


_CONFIGURED = False


def setup_observability(
        service_name: str = "analytics-pipeline",
        log_level: str = "INFO",
        log_format: str = "pretty",
        metrics_path: str | Path | None = "metrics.prom",
        otlp_endpoint: str | None = None,
        force: bool = False,
) -> None:
    global _CONFIGURED
    if _CONFIGURED and not force:
        return

    _configure_structlog(log_level, log_format)
    _configure_tracing(service_name, otlp_endpoint)
    METRICS._path = Path(metrics_path) if metrics_path else None
    _CONFIGURED = True

    get_logger(__name__).info(
        "observability.ready",
        log_format=log_format,
        log_level=log_level,
        metrics_path=str(metrics_path) if metrics_path else None,
        tracing=otlp_endpoint or "console",
    )
