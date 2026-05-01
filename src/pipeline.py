from __future__ import annotations

import hashlib
import time

from opentelemetry import trace

from src.config import SETTINGS
from src.schema import SchemaCache
from src.executor import SQLiteExecutor
from src.my_types import PipelineOutput
from src.observability import METRICS, get_logger, setup_observability
from src.sql_validator import SQLValidator
from src.llm_client import OpenRouterLLMClient, build_default_llm_client

log = get_logger(__name__)


class AnalyticsPipeline:
    def __init__(
            self,
            db_path=SETTINGS.db_path,
            llm_client: OpenRouterLLMClient | None = None,
    ) -> None:
        setup_observability()
        self.llm = llm_client or build_default_llm_client()
        self.executor = SQLiteExecutor(db_path)
        self.schema_cache = SchemaCache(SETTINGS.schema_path, SETTINGS.table)
        self.validator = SQLValidator(self.schema_cache.schema)

    def run(self, question: str, request_id: str | None = None) -> PipelineOutput:
        question_hash = hashlib.sha256((question or "").encode("utf-8")).hexdigest()[:8]
        pipeline_log = log.bind(request_id=request_id, question_hash=question_hash)
        pipeline_log.info("pipeline.started")
        start_ns = time.perf_counter_ns()

        tracer = trace.get_tracer(__name__)
        with tracer.start_as_current_span("pipeline.run") as root:
            root.set_attribute("request_id", request_id or "")
            root.set_attribute("question_hash", question_hash)

            sql_gen_output = self.llm.generate_sql(question, self.schema_cache)

            with tracer.start_as_current_span("pipeline.validate_sql") as vspan:
                validation_output = self.validator.validate(sql_gen_output.sql)
                vspan.set_attribute("is_valid", validation_output.is_valid)
                if not validation_output.is_valid and validation_output.error:
                    vspan.set_attribute("error", validation_output.error)
            METRICS.stage_duration.labels(stage="validation").observe(
                validation_output.timing_ms / 1000
            )

            execution_output = self.executor.run(validation_output.validated_sql)
            answer_output = self.llm.generate_answer(
                question, validation_output.validated_sql, execution_output.rows
            )

            status = self._resolve_status(sql_gen_output, validation_output, execution_output)

            duration_ns = time.perf_counter_ns() - start_ns
            total_ms = duration_ns / 1_000_000

            METRICS.pipeline_requests.labels(status=status).inc()
            METRICS.stage_duration.labels(stage="total").observe(duration_ns / 1_000_000_000)

            METRICS.record_llm_stats(_merge_llm_stats(sql_gen_output.llm_stats, answer_output.llm_stats))
            METRICS.flush()

            root.set_attribute("status", status)
            pipeline_log.info("pipeline.completed", status=status, total_ms=round(total_ms, 1))

        return PipelineOutput(
            status=status,
            question=question,
            request_id=request_id,
            sql_generation=sql_gen_output,
            sql_validation=validation_output,
            sql_execution=execution_output,
            answer_generation=answer_output,
            sql=validation_output.validated_sql,
            rows=execution_output.rows,
            answer=answer_output.answer,
            timings={
                "sql_generation_ms": sql_gen_output.timing_ms,
                "sql_validation_ms": validation_output.timing_ms,
                "sql_execution_ms": execution_output.timing_ms,
                "answer_generation_ms": answer_output.timing_ms,
                "total_ms": total_ms,
            },
            total_llm_stats=_merge_llm_stats(sql_gen_output.llm_stats, answer_output.llm_stats),
        )

    @staticmethod
    def _resolve_status(sql_gen, validation, execution) -> str:
        if sql_gen.sql is None and sql_gen.error:
            return "unanswerable"
        if not validation.is_valid:
            return "invalid_sql"
        if execution.error:
            return "error"
        if validation.validated_sql is None:
            return "unanswerable"
        return "success"


def _merge_llm_stats(*stats_dicts: dict) -> dict:
    merged: dict = {"llm_calls": 0, "prompt_tokens": 0,
                    "completion_tokens": 0, "total_tokens": 0, "model": "unknown"}
    for s in stats_dicts:
        for key in ("llm_calls", "prompt_tokens", "completion_tokens", "total_tokens"):
            merged[key] += s.get(key, 0)
        merged["model"] = s.get("model", merged["model"])
    return merged
