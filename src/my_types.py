from __future__ import annotations

from typing import Any, Self, Optional
from dataclasses import dataclass, field

from pydantic import BaseModel, Field, model_validator


@dataclass
class PipelineInput:
    """Input to the AnalyticsPipeline.run() method."""
    question: str
    request_id: str | None = None


@dataclass
class SQLGenerationOutput:
    """Output from the SQL generation stage.

    For complex solutions with multiple LLM calls (chain-of-thought, planning,
    query refinement), populate intermediate_outputs with per-call details.
    llm_stats aggregates all calls for efficient evaluation.
    """
    sql: str | None
    timing_ms: float
    llm_stats: dict[str, Any]  # Aggregated: {llm_calls, prompt_tokens, completion_tokens, total_tokens, model}
    intermediate_outputs: list[dict[str, Any]] = field(default_factory=list)
    error: str | None = None


@dataclass
class SQLValidationOutput:
    """Output from the SQL validation stage."""
    is_valid: bool
    validated_sql: str | None
    error: str | None = None
    timing_ms: float = 0.0


@dataclass
class SQLExecutionOutput:
    """Output from the SQL execution stage."""
    rows: list[dict[Any, Any]]
    row_count: int
    timing_ms: float
    error: str | None = None


@dataclass
class AnswerGenerationOutput:
    """Output from the answer generation stage.

    For complex solutions with multiple LLM calls (summarization, verification),
    populate intermediate_outputs with per-call details.
    llm_stats aggregates all calls for efficient evaluation.
    """
    answer: str
    timing_ms: float
    llm_stats: dict[str, Any]  # Aggregated: {llm_calls, prompt_tokens, completion_tokens, total_tokens, model}
    intermediate_outputs: list[dict[str, Any]] = field(default_factory=list)
    error: str | None = None


@dataclass
class PipelineOutput:
    """Complete output from AnalyticsPipeline.run()."""
    # Status
    status: str  # "success" | "unanswerable" | "invalid_sql" | "error"
    question: str
    request_id: str | None

    # Stage outputs (for evaluation)
    sql_generation: SQLGenerationOutput
    sql_validation: SQLValidationOutput
    sql_execution: SQLExecutionOutput
    answer_generation: AnswerGenerationOutput

    # Convenience fields
    sql: str | None = None
    rows: list[dict[str, Any]] = field(default_factory=list)
    answer: str = ""

    # Aggregates
    timings: dict[str, float] = field(default_factory=dict)
    total_llm_stats: dict[str, Any] = field(default_factory=dict)


class SQLResponse(BaseModel):
    sql: str = Field(
        description=(
            "A SQLite statement using only the columns from the provided schema. "
            "set it to `INVALID` if query is not possible."
        ),
    )
    reason: str = Field(
        description="Brief explanation of why the question cannot be answered.",
    )

    @model_validator(mode="after")
    def _validate_response_content(self) -> Self:
        sql_clean = self.sql.strip() if self.sql else ""
        reason_clean = self.reason.strip() if self.reason else ""

        if not sql_clean and not reason_clean:
            raise ValueError("Set the sql to `INVALID` if the answer is not possible but do not provide both sql and reason.")

        if sql_clean:
            self.sql = sql_clean
        else:
            self.reason = reason_clean

        return self
