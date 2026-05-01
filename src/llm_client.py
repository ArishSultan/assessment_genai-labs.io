from __future__ import annotations

import os
import time

from config import SETTINGS
from schema import SchemaCache
from typing import Any, List, Optional
from prompts import build_sql_messages, build_answer_messages
from pydantic import ValidationError
from openrouter.components import ChatResult, ChatMessages, ResponseFormat, ChatFormatJSONSchemaConfig, \
    ChatJSONSchemaConfig

from src.my_types import SQLGenerationOutput, AnswerGenerationOutput, SQLResponse

DEFAULT_MODEL = "openai/gpt-5-nano"


class OpenRouterLLMClient:
    """LLM client using the OpenRouter SDK for chat completions."""

    provider_name = "openrouter"

    def __init__(self, api_key: str, model: str | None = None) -> None:
        try:
            from openrouter import OpenRouter
        except ModuleNotFoundError as exc:
            raise RuntimeError("Missing dependency: install 'openrouter'.") from exc
        self.model = model or os.getenv("OPENROUTER_MODEL", DEFAULT_MODEL)
        self._client = OpenRouter(api_key=api_key)
        self._stats = self._empty_stats()

    def _update_stats(self, res: ChatResult) -> None:
        # Increment the llm calls after getting a response from API, regardless of valid or invalid SQL.
        self._stats["llm_calls"] += 1

        # Skip if usage is not found for some reason
        if res.usage is None:
            return

        # Add the new values to overall stats
        self._stats["prompt_tokens"] += res.usage.prompt_tokens
        self._stats["completion_tokens"] += res.usage.completion_tokens
        self._stats["total_tokens"] += res.usage.total_tokens

    def _chat(
            self,
            messages: List[ChatMessages],
            temperature: float,
            max_tokens: int,
            response_format: Optional[ResponseFormat] = None
    ) -> str:
        res = self._client.chat.send(
            messages=messages,
            model=self.model,
            temperature=temperature,
            max_tokens=max_tokens,
            response_format=response_format,
            stream=False,
        )

        self._update_stats(res)

        choices = getattr(res, "choices", None) or []
        if not choices:
            raise RuntimeError("OpenRouter response contained no choices.")
        content = getattr(getattr(choices[0], "message", None), "content", None)
        if not isinstance(content, str):
            raise RuntimeError("OpenRouter response content is not text.")
        return content.strip()

    def generate_sql(self, question: str, schema_cache: SchemaCache) -> SQLGenerationOutput:
        start_ns = time.perf_counter_ns()
        sql, error = None, None

        try:
            response_text = self._chat(
                max_tokens=240,
                temperature=0.0,
                messages=build_sql_messages(question, schema_cache.condensed_text()),
                response_format=ChatFormatJSONSchemaConfig(
                    type='json_schema',
                    json_schema=ChatJSONSchemaConfig(
                        name=SQLResponse.__name__,
                        strict=True,
                        schema_=SQLResponse.model_json_schema(),
                    )
                ),
            )

            parsed = SQLResponse.model_validate_json(response_text or "{}")

            if parsed.sql:
                sql = parsed.sql
            else:
                error = parsed.reason or "LLM provided no SQL and no reason."

        except ValidationError as exc:
            error = f"Validation failed: {exc.errors()[0]['msg']}"
        except Exception as exc:
            error = f"LLM Error: {str(exc)}"

        return SQLGenerationOutput(
            sql=sql,
            error=error,
            timing_ms=(time.perf_counter_ns() - start_ns) / 1_000_000,
            llm_stats=self._pop_stats(),
        )

    def generate_answer(
            self,
            question: str,
            sql: str | None,
            rows: list[dict[str, Any]],
    ) -> AnswerGenerationOutput:
        if not sql:
            return AnswerGenerationOutput(
                error=None,
                timing_ms=0.0,
                llm_stats=self._empty_stats(),
                answer="I cannot answer this with the available table and schema. "
                       "Please rephrase using known survey fields.",
            )
        if not rows:
            return AnswerGenerationOutput(
                answer="Query executed, but no rows were returned.",
                timing_ms=0.0,
                llm_stats=self._empty_stats(),
                error=None,
            )

        start = time.perf_counter()
        error: str | None = None
        stats = self._empty_stats()

        try:
            answer = self._chat(
                messages=build_answer_messages(
                    sql=sql,
                    rows=rows,
                    question=question,
                    row_preview=SETTINGS.answer_row_preview,
                    max_str_len=SETTINGS.answer_max_str_len,
                    max_avg_col_len=SETTINGS.answer_max_avg_col_len,
                ),
                temperature=0.2,
                max_tokens=SETTINGS.max_answer_tokens,
            )
        except Exception as exc:
            error = str(exc)
            answer = f"Error generating answer: {error}"

        timing_ms = (time.perf_counter() - start) * 1000.0
        return AnswerGenerationOutput(
            answer=answer,
            timing_ms=timing_ms,
            llm_stats=stats,
            error=error,
        )

    def _pop_stats(self) -> dict[str, Any]:
        out = dict(self._stats or {})
        self._stats = self._empty_stats()
        return out

    def _empty_stats(self) -> dict[str, Any]:
        return {
            "llm_calls": 0,
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
            "model": self.model,
        }


def build_default_llm_client() -> OpenRouterLLMClient:
    api_key = os.getenv("OPENROUTER_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY is required.")
    return OpenRouterLLMClient(api_key=api_key)
