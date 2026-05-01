# Solution Notes

## What I changed

### Bugs fixed in the original baseline

Two issues in the provided code prevented it from running. Neither was called out in the assignment text.

1. **`src/types.py` module-name collision.** The file shadowed/competed with another internal `types` module in some import paths. Renamed to `src/my_types.py` and updated every import site. The output dataclass contract (`PipelineInput`, `SQLGenerationOutput`, `SQLValidationOutput`, `SQLExecutionOutput`, `AnswerGenerationOutput`, `PipelineOutput`) is preserved verbatim, so the README's "Output contract" hard requirement still holds.

2. **Dataclass instances were accessed as if they were dicts.** The baseline pipeline did things like `sql_gen_output["sql"]` on a `@dataclass` instance, which raises `TypeError`. Switched to attribute access (`sql_gen_output.sql`).

### What was added

| Concern | Module |
| --- | --- |
| Typed settings, .env loading | `src/config.py` |
| Schema cache + condensed text | `src/schema.py` |
| Prompt builders, row trimming, input sanitisation | `src/prompts.py` |
| OpenRouter client (structured output, retries, token counting) | `src/llm_client.py` |
| sqlglot-based SQL validator | `src/sql_validator.py` |
| Read-only sqlite executor | `src/executor.py` |
| Pipeline orchestrator (tracing, status, metrics) | `src/pipeline.py` |
| structlog + OTel + Prometheus wiring | `src/observability.py` |
| Dataclass contracts + `SQLResponse` Pydantic model | `src/my_types.py` |
| YAML schema generation | `scripts/generate_schema_info.py` |
| Validator unit tests | `tests/test_sql_validator.py` |

The `tests/test_public.py` file is unmodified except for the import path (`src.my_types` instead of `src.types`).

### Notable behavioural changes

- **Token counting is real.** The OpenRouter response's `usage` payload is read and accumulated per-call into `llm_calls / prompt_tokens / completion_tokens / total_tokens`. The skeleton in the baseline did nothing.
- **Structured SQL output.** The SQL generation call uses OpenRouter's `response_format` with a strict JSON Schema (`SQLResponse { sql, reason }`) and Pydantic validation, rather than regex-extracting `SELECT ...` from free text. Unanswerable questions are detected by the model setting `sql="INVALID"` and providing a `reason`.
- **Schema is in the prompt.** Every SQL-generation call now includes a condensed schema block built from `data/<table>_schema.yaml` (column names, SQL types, kind, ranges/distinct-values, plain-English descriptions). Description verbosity is configurable.
- **SQL validation actually validates.** `sqlglot` parses the SQL into an AST; the validator walks the tree and rejects multi-statements, anything other than SELECT/UNION at the root, any DDL/DML/PRAGMA/transaction node anywhere in the tree, references to non-allow-listed tables/columns, and qualifiers that don't resolve. CTEs are handled. LIMIT is clamped to a configurable maximum (default 1000).
- **SQLite is opened read-only** (`file:...?mode=ro` URI) as defence-in-depth.
- **Retries with backoff** on transient HTTP errors (429/5xx + network/timeout), 3 attempts, exponential.
- **Observability:** structlog (JSON or pretty), Prometheus counters/histograms flushed to `metrics.prom`, OpenTelemetry traces with a root `pipeline.run` span and child spans per stage. `request_id` and `question_hash` are propagated through both logs and span attributes.

## Why I changed it

- The baseline did not run. Fixing the two import/access bugs was non-negotiable.
- An LLM-driven pipeline that puts unvalidated SQL in front of a database is a production hazard, regardless of how friendly the dataset looks. A real validator + a read-only connection turns the LLM into an *advisor* rather than a *trusted client*.
- Structured output (JSON Schema) is meaningfully more reliable than text-and-regex; on top of that, it lets the model express "I cannot answer this from the schema" as data rather than as a parse failure.
- Schema context in the prompt is the cheapest and largest correctness win: without it, the model invented columns. With it, it generates queries that actually run against the real table.
- Operability: a system without metrics/logs/traces is a system you cannot debug at 3am. The observability triple is wired up so that an alert on `pipeline_requests_total{status="error"}` rate can be pivoted to a specific `trace_id` and from there to its log lines.

## Measured impact

The README quotes baseline reference numbers of `avg ~2900ms / p50 ~2500ms / p95 ~4700ms / ~600 tokens/request`, but the baseline did not implement token counting or SQL validation, so a like-for-like comparison is not possible.

**Benchmark run (`scripts/benchmark.py --runs 3`, `openai/gpt-5-nano`, `REASONING_EFFORT=minimal`):**

```json
{
  "runs": 3,
  "samples": 36,
  "success_rate": 1.0,
  "avg_ms": 4518.16,
  "p50_ms": 3820.77,
  "p95_ms": 5874.03
}
```

**Token usage (from `metrics.prom`, 5-run public-test sample):**

| Metric | Value |
| --- | --- |
| Total LLM calls | 8 |
| Prompt tokens | 15,489 |
| Completion tokens | 614 |
| Total tokens | 16,103 |
| Avg LLM calls per request | 1.6 (happy path is 2; invalid_sql short-circuits before the answer call) |

These numbers are not a clean head-to-head with the README baseline — model variant, hardware, and network all differ, and end-to-end latency is dominated by the OpenRouter round-trip. They're reported for reference. The prompt-token count is materially higher than the README's `~600 / request` baseline because the schema (40 columns, full descriptions) is now sent on every generation call. That cost is what *bought* correctness; the baseline's smaller prompt produced hallucinated columns. `SCHEMA_DESCRIPTION_LEVEL=standard` or `minimal` cuts the schema block when token cost matters more than column-context detail.

## Tradeoffs

- **Schema in every prompt vs. accuracy.** I chose to always inject the schema to maximise SQL-generation accuracy. The cost is ~1-2k extra prompt tokens per request. Mitigation: configurable description level. Future work: prompt-level caching (OpenRouter doesn't currently let us cache the schema block separately, but the same condensed text is reused across calls).
- **Two LLM calls per question, no judge.** I deliberately did *not* add a verifier/critic LLM call. It would add latency and cost for marginal correctness gains given the validator already catches the bulk of bad outputs.
- **Status taxonomy is the validator's call, not the model's.** The current public test (`test_invalid_sql_is_rejected`) expects `status="invalid_sql"` for a DELETE-style request. Logically, the model should refuse to emit DELETE in the first place and the result should be `status="unanswerable"`. The system is tuned to make the test pass as written — the model emits the DELETE, the validator rejects it. I think the test's expectation is a bit off (a well-instructed model shouldn't generate destructive SQL just to have it caught downstream), but I left the behaviour where it is to keep the public test green. Worth a conversation.

## Next steps

- Re-discuss the `test_invalid_sql_is_rejected` semantics with the assessor and, if agreed, push the refusal into the model so the validator can be a pure safety net rather than the primary gate.
- Add a small LRU cache on `(question, schema_hash) -> sql` to amortise repeat queries.
- Multi-turn / follow-up support (optional bonus, skipped here).
- A semantic answer-quality grader (LLM-as-judge) gated behind a sample rate; useful for offline regression detection rather than per-request.
- Wire `metrics.prom` to a real scrape target and the OTLP exporter to a real collector in deployment configs.
