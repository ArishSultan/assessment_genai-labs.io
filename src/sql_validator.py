import time
import sqlglot

from typing import Iterable, Mapping
from sqlglot import expressions as exp

from src.my_types import SQLValidationOutput

_FORBIDDEN_NODE_TYPES: tuple[type, ...] = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Drop,
    exp.Alter,
    exp.Create,
    exp.Pragma,
    exp.TruncateTable,
    exp.Command,
    exp.Set,
    exp.Transaction,
)


class SQLValidator:
    def __init__(
            self,
            schema: Mapping[str, Iterable[str]],
            *,
            dialect: str = "sqlite",
            enforce_limit: bool = True,
            max_limit: int = 1000,
    ) -> None:
        self._schema: dict[str, set[str]] = {
            t.lower(): {c.lower() for c in cols}
            for t, cols in schema.items()
        }
        self.dialect = dialect
        self.enforce_limit = enforce_limit
        self.max_limit = max_limit

    @staticmethod
    def _ok(start_ns: int, sql: str) -> SQLValidationOutput:
        elapsed_ms = (time.perf_counter_ns() - start_ns) / 1_000_000
        return SQLValidationOutput(
            error=None,
            is_valid=True,
            validated_sql=sql,
            timing_ms=elapsed_ms,
        )

    @staticmethod
    def _fail(start_ns: int, error: str) -> SQLValidationOutput:
        elapsed_ms = (time.perf_counter_ns() - start_ns) / 1_000_000
        return SQLValidationOutput(
            error=error,
            is_valid=False,
            validated_sql=None,
            timing_ms=elapsed_ms,
        )

    def validate(self, sql: str | None) -> SQLValidationOutput:
        start = time.perf_counter_ns()

        # 1. Empty input.
        if sql is None or not sql.strip():
            return self._fail(start, "No SQL provided")

        cleaned = sql.strip().rstrip(";").strip()

        # 2. Parse.
        try:
            raw_parsed = sqlglot.parse(cleaned, dialect=self.dialect)
        except sqlglot.errors.ParseError as exc:
            return self._fail(start, f"SQL parse error: {exc}")
        except Exception as exc:  # noqa: BLE001 -- sqlglot raises various types
            return self._fail(start, f"SQL parse error: {exc}")

        # Filter Nones and Semicolon nodes (sqlglot represents trailing `;`
        # or trailing comments as a separate Semicolon "statement").
        parsed_list = [
            p for p in raw_parsed
            if p is not None and not isinstance(p, exp.Semicolon)
        ]

        # 3. Single statement only.
        if not parsed_list:
            return self._fail(start, "No parseable statement found")
        if len(parsed_list) != 1:
            return self._fail(start, "Only a single statement is allowed")

        tree = parsed_list[0]

        # 4. Top-level type check. SELECT, UNION, and CTE-prefixed selects
        # all parse with a Select or Union root.
        if not isinstance(tree, (exp.Select, exp.Union)):
            return self._fail(
                start,
                f"Only SELECT statements are allowed (got {type(tree).__name__})",
            )

        # 5. Forbidden nodes anywhere (DDL/DML/PRAGMA/etc.).
        for node in tree.walk():
            if isinstance(node, _FORBIDDEN_NODE_TYPES):
                return self._fail(
                    start,
                    f"Forbidden statement type: {type(node).__name__}",
                )

        # 6a. CTE aliases -- these shadow real tables and have no schema
        # we can introspect.
        cte_aliases: set[str] = set()
        for cte in tree.find_all(exp.CTE):
            alias = (cte.alias_or_name or "").lower()
            if alias:
                cte_aliases.add(alias)

        # 6b. Real-table reference and alias map.
        # alias_to_table[name] = real_table_name (or None for CTE refs).
        alias_to_table: dict[str, str | None] = {}
        referenced_tables: set[str] = set()
        for tbl in tree.find_all(exp.Table):
            real_name = (tbl.name or "").lower()
            if not real_name:
                continue
            tbl_alias = (tbl.alias or "").lower()
            if real_name in cte_aliases:
                # CTE reference -- register alias with None so qualifiers
                # like `h.col` resolve, but skip schema check.
                alias_to_table[real_name] = None
                if tbl_alias:
                    alias_to_table[tbl_alias] = None
                continue
            if real_name not in self._schema:
                return self._fail(start, f"Unknown table: {tbl.name}")
            referenced_tables.add(real_name)
            alias_to_table[real_name] = real_name
            if tbl_alias:
                alias_to_table[tbl_alias] = real_name

        # 6c. SELECT-list output aliases. Valid in ORDER BY / HAVING.
        output_aliases: set[str] = set()
        for select in tree.find_all(exp.Select):
            for projection in select.expressions:
                if isinstance(projection, exp.Alias):
                    a = (projection.alias or "").lower()
                    if a:
                        output_aliases.add(a)

        # 7. Column validation.
        for col in tree.find_all(exp.Column):
            cname = (col.name or "").lower()
            if not cname or cname == "*":
                continue

            qualifier = (col.table or "").lower()
            if qualifier:
                if qualifier not in alias_to_table:
                    return self._fail(
                        start, f"Unknown table qualifier: {col.table}"
                    )
                real = alias_to_table[qualifier]
                if real is None:
                    continue  # CTE column, can't introspect
                if cname not in self._schema[real]:
                    return self._fail(
                        start, f"Unknown column: {col.table}.{col.name}"
                    )
            else:
                if cname in output_aliases:
                    continue
                if not referenced_tables:
                    continue  # only CTE refs, allow through
                if not any(
                        cname in self._schema[t] for t in referenced_tables
                ):
                    return self._fail(start, f"Unknown column: {col.name}")

        # 8. LIMIT enforcement (always overwrite to min(existing, max_limit)).
        validated_sql = cleaned
        if self.enforce_limit:
            existing = tree.args.get("limit")
            apply_limit = self.max_limit
            if existing is not None:
                lit = getattr(existing, "expression", None)
                if isinstance(lit, exp.Literal) and lit.is_int:
                    try:
                        current = int(lit.this)
                        if 0 < current <= self.max_limit:
                            apply_limit = current
                    except (ValueError, TypeError):
                        pass
            tree = tree.limit(apply_limit)
            validated_sql = tree.sql(dialect=self.dialect)

        return self._ok(start, validated_sql)
