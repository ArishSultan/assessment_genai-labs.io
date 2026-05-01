from __future__ import annotations

import sys
import unittest
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from src.sql_validator import SQLValidator

# Realistic-ish analytics schema with a few tables and overlapping columns.
SCHEMA = {
    "users": ["id", "email", "created_at", "country", "name"],
    "orders": ["id", "user_id", "total", "created_at", "status"],
    "products": ["id", "name", "price", "category"],
    "order_items": ["id", "order_id", "product_id", "quantity", "price"],
}


class SQLValidatorBaseTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.validator = SQLValidator(
            schema=SCHEMA,
            enforce_limit=True,
            max_limit=1000,
        )


class EmptyInputTests(SQLValidatorBaseTest):
    def test_empty_inputs_rejected(self) -> None:
        for sql in (None, "", "   ", "\n\n", "\t  \t"):
            with self.subTest(sql=repr(sql)):
                result = self.validator.validate(sql)
                self.assertFalse(result.is_valid)
                self.assertIn("No SQL provided", result.error)


class ParseErrorTests(SQLValidatorBaseTest):
    def test_unparseable_rejected(self) -> None:
        for sql in (
                "SELEKT * FROM users",
                "SELECT FROM",
                "SELECT * FROM users WHERE",
                "SELECT * FROM users WHERE id =",
                "((((SELECT 1",
        ):
            with self.subTest(sql=sql):
                result = self.validator.validate(sql)
                self.assertFalse(result.is_valid)


class MultiStatementTests(SQLValidatorBaseTest):
    def test_stacked_statements_rejected(self) -> None:
        for sql in (
                "SELECT id FROM users; SELECT id FROM orders",
                "SELECT 1; DROP TABLE users",
                "SELECT id FROM users; DELETE FROM users WHERE id = 1",
        ):
            with self.subTest(sql=sql):
                result = self.validator.validate(sql)
                self.assertFalse(result.is_valid)

    def test_trailing_comment_passes(self) -> None:
        sql = "SELECT id FROM users WHERE id = 1; --"
        result = self.validator.validate(sql)
        self.assertTrue(result.is_valid, msg=result.error)


class TopLevelTypeTests(SQLValidatorBaseTest):
    def test_non_select_rejected(self) -> None:
        cases = (
            "INSERT INTO users (id) VALUES (1)",
            "UPDATE users SET email='x' WHERE id=1",
            "DELETE FROM users WHERE id=1",
            "DROP TABLE users",
            "ALTER TABLE users ADD COLUMN x TEXT",
            "CREATE TABLE foo (id INT)",
            "CREATE INDEX idx ON users(email)",
            "PRAGMA foreign_keys = ON",
            "VACUUM",
            "ATTACH DATABASE 'evil.db' AS evil",
            "DETACH DATABASE main",
            "BEGIN TRANSACTION",
            "COMMIT",
            "ROLLBACK",
            "REPLACE INTO users (id) VALUES (1)",
        )
        for sql in cases:
            with self.subTest(sql=sql):
                result = self.validator.validate(sql)
                self.assertFalse(result.is_valid, msg=f"Expected rejection for: {sql}")
                msg = result.error.lower()
                self.assertTrue(
                    "only select" in msg or "forbidden" in msg,
                    msg=f"Unexpected error message for {sql!r}: {result.error}",
                )


class StringLiteralFalsePositiveTests(SQLValidatorBaseTest):
    def test_keyword_in_string_literal_passes(self) -> None:
        cases = (
            "SELECT id FROM users WHERE name LIKE '%delete this%'",
            "SELECT id FROM users WHERE name = 'drop the bass'",
            "SELECT id FROM users WHERE email LIKE '%update@example.com%'",
            "SELECT id FROM users WHERE name = 'CREATE was the start'",
            "SELECT id FROM users WHERE name LIKE 'INSERT %'",
            "SELECT id FROM users WHERE country = 'TRUNCATE Island'",
        )
        for sql in cases:
            with self.subTest(sql=sql):
                result = self.validator.validate(sql)
                self.assertTrue(
                    result.is_valid,
                    msg=f"False positive on: {sql} -> {result.error}",
                )


class TableAllowListTests(SQLValidatorBaseTest):
    def test_unknown_table_rejected(self) -> None:
        cases = (
            "SELECT * FROM secret_admin_table",
            "SELECT u.id FROM users u JOIN secrets s ON u.id = s.user_id",
            "SELECT * FROM sqlite_master",
        )
        for sql in cases:
            with self.subTest(sql=sql):
                result = self.validator.validate(sql)
                self.assertFalse(result.is_valid)
                self.assertIn("unknown table", result.error.lower())

    def test_table_alias_resolution(self) -> None:
        sql = "SELECT u.email FROM users u WHERE u.id = 1"
        result = self.validator.validate(sql)
        self.assertTrue(result.is_valid, msg=result.error)

    def test_case_insensitive_table_name(self) -> None:
        sql = "SELECT * FROM USERS LIMIT 10"
        result = self.validator.validate(sql)
        self.assertTrue(result.is_valid, msg=result.error)


class ColumnAllowListTests(SQLValidatorBaseTest):
    def test_unknown_unqualified_column_rejected(self) -> None:
        result = self.validator.validate("SELECT password FROM users")
        self.assertFalse(result.is_valid)
        self.assertIn("unknown column", result.error.lower())

    def test_unknown_qualified_column_rejected(self) -> None:
        result = self.validator.validate("SELECT u.password FROM users u")
        self.assertFalse(result.is_valid)
        self.assertIn("password", result.error)

    def test_column_from_wrong_table_rejected(self) -> None:
        # 'total' exists on orders, not users
        result = self.validator.validate("SELECT u.total FROM users u")
        self.assertFalse(result.is_valid)
        self.assertIn("unknown column", result.error.lower())

    def test_qualified_column_correct_table_passes(self) -> None:
        result = self.validator.validate("SELECT o.total FROM orders o")
        self.assertTrue(result.is_valid, msg=result.error)

    def test_unknown_table_qualifier_rejected(self) -> None:
        result = self.validator.validate("SELECT x.id FROM users u")
        self.assertFalse(result.is_valid)
        msg = result.error.lower()
        self.assertTrue("qualifier" in msg or "unknown" in msg)

    def test_column_shared_between_tables(self) -> None:
        # 'created_at' exists on both users and orders
        result = self.validator.validate("SELECT created_at FROM users")
        self.assertTrue(result.is_valid, msg=result.error)

    def test_select_star_passes(self) -> None:
        result = self.validator.validate("SELECT * FROM users")
        self.assertTrue(result.is_valid, msg=result.error)

    def test_qualified_star_passes(self) -> None:
        result = self.validator.validate("SELECT u.* FROM users u")
        self.assertTrue(result.is_valid, msg=result.error)

    def test_select_alias_usable_in_order_by(self) -> None:
        sql = "SELECT email AS contact FROM users ORDER BY contact"
        result = self.validator.validate(sql)
        self.assertTrue(result.is_valid, msg=result.error)

    def test_alias_does_not_grant_arbitrary_columns(self) -> None:
        sql = "SELECT email AS contact, password FROM users"
        result = self.validator.validate(sql)
        self.assertFalse(result.is_valid)
        self.assertIn("password", result.error)

    def test_cte_columns_allowed_through(self) -> None:
        sql = (
            "WITH x AS (SELECT id, email FROM users) "
            "SELECT x.email FROM x"
        )
        result = self.validator.validate(sql)
        self.assertTrue(result.is_valid, msg=result.error)


class RelaxedRuleTests(SQLValidatorBaseTest):
    def test_long_sql_accepted(self) -> None:
        big_select = ", ".join(
            ["u.id", "u.email", "u.created_at", "u.country", "u.name"] * 16
        )
        sql = f"SELECT {big_select} FROM users u WHERE u.country = 'US'"
        self.assertGreater(len(sql), 800, msg="Test setup: SQL not long enough")
        result = self.validator.validate(sql)
        self.assertTrue(result.is_valid, msg=result.error)

    def test_deeply_nested_subqueries(self) -> None:
        sql = (
            "SELECT id FROM users WHERE id IN ("
            "  SELECT user_id FROM orders WHERE id IN ("
            "    SELECT order_id FROM order_items WHERE product_id IN ("
            "      SELECT id FROM products WHERE category = 'x'"
            "    )"
            "  )"
            ")"
        )
        result = self.validator.validate(sql)
        self.assertTrue(result.is_valid, msg=result.error)

    def test_cross_join_accepted(self) -> None:
        result = self.validator.validate("SELECT u.id FROM users u CROSS JOIN orders o")
        self.assertTrue(result.is_valid, msg=result.error)

    def test_implicit_cross_join_accepted(self) -> None:
        result = self.validator.validate("SELECT u.id FROM users u, orders o")
        self.assertTrue(result.is_valid, msg=result.error)

    def test_recursive_cte_accepted(self) -> None:
        sql = (
            "WITH RECURSIVE cnt(x) AS ("
            "  SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x<10"
            ") SELECT x FROM cnt"
        )
        result = self.validator.validate(sql)
        self.assertTrue(result.is_valid, msg=result.error)

    def test_function_calls_accepted(self) -> None:
        cases = (
            "SELECT count(*) FROM users",
            "SELECT strftime('%Y', created_at) FROM users",
            "SELECT lower(email) FROM users",
            "SELECT abs(total) FROM orders",
            "SELECT typeof(id) FROM users",
        )
        for sql in cases:
            with self.subTest(sql=sql):
                result = self.validator.validate(sql)
                self.assertTrue(result.is_valid, msg=f"{sql} -> {result.error}")

    def test_many_joins_accepted(self) -> None:
        sql = (
            "SELECT u.id FROM users u "
            "JOIN orders o1 ON u.id = o1.user_id "
            "JOIN orders o2 ON u.id = o2.user_id "
            "JOIN orders o3 ON u.id = o3.user_id "
            "JOIN orders o4 ON u.id = o4.user_id "
            "JOIN orders o5 ON u.id = o5.user_id "
            "JOIN orders o6 ON u.id = o6.user_id"
        )
        result = self.validator.validate(sql)
        self.assertTrue(result.is_valid, msg=result.error)


class LimitEnforcementTests(SQLValidatorBaseTest):
    def test_missing_limit_added(self) -> None:
        result = self.validator.validate("SELECT id FROM users")
        self.assertTrue(result.is_valid)
        self.assertIn("LIMIT 1000", result.validated_sql.upper())

    def test_existing_small_limit_preserved(self) -> None:
        result = self.validator.validate("SELECT id FROM users LIMIT 10")
        self.assertTrue(result.is_valid)
        self.assertIn("LIMIT 10", result.validated_sql.upper())

    def test_oversize_limit_clamped(self) -> None:
        result = self.validator.validate("SELECT id FROM users LIMIT 999999")
        self.assertTrue(result.is_valid)
        self.assertIn("LIMIT 1000", result.validated_sql.upper())
        self.assertNotIn("999999", result.validated_sql)

    def test_limit_disabled(self) -> None:
        v = SQLValidator(schema=SCHEMA, enforce_limit=False)
        result = v.validate("SELECT id FROM users")
        self.assertTrue(result.is_valid)
        self.assertNotIn("limit", result.validated_sql.lower())


class UnionTests(SQLValidatorBaseTest):
    def test_union_passes(self) -> None:
        sql = "SELECT id FROM users UNION SELECT user_id FROM orders"
        result = self.validator.validate(sql)
        self.assertTrue(result.is_valid, msg=result.error)

    def test_union_all_passes(self) -> None:
        sql = "SELECT id FROM users UNION ALL SELECT user_id FROM orders"
        result = self.validator.validate(sql)
        self.assertTrue(result.is_valid, msg=result.error)

    def test_union_with_bad_table_rejected(self) -> None:
        sql = "SELECT id FROM users UNION SELECT id FROM secrets"
        result = self.validator.validate(sql)
        self.assertFalse(result.is_valid)


class AdversarialTests(SQLValidatorBaseTest):
    def test_smuggled_dml_in_subquery(self) -> None:
        sql = "SELECT * FROM users WHERE id IN (SELECT 1; DROP TABLE users; --)"
        result = self.validator.validate(sql)
        self.assertFalse(result.is_valid)

    def test_comment_smuggle_passes_harmlessly(self) -> None:
        # Comment is stripped; what remains is a valid SELECT.
        sql = "SELECT * FROM users /* ; DROP TABLE users; */"
        result = self.validator.validate(sql)
        self.assertTrue(result.is_valid, msg=result.error)

    def test_unicode_table_name_rejected(self) -> None:
        sql = "SELECT * FROM \"users\u200b\""
        result = self.validator.validate(sql)
        self.assertFalse(result.is_valid)


class RealisticQueryTests(SQLValidatorBaseTest):
    def test_realistic_analytics_queries(self) -> None:
        cases = (
            "SELECT id, email FROM users WHERE country = 'US' LIMIT 100",
            (
                "SELECT u.email, COUNT(o.id) AS order_count "
                "FROM users u LEFT JOIN orders o ON u.id = o.user_id "
                "GROUP BY u.email ORDER BY order_count DESC LIMIT 50"
            ),
            (
                "SELECT p.category, SUM(oi.quantity * oi.price) AS revenue "
                "FROM products p "
                "JOIN order_items oi ON p.id = oi.product_id "
                "GROUP BY p.category"
            ),
            (
                "WITH high_value AS ("
                "  SELECT user_id FROM orders GROUP BY user_id HAVING SUM(total) > 1000"
                ") "
                "SELECT u.email FROM users u JOIN high_value h ON u.id = h.user_id"
            ),
            "SELECT DISTINCT country FROM users",
            "SELECT id FROM users WHERE created_at > '2024-01-01'",
            "SELECT COUNT(DISTINCT user_id) FROM orders",
            (
                "SELECT id, "
                "row_number() OVER (PARTITION BY country ORDER BY created_at) "
                "FROM users"
            ),
        )
        for sql in cases:
            with self.subTest(sql=sql):
                result = self.validator.validate(sql)
                self.assertTrue(
                    result.is_valid,
                    msg=f"Should accept: {sql}\n  Error: {result.error}",
                )


class OutputContractTests(SQLValidatorBaseTest):
    def test_success_shape(self) -> None:
        result = self.validator.validate("SELECT id FROM users")
        self.assertTrue(result.is_valid)
        self.assertIsNotNone(result.validated_sql)
        self.assertIsNone(result.error)
        self.assertGreaterEqual(result.timing_ms, 0)

    def test_failure_shape(self) -> None:
        result = self.validator.validate("DROP TABLE users")
        self.assertFalse(result.is_valid)
        self.assertIsNone(result.validated_sql)
        self.assertIsNotNone(result.error)
        self.assertGreaterEqual(result.timing_ms, 0)


if __name__ == "__main__":
    unittest.main()
