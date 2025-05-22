import os
import re
import sys
from unittest import TestCase

from sqlglot import transpile

MODELS_DIR = "dbt/models"
TARGET_DIALECT = "bigquery"
CONFIG_JINJA_PATTERN = re.compile(r"\{\{\s*config\s*\(.*?\)\s*\}\}", re.DOTALL)
GENERAL_JINJA_PATTERN = re.compile(r"(\{\{.*?\}\}|\{%-?.*?-%\}|\{%.*?%\})", re.DOTALL)


def mask_jinja(sql):
    config_blocks = []
    general_blocks = []

    # Step 1: Mask top-level config blocks with SQL comments
    def config_replacer(match):
        config_blocks.append(match.group(0))
        return f"/* JINJA_CONFIG_EXPRESSION_{len(config_blocks) - 1} */ "

    sql = CONFIG_JINJA_PATTERN.sub(config_replacer, sql)

    # Step 2: Mask all other jinja blocks with placeholders
    def general_replacer(match):
        general_blocks.append(match.group(0))
        return f"__JINJA_EXPRESSION_{len(general_blocks) - 1}__"

    sql = GENERAL_JINJA_PATTERN.sub(general_replacer, sql)

    return sql, config_blocks, general_blocks


def unmask_jinja(sql, config_blocks, general_blocks):
    # Restore config blocks
    for i, block in enumerate(config_blocks):
        sql = sql.replace(f"/* JINJA_CONFIG_EXPRESSION_{i} */", block)

    # Restore general blocks
    for i, block in enumerate(general_blocks):
        sql = sql.replace(f"__JINJA_EXPRESSION_{i}__", block)

    return sql


def transpile_sql(sql):
    try:
        formatted_sql = transpile(sql, read="bigquery", write=TARGET_DIALECT, pretty=True)[0]
        return formatted_sql
    except Exception as e:
        print(f"Error transpiling SQL: {e}")
        print(f"Original SQL: {sql}")
        e.with_traceback(sys.exc_info()[2])
        raise e


def add_cte_break_line(formatted_sql):
    lines = formatted_sql.splitlines()
    result_lines = []
    for i, line in enumerate(lines):
        result_lines.append(line)
        if line.strip().endswith("),") and i + 1 < len(lines):
            next_line = lines[i + 1].strip().lower()
            if next_line.startswith("(") or next_line.endswith("as ("):
                result_lines.append("")

    return "\n".join(result_lines)


def format_sql_file(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        sql = f.read()

    masked_sql, jinja_config_blocks, jinja_general_blocks = mask_jinja(sql)
    formatted_sql = transpile_sql(masked_sql)
    final_sql = unmask_jinja(formatted_sql, jinja_config_blocks, jinja_general_blocks)

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(final_sql)


def format_all_models():
    for root, _, files in os.walk(MODELS_DIR):
        for file in files:
            if file.endswith(".sql"):
                format_sql_file(os.path.join(root, file))


class TestMaskJinja(TestCase):

    def test_config_only(self):
        sql = "{{ config(materialized='view') }}\nSELECT * FROM table"
        masked, config_blocks, general_blocks = mask_jinja(sql)

        self.assertIn("/* JINJA_CONFIG_EXPRESSION_0 */", masked)
        self.assertIn("SELECT * FROM table", masked)
        self.assertEqual(config_blocks[0], "{{ config(materialized='view') }}")
        self.assertEqual(general_blocks, [])

    def test_general_jinja_only(self):
        sql = "SELECT * FROM {{ source('project', 'table') }} WHERE date = {{ get_date() }}"
        masked, config_blocks, general_blocks = mask_jinja(sql)

        self.assertIn("__JINJA_EXPRESSION_0__", masked)
        self.assertIn("__JINJA_EXPRESSION_1__", masked)
        self.assertEqual(config_blocks, [])
        self.assertEqual(general_blocks[0], "{{ source('project', 'table') }}")
        self.assertEqual(general_blocks[1], "{{ get_date() }}")

    def test_both_config_and_general(self):
        sql = "{{ config(materialized='view') }}\nSELECT * FROM {{ ref('table') }}"
        masked, config_blocks, general_blocks = mask_jinja(sql)

        self.assertIn("/* JINJA_CONFIG_EXPRESSION_0 */", masked)
        self.assertIn("__JINJA_EXPRESSION_0__", masked)
        self.assertEqual(config_blocks[0], "{{ config(materialized='view') }}")
        self.assertEqual(general_blocks[0], "{{ ref('table') }}")

    def test_multiline_config(self):
        sql = """
        {{ config(
            materialized='table',
            tags=['daily']
        ) }}
        SELECT * FROM users
        """
        masked, config_blocks, general_blocks = mask_jinja(sql)

        self.assertIn("/* JINJA_CONFIG_EXPRESSION_0 */", masked)
        self.assertIn("SELECT * FROM users", masked)
        self.assertEqual(len(config_blocks), 1)
        self.assertTrue("materialized='table'" in config_blocks[0])

    def test_no_jinja(self):
        sql = "SELECT * FROM clean_table"
        masked, config_blocks, general_blocks = mask_jinja(sql)

        self.assertEqual(masked, sql)
        self.assertEqual(config_blocks, [])
        self.assertEqual(general_blocks, [])


class TestTranspileSQL(TestCase):

    def test_query_with_bigquery_like_datasource(self):
        sql = """
        SELECT *
        FROM `bigquery-like.exmple.users`
        WHERE id = 1
        """

        expected = """
SELECT
  *
FROM `bigquery-like.exmple.users`
WHERE
  id = 1
"""
        actual = transpile_sql(sql)
        self.assertEqual(expected.strip(), actual)

    def test_query_with_cte_should_be_formatted(self):
        sql = """
        WITH cte1 AS (SELECT id
                    FROM users),
            cte2 AS (SELECT id
                    FROM orders)
        SELECT *
        FROM cte1
                JOIN cte2 USING (id)
        """

        expected = """
WITH cte1 AS (
  SELECT
    id
  FROM users
), cte2 AS (
  SELECT
    id
  FROM orders
)
SELECT
  *
FROM cte1
JOIN cte2
  USING (id)
"""
        actual = transpile_sql(sql)
        self.assertEqual(expected.strip(), actual)

    def test_query_with_jinja_config_placeholder_should_be_formatted(self):
        sql = """
        /* JINJA_CONFIG_EXPRESSION_0 */
        WITH cte1 AS (SELECT id
                    FROM users),
            cte2 AS (SELECT id
                    FROM orders)
        SELECT *
        FROM cte1
                JOIN cte2 USING (id)
        """

        expected = """
/* JINJA_CONFIG_EXPRESSION_0 */
WITH cte1 AS (
  SELECT
    id
  FROM users
), cte2 AS (
  SELECT
    id
  FROM orders
)
SELECT
  *
FROM cte1
JOIN cte2
  USING (id)
"""
        actual = transpile_sql(sql)
        self.assertEqual(expected.strip(), actual)


if __name__ == "__main__":
    format_all_models()
