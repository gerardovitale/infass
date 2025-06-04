import logging
import os
import re
import sys
from pathlib import Path
from unittest import TestCase

from sqlglot import transpile

DIRECTORY_LIST = [
    "dbt/models",
]
READ_DIALECT = "bigquery"
TARGET_DIALECT = "bigquery"
TRANSPILE_PARAMS = {
    "read": READ_DIALECT,
    "write": TARGET_DIALECT,
    "pretty": True,
    "indent": 4,
    "pad": 4,
}

# Regex patterns to match Jinja blocks
CONFIG_JINJA_PATTERN = re.compile(r"\{\{\s*config\s*\(.*?\)\s*\}\}", re.DOTALL)
GENERAL_JINJA_PATTERN = re.compile(r"(\{\{.*?\}\}|\{%-?.*?-%\}|\{%.*?%\})", re.DOTALL)

# Pattern to split consecutive CTEs with a newline
CTE_SEPARATOR_PATTERN = r"\),\s*(?=\w+\s+AS\s+\()"
CTE_SEPARATOR_REPLACEMENT = "),\n\n"

# Pattern to insert a blank line before the final SELECT after last CTE
FINAL_SELECT_SPACING_PATTERN = r"\)\s*\n\s*SELECT"
FINAL_SELECT_SPACING_REPLACEMENT = ")\n\nSELECT"


#######################################################################################################################
# MAIN
#######################################################################################################################


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    logging.info("Starting SQL formatting process.")
    for directory in DIRECTORY_LIST:
        format_all_models(Path(directory))
    logging.info("SQL formatting process completed.")


def format_all_models(directory: Path):
    logging.info(f"Scanning directory: {directory}")
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".sql"):
                file_path = os.path.join(root, file)
                logging.info(f"Formatting file: {file_path}")
                format_sql_file(file_path)


#######################################################################################################################
# SQL FORMATTING
#######################################################################################################################


def format_sql_file(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        sql = f.read()

    masked_sql, jinja_config_blocks, jinja_general_blocks = mask_jinja(sql)
    formatted_sql = transpile_sql(masked_sql)
    final_sql = unmask_jinja(formatted_sql, jinja_config_blocks, jinja_general_blocks)

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(final_sql)


def transpile_sql(sql):
    try:
        formatted_sql = transpile(sql, **TRANSPILE_PARAMS)[0].rstrip() + "\n"
        return add_cte_break_line(formatted_sql)

    except Exception as e:
        logging.error(f"Error transpiling SQL: {e}")
        logging.error(f"Original SQL: {sql}")
        e.with_traceback(sys.exc_info()[2])
        raise e


def add_cte_break_line(sql):
    sql = re.sub(CTE_SEPARATOR_PATTERN, CTE_SEPARATOR_REPLACEMENT, sql)
    sql = re.sub(FINAL_SELECT_SPACING_PATTERN, FINAL_SELECT_SPACING_REPLACEMENT, sql)
    return sql


#######################################################################################################################
# JINJA MASKING AND UNMASKING
#######################################################################################################################


def mask_jinja(sql):
    config_blocks = []
    general_blocks = []

    # Step 1: Mask top-level config blocks with SQL comments
    def config_replacer(match):
        config_blocks.append(match.group(0))
        return f"/* JINJA_CONFIG_EXPRESSION_{len(config_blocks) - 1} */"

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


#######################################################################################################################
# UNIT TESTS
#######################################################################################################################


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

        expected = """SELECT
    *
FROM `bigquery-like.exmple.users`
WHERE
    id = 1
"""
        actual = transpile_sql(sql)
        self.assertEqual(expected, actual)

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

        expected = """WITH cte1 AS (
    SELECT
        id
    FROM users
),

cte2 AS (
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
        self.assertEqual(expected, actual)

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

        expected = """/* JINJA_CONFIG_EXPRESSION_0 */
WITH cte1 AS (
    SELECT
        id
    FROM users
),

cte2 AS (
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
        self.assertEqual(expected, actual)

    def test_query_with_jinja_placeholder_should_be_formatted(self):
        sql = """
              WITH cte1 AS (SELECT id
                            FROM users),
                   cte2 AS (SELECT id
                            FROM orders)
              SELECT *
              FROM __JINJA_EXPRESSION_0__
                       JOIN cte2 USING (id) \
              """

        expected = """WITH cte1 AS (
    SELECT
        id
    FROM users
),

cte2 AS (
    SELECT
        id
    FROM orders
)

SELECT
    *
FROM __JINJA_EXPRESSION_0__
JOIN cte2
    USING (id)
"""
        actual = transpile_sql(sql)
        self.assertEqual(expected, actual)


if __name__ == "__main__":
    main()
