import logging
import sqlite3
from abc import ABC
from abc import abstractmethod
from sqlite3 import Cursor

from google.cloud.bigquery import Client

logger = logging.getLogger("uvicorn.error")


class ProductRepository(ABC):

    @abstractmethod
    def search_products(self, search_term: str) -> list[dict]:
        raise NotImplementedError()


class SQLiteProductRepository(ProductRepository):

    def __init__(self, db_path: str):
        self.db_path = db_path

    @staticmethod
    def _get_column_names(cursor: Cursor) -> list[str]:
        return [column[0] for column in cursor.description]

    def search_products(self, search_term: str) -> list[dict]:
        logger.info(f"SQLite: Searching products with term '{search_term}'")
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        query = """
                SELECT
                    product_id as id,
                    name,
                    size,
                    categories,
                    subcategories,
                    price,
                    image_url
                FROM products
                WHERE LOWER(name) LIKE :search
                   OR LOWER(size) LIKE :search
                   OR LOWER(categories) LIKE :search
                   OR LOWER(subcategories) LIKE :search \
                """
        search_term = f"%{search_term.lower()}%"
        rows = cursor.execute(query, {"search": search_term}).fetchall()
        logger.info(f"SQLite: Found {len(rows)} products for term '{search_term}'")
        conn.close()
        return [dict(zip(self._get_column_names(cursor), row)) for row in rows]


class BigQueryProductRepository(ProductRepository):

    def __init__(self, project_id: str, dataset_id: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bq = Client(project=project_id)

    def search_products(self, search_term: str) -> list[dict]:
        logger.info(f"BigQuery: Searching products with term '{search_term}'")
        query = f"""
            SELECT
                product_id as id,
                name,
                size,
                categories,
                subcategories,
                price,
                image_url
            FROM `{self.project_id}.{self.dataset_id}.dbt_unique_products`
            WHERE LOWER(name) LIKE '%{search_term}%'
               OR LOWER(size) LIKE '%{search_term}%'
               OR LOWER(categories) LIKE '%{search_term}%'
               OR LOWER(subcategories) LIKE '%{search_term}%'
            """
        rows = self.bq.query(query).result()
        results = [dict(row.items()) for row in rows]
        logger.info(f"BigQuery: Found {len(results)} products for term '{search_term}'")
        return results
