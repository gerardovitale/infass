import logging
import sqlite3
from pathlib import Path
from sqlite3 import Connection
from sqlite3 import Cursor

from repositories.product_repo import ProductRepository

logger = logging.getLogger("uvicorn.error")


class SQLiteProductRepository(ProductRepository):

    def __init__(self, db_path: str):
        self.check_db_path_exist(db_path)
        self.db_path = db_path

    def search_products(self, search_term: str) -> list[dict]:
        logger.info(f"SQLiteRepo - Searching products with term '{search_term}'")
        conn = self.get_connection()
        cursor = conn.cursor()
        query = """
                SELECT product_id as id,
                       name,
                       size,
                       categories,
                       subcategories,
                       price      AS current_price,
                       image_url
                FROM products
                WHERE LOWER(name) LIKE :search
                   OR LOWER(size) LIKE :search
                   OR LOWER(categories) LIKE :search
                   OR LOWER(subcategories) LIKE :search
                """
        search_term = f"%{search_term.lower()}%"
        rows = cursor.execute(query, {"search": search_term}).fetchall()
        logger.info(f"SQLiteRepo - Found {len(rows)} products for term '{search_term}'")
        conn.close()
        return self.map_rows(rows, cursor)

    def get_enriched_product(self, product_id: str):
        def map_enriched_product(mapped_rows):
            base_keys = ["id", "name", "size", "categories", "subcategories", "current_price", "image_url"]
            detail_keys = ["date", "price", "sma7", "sma15", "sma30"]
            product = {k: mapped_rows[0][k] for k in base_keys}
            product["price_details"] = [{k: d[k] for k in detail_keys} for d in mapped_rows]
            return product

        logger.info(f"SQLiteRepo - Getting enriched product by id: '{product_id}'")
        conn = self.get_connection()
        cursor = conn.cursor()
        query = """
                SELECT p.product_id AS id,
                       p.name,
                       p.size,
                       p.categories,
                       p.subcategories,
                       p.price      AS current_price,
                       p.image_url,
                       ppd.date,
                       ppd.price    AS price,
                       ppd.sma7,
                       ppd.sma15,
                       ppd.sma30
                FROM products AS p
                         JOIN product_price_details AS ppd
                              ON p.product_id = ppd.product_id
                WHERE p.product_id = :product_id
                """
        rows = cursor.execute(query, {"product_id": product_id}).fetchall()
        logger.info(f"SQLiteRepo - Found {len(rows)} records for product_id '{product_id}'")
        conn.close()
        return map_enriched_product(self.map_rows(rows, cursor))

    def map_rows(self, rows: list[tuple], cursor: Cursor):
        return [dict(zip(self._get_column_names(cursor), row)) for row in rows]

    def get_connection(self) -> Connection:
        logger.info(f"SQLiteRepo - Connecting to database at {self.db_path}")
        try:
            return sqlite3.connect(self.db_path)
        except sqlite3.OperationalError:
            logger.error(f"SQLiteRepo - Failed to connect to database at {self.db_path}")
            raise sqlite3.OperationalError("Could not connect to the SQLite database.")

    @staticmethod
    def check_db_path_exist(db_path: str) -> None:
        if not Path(db_path).exists():
            logger.error(f"SQLiteRepo - Database file not found at {db_path}")
            raise FileNotFoundError(f"Database file not found at {db_path}")

    @staticmethod
    def _get_column_names(cursor: Cursor) -> list[str]:
        return [column[0] for column in cursor.description]
