import logging
import sqlite3
from pathlib import Path
from sqlite3 import Connection
from sqlite3 import Cursor

from fastapi import HTTPException
from repositories.product_repo import ProductRepository

logger = logging.getLogger("uvicorn.error")


class SQLiteProductRepository(ProductRepository):

    def __init__(self, db_path: str):
        self.check_db_path_exist(db_path)
        self.db_path = db_path

    def search_products(self, search_term: str) -> list[dict]:
        logger.info(f"SQLiteRepo - Searching products with term '{search_term}' using FTS5 table if available")
        conn = self.get_connection()
        cursor = conn.cursor()
        fts_query = """
                SELECT p.id,
                       p.name,
                       p.size,
                       p.categories,
                       p.subcategories,
                       p.price      AS current_price,
                       p.image_url
                FROM products AS p
                JOIN products_fts ON p.id = products_fts.id
                WHERE products_fts MATCH :search
                """
        search_term_fts = search_term.strip().lower()
        if not search_term_fts.endswith("*"):
            search_term_fts += "*"
        rows = cursor.execute(fts_query, {"search": search_term_fts}).fetchall()
        logger.info(
            f"SQLiteRepo - Found {len(rows)} products for term '{search_term}' (fts query: '{search_term_fts}')"
        )
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
                SELECT p.id,
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
                              ON p.id = ppd.id
                WHERE p.id = :product_id
                """
        rows = cursor.execute(query, {"product_id": product_id}).fetchall()
        logger.info(f"SQLiteRepo - Found {len(rows)} records for product_id '{product_id}'")
        if not rows:
            logger.warning(f"SQLiteRepo - No product found for id: '{product_id}'")
            conn.close()
            raise HTTPException(status_code=404, detail="Product not found or no price details available")
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
