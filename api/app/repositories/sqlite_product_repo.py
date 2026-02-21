import logging
import os
import sqlite3
from pathlib import Path
from sqlite3 import Cursor

from fastapi import HTTPException
from repositories.product_repo import ProductRepository

logger = logging.getLogger("uvicorn.error")


class SQLiteProductRepository(ProductRepository):

    def __init__(self, db_path: str):
        self.check_db_path_exist(db_path)
        self.db_path = db_path

    def search_products(self, search_term: str, limit: int = 20, offset: int = 0) -> list[dict]:
        logger.info(f"SQLiteRepo - Searching products with term '{search_term}' using FTS5 table if available")
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
                LIMIT :limit OFFSET :offset
                """
        search_term_fts = self._prepare_fts_term(search_term)
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            rows = cursor.execute(fts_query, {"search": search_term_fts, "limit": limit, "offset": offset}).fetchall()
            logger.info(
                f"SQLiteRepo - Found {len(rows)} products for term '{search_term}' (fts query: '{search_term_fts}')"
            )
            return self.map_rows(rows, cursor)

    def count_search_products(self, search_term: str) -> int:
        logger.info(f"SQLiteRepo - Counting products with term '{search_term}'")
        count_query = """
                SELECT COUNT(*)
                FROM products AS p
                JOIN products_fts ON p.id = products_fts.id
                WHERE products_fts MATCH :search
                """
        search_term_fts = self._prepare_fts_term(search_term)
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            count = cursor.execute(count_query, {"search": search_term_fts}).fetchone()[0]
            logger.info(f"SQLiteRepo - Total count for term '{search_term}': {count}")
            return count

    @staticmethod
    def _prepare_fts_term(search_term: str) -> str:
        search_term_fts = search_term.strip().lower()
        if not search_term_fts.endswith("*"):
            search_term_fts += "*"
        return search_term_fts

    def get_enriched_product(self, product_id: str):
        def map_enriched_product(mapped_rows):
            base_keys = ["id", "name", "size", "categories", "subcategories", "current_price", "image_url"]
            detail_keys = ["date", "price", "sma7", "sma15", "sma30"]
            product = {k: mapped_rows[0][k] for k in base_keys}
            product["price_details"] = [{k: d[k] for k in detail_keys} for d in mapped_rows]
            return product

        logger.info(f"SQLiteRepo - Getting enriched product by id: '{product_id}'")
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
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            rows = cursor.execute(query, {"product_id": product_id}).fetchall()
            logger.info(f"SQLiteRepo - Found {len(rows)} records for product_id '{product_id}'")
            if not rows:
                logger.warning(f"SQLiteRepo - No product found for id: '{product_id}'")
                raise HTTPException(status_code=404, detail="Product not found or no price details available")
            return map_enriched_product(self.map_rows(rows, cursor))

    def map_rows(self, rows: list[tuple], cursor: Cursor):
        return [dict(zip(self._get_column_names(cursor), row)) for row in rows]

    # get_connection is no longer needed due to context manager usage

    @staticmethod
    def check_db_path_exist(db_path: str) -> None:
        path_obj = Path(db_path)
        logger.info(f"SQLiteRepo - Checking db_path: {db_path} (absolute: {path_obj.resolve()})")
        logger.info(f"SQLiteRepo - Current working directory: {Path.cwd()}")
        exists = path_obj.exists()
        readable = path_obj.is_file() and os.access(path_obj, os.R_OK)
        writable = path_obj.is_file() and os.access(path_obj, os.W_OK)
        logger.info(f"SQLiteRepo - Exists: {exists}, Readable: {readable}, Writable: {writable}")
        if not exists:
            logger.error(f"SQLiteRepo - Database file not found at {db_path} (absolute: {path_obj.resolve()})")
            raise FileNotFoundError(f"Database file not found at {db_path}")
        if not readable:
            logger.error(f"SQLiteRepo - Database file at {db_path} is not readable")
        if not writable:
            logger.warning(f"SQLiteRepo - Database file at {db_path} is not writable")

    @staticmethod
    def _get_column_names(cursor: Cursor) -> list[str]:
        return [column[0] for column in cursor.description]
