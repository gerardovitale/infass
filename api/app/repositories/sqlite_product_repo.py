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
        logger.info(f"SQLite: Searching products with term '{search_term}'")
        conn = self.get_connection()
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

    def get_connection(self) -> Connection:
        logger.info(f"SQLite: Connecting to database at {self.db_path}")
        try:
            return sqlite3.connect(self.db_path)
        except sqlite3.OperationalError:
            logger.error(f"SQLite: Failed to connect to database at {self.db_path}")
            raise sqlite3.OperationalError("Could not connect to the SQLite database.")

    @staticmethod
    def check_db_path_exist(db_path: str) -> None:
        if not Path(db_path).exists():
            logger.error(f"SQLite: Database file not found at {db_path}")
            raise FileNotFoundError(f"Database file not found at {db_path}")

    @staticmethod
    def _get_column_names(cursor: Cursor) -> list[str]:
        return [column[0] for column in cursor.description]
