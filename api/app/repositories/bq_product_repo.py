import logging

from google.cloud.bigquery import Client
from repositories.product_repo import ProductRepository

logger = logging.getLogger("uvicorn.error")


class BigQueryProductRepository(ProductRepository):

    def __init__(self, project_id: str, dataset_id: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bq = Client(project=project_id)

    def _build_where_clause(self, search_term: str) -> str:
        return f"""
            WHERE LOWER(name) LIKE '%{search_term}%'
               OR LOWER(size) LIKE '%{search_term}%'
               OR LOWER(categories) LIKE '%{search_term}%'
               OR LOWER(subcategories) LIKE '%{search_term}%'
        """

    def search_products(self, search_term: str, limit: int = 20, offset: int = 0) -> list[dict]:
        logger.info(f"BigQuery: Searching products with term '{search_term}' (limit={limit}, offset={offset})")
        query = f"""
            SELECT
                product_id as id,
                name,
                size,
                categories,
                subcategories,
                price,
                image_url
            FROM `{self.project_id}.{self.dataset_id}.dbt_ref_products`
            {self._build_where_clause(search_term)}
            LIMIT {limit} OFFSET {offset}
            """
        rows = self.bq.query(query).result()
        results = [dict(row.items()) for row in rows]
        logger.info(f"BigQuery: Found {len(results)} products for term '{search_term}'")
        return results

    def count_search_products(self, search_term: str) -> int:
        logger.info(f"BigQuery: Counting products with term '{search_term}'")
        query = f"""
            SELECT COUNT(*) as cnt
            FROM `{self.project_id}.{self.dataset_id}.dbt_ref_products`
            {self._build_where_clause(search_term)}
            """
        rows = self.bq.query(query).result()
        count = list(rows)[0]["cnt"]
        logger.info(f"BigQuery: Total count for term '{search_term}': {count}")
        return count
