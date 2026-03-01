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

    def search_products(self, search_term: str, limit: int = 20, offset: int = 0) -> tuple[list[dict], int]:
        logger.info(f"BigQuery: Searching products with term '{search_term}' (limit={limit}, offset={offset})")
        query = f"""
            SELECT
                product_id as id,
                name,
                size,
                categories,
                subcategories,
                price,
                image_url,
                COUNT(*) OVER() AS total_count
            FROM `{self.project_id}.{self.dataset_id}.dbt_ref_products`
            {self._build_where_clause(search_term)}
            LIMIT {limit} OFFSET {offset}
            """
        rows = self.bq.query(query).result()
        results = []
        total_count = 0
        for row in rows:
            row_dict = dict(row.items())
            total_count = row_dict.pop("total_count", 0)
            results.append(row_dict)
        logger.info(f"BigQuery: Found {len(results)} products (total: {total_count}) for term '{search_term}'")
        return results, total_count

    def get_enriched_product(self, product_id: str, months: int = 6) -> dict:
        raise NotImplementedError("get_enriched_product is not implemented for BigQuery repository")
