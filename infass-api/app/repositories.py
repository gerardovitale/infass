from abc import ABC
from abc import abstractmethod

from google.cloud.bigquery import Client


class ProductRepository(ABC):

    @abstractmethod
    def search_products(self, search_term: str) -> list[dict]:
        raise NotImplementedError()


class BigQueryProductRepository(ProductRepository):

    def __init__(self, project_id: str, dataset_id: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bq = Client(project=project_id)

    def search_products(self, search_term: str) -> list[dict]:
        query = f"""
            SELECT
                product_id,
                name,
                size,
                category,
                subcategory,
                price,
                image_url
            FROM `{self.project_id}.{self.dataset_id}.dbt_unique_products`
            WHERE LOWER(name) LIKE '%{search_term}%'
               OR LOWER(size) LIKE '%{search_term}%'
               OR LOWER(category) LIKE '%{search_term}%'
               OR LOWER(subcategory) LIKE '%{search_term}%'
            """
        rows = self.bq.query(query).result()
        return [dict(row.items()) for row in rows]
