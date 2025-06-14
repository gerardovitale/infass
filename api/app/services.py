import logging
from typing import List

from models import EnrichedProduct
from models import Product
from repositories.product_repo import ProductRepository

logger = logging.getLogger("uvicorn.error")


class ProductService:
    def __init__(self, product_repository: ProductRepository):
        self.repo = product_repository

    def search(self, query: str) -> List[Product]:
        logger.info(f"ProductService - Searching for products with query '{query}'")
        query = query.lower()
        products = self.repo.search_products(query)
        logger.info(f"ProductService - Found {len(products)} products for query '{query}'")
        return [Product(**product) for product in products]

    def get_enriched_product(self, product_id: str) -> EnrichedProduct:
        logger.info(f"ProductService - Getting enriched product for product_id '{product_id}'")
        enriched_product = self.repo.get_enriched_product(product_id)
        return EnrichedProduct(**enriched_product)
