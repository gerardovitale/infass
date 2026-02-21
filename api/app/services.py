import logging
from typing import List

from models import EnrichedProduct
from models import Product
from repositories.product_repo import ProductRepository

logger = logging.getLogger("uvicorn.error")


class ProductService:
    def __init__(self, product_repository: ProductRepository):
        self.repo = product_repository

    def search(self, query: str, limit: int = 20, offset: int = 0) -> tuple[List[Product], int]:
        logger.info(f"ProductService - Searching for products with query '{query}' (limit={limit}, offset={offset})")
        query = query.lower()
        products = self.repo.search_products(query, limit=limit, offset=offset)
        total_count = self.repo.count_search_products(query)
        logger.info(f"ProductService - Found {len(products)} products (total: {total_count}) for query '{query}'")
        return [Product(**product) for product in products], total_count

    def get_enriched_product(self, product_id: str) -> EnrichedProduct:
        logger.info(f"ProductService - Getting enriched product for product_id '{product_id}'")
        enriched_product = self.repo.get_enriched_product(product_id)
        return EnrichedProduct(**enriched_product)
