import logging

from models import Product
from models import ProductSearchResponse
from repositories.product_repo import ProductRepository

logger = logging.getLogger("uvicorn.error")


class ProductService:
    def __init__(self, product_repository: ProductRepository):
        self.repo = product_repository

    def search(self, query: str) -> ProductSearchResponse:
        logger.info(f"ProductService: Searching for products with query '{query}'")
        query = query.lower()
        products = self.repo.search_products(query)
        logger.info(f"ProductService: Found {len(products)} products for query '{query}'")
        return ProductSearchResponse(
            query=query, total_results=len(products), results=[Product(**product) for product in products]
        )
