from models import Product
from models import ProductSearchResponse
from repositories import ProductRepository


class ProductService:
    def __init__(self, product_repository: ProductRepository):
        self.repo = product_repository

    def search(self, query: str) -> ProductSearchResponse:
        products = self.repo.search_products(query)
        return ProductSearchResponse(
            query=query, total_results=len(products), results=[Product(**product) for product in products]
        )
