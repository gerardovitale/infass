import os

from fastapi import Depends
from fastapi import FastAPI
from models import ProductSearchResponse
from repositories import BigQueryProductRepository
from services import ProductService

app = FastAPI(
    title="Infass API",
    description="API for Infass, a product search service",
)


@app.get("/")
def read_root():
    return {"message": "Hello, FastAPI!"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: str = None):
    return {"item_id": item_id, "query": q}


def get_product_service() -> ProductService:
    return ProductService(
        BigQueryProductRepository(
            project_id=os.environ["PROJECT_ID"],
            dataset_id=os.environ["PRODUCT_DATASET_ID"],
        )
    )


@app.get("/products/search", response_model=ProductSearchResponse)
def search_products(search_term: str, service: ProductService = Depends(get_product_service)):
    return service.search(search_term)
