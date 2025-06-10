import os

from fastapi import Depends
from fastapi import FastAPI
from fastapi import HTTPException
from models import ProductSearchResponse
from repositories import SQLiteProductRepository
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
    product_repo = SQLiteProductRepository(os.environ["SQLITE_DB_PATH"])
    return ProductService(product_repo)


@app.get("/products/search", response_model=ProductSearchResponse)
def search_products(search_term: str, service: ProductService = Depends(get_product_service)):
    if not search_term:
        raise HTTPException(status_code=400, detail="Search term cannot be empty")
    return service.search(search_term)
