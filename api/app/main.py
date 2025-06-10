import logging
import os

from fastapi import Depends
from fastapi import FastAPI
from fastapi import HTTPException
from models import ProductSearchResponse
from repositories.sqlite_product_repo import SQLiteProductRepository
from services import ProductService

# Configure logging to be compatible with Uvicorn/FastAPI
logger = logging.getLogger("uvicorn.error")

app = FastAPI(
    title="Infass API",
    description="API for Infass, a product search service",
)


def get_product_service() -> ProductService:
    db_path = os.environ["SQLITE_DB_PATH"]
    logger.info(f"Creating ProductService with SQLite DB at {db_path}")
    product_repo = SQLiteProductRepository(db_path)
    return ProductService(product_repo)


@app.get("/products/search", response_model=ProductSearchResponse)
def search_products(search_term: str, service: ProductService = Depends(get_product_service)):
    logger.info(f"Search products endpoint called with search_term='{search_term}'")
    if not search_term:
        logger.warning("Search term is empty")
        raise HTTPException(status_code=400, detail="Search term cannot be empty")
    result = service.search(search_term)
    logger.info(f"Search completed for term '{search_term}', found {result.total_results} results")
    return result
