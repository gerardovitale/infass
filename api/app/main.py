import logging
import os

from fastapi import Depends
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Query
from models import EnrichedProduct
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
    logger.info(f"Initializing ProductService with SQLite database path: '{db_path}'")
    product_repo = SQLiteProductRepository(db_path)
    return ProductService(product_repo)


@app.get("/products/search", response_model=ProductSearchResponse)
def search_products(
    search_term: str,
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    service: ProductService = Depends(get_product_service),
):
    logger.info(f"Search products endpoint called with search_term='{search_term}', limit={limit}, offset={offset}")
    if not search_term:
        logger.warning("Search term is empty")
        raise HTTPException(status_code=400, detail="Search term cannot be empty")
    product_list, total_count = service.search(search_term, limit=limit, offset=offset)
    has_more = (offset + limit) < total_count
    logger.info(f"Search completed for term '{search_term}', found {len(product_list)} results (total: {total_count})")
    return ProductSearchResponse(
        query=search_term,
        total_results=total_count,
        results=product_list,
        limit=limit,
        offset=offset,
        has_more=has_more,
    )


@app.get("/products/{product_id}", response_model=EnrichedProduct)
def get_enriched_product(product_id: str, service: ProductService = Depends(get_product_service)):
    logger.info(f"Get product price details for product_id={product_id}")
    enriched_product = service.get_enriched_product(product_id)
    return enriched_product
