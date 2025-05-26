from typing import List

from pydantic import BaseModel


class Product(BaseModel):
    product_id: str
    name: str
    size: str
    category: str
    subcategory: str
    price: float
    image_url: str


class ProductSearchResponse(BaseModel):
    query: str
    total_results: int
    results: List[Product]
