from typing import List
from typing import Optional

from pydantic import BaseModel


class Product(BaseModel):
    id: str
    name: str
    size: str
    categories: str
    subcategories: str
    price: float
    image_url: Optional[str]


class ProductSearchResponse(BaseModel):
    query: str
    total_results: int
    results: List[Product]
