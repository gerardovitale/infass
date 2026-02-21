from datetime import date
from typing import List
from typing import Optional

from pydantic import BaseModel


class Product(BaseModel):
    id: str
    name: str
    size: str
    categories: str
    subcategories: str
    current_price: float
    image_url: Optional[str]


class ProductPriceDetails(BaseModel):
    date: date
    price: float
    sma7: Optional[float]
    sma15: Optional[float]
    sma30: Optional[float]


class EnrichedProduct(Product):
    price_details: List[ProductPriceDetails]


class ProductSearchResponse(BaseModel):
    query: str
    total_results: int
    results: List[Product]
    limit: int
    offset: int
    has_more: bool
