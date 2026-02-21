import logging
from abc import ABC
from abc import abstractmethod

logger = logging.getLogger("uvicorn.error")


class ProductRepository(ABC):

    @abstractmethod
    def search_products(self, search_term: str, limit: int = 20, offset: int = 0) -> list[dict]:
        raise NotImplementedError()

    @abstractmethod
    def count_search_products(self, search_term: str) -> int:
        raise NotImplementedError()

    def get_enriched_product(self, search_term: str) -> dict:
        raise NotImplementedError()
