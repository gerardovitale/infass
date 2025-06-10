import logging
from abc import ABC
from abc import abstractmethod

logger = logging.getLogger("uvicorn.error")


class ProductRepository(ABC):

    @abstractmethod
    def search_products(self, search_term: str) -> list[dict]:
        raise NotImplementedError()
