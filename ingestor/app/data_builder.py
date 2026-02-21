import logging
from datetime import datetime
from typing import Any
from typing import Dict
from typing import Generator
from typing import List

import pandas as pd
from timing import timed_phase

logger = logging.getLogger(__name__)


def build_df(product_gen: Generator[Dict[str, Any], None, None]) -> pd.DataFrame:
    logger.info("Building dataframes")
    df = pd.DataFrame(product_gen)
    df["date"] = datetime.now().date().isoformat()
    logger.info(f"Built dataframe of shape and size: {df.shape}, {df.size}")
    return df


@timed_phase("data_building")
def build_data_gen(
    product_gen_list: List[Generator[Dict[str, Any], None, None]],
) -> Generator[pd.DataFrame, None, None]:
    logger.info("Building data generator")
    return (build_df(product_gen) for product_gen in product_gen_list)
