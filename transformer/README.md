# Transformer Module

This module is responsible for transforming product price data from a cloud storage bucket, processing it through a
series of transformations, and writing the transformed data to a BigQuery table or local files. The module is designed
to handle medium size datasets efficiently and includes various data cleaning, transformation, and enrichment steps.

---

## Table of Contents

1. [Overview](#overview)
2. [Input Data](#input-data)
3. [Transformations](#transformations)
4. [Output Data](#output-data)
5. [Data Flow](#data-flow)
6. [Configuration](#configuration)
7. [Usage](#usage)
8. [Dependencies](#dependencies)

---

## Overview

The module processes product price data stored in a Google Cloud Storage bucket. It performs the following:

- Reads raw CSV files from the bucket.
- Applies a series of transformations to clean, standardize, and enrich the data.
- Writes the transformed data to a BigQuery table or local files in the case of local runs.

---

## Input Data

The input data is read from a Google Cloud Storage bucket. Each file is expected to be in CSV format with the following
schema:

| Column Name      | Data Type   | Description                                  |
|------------------|-------------|----------------------------------------------|
| `date`           | `timestamp` | The date of the product price record.        |
| `name`           | `string`    | The name of the product.                     |
| `size`           | `string`    | The size of the product.                     |
| `category`       | `string`    | The category and subcategory of the product. |
| `original_price` | `float`     | The original price of the product.           |
| `discount_price` | `float`     | The discounted price of the product.         |

---

## Transformations

The following transformations are applied to the input data:

1. **Date Casting**: Converts the `date` column to a `datetime64[ns]` format.
2. **Price Casting**: Converts `original_price` and `discount_price` columns to `float32` after removing currency
   symbols and formatting inconsistencies.
3. **Category Splitting**: Splits the `category` column into `category` and `subcategory` based on a separator (`>`).
4. **String Standardization**: Standardizes string columns (`name`, `size`, `category`, `subcategory`) by:
    - Converting to lowercase.
    - Removing accents and special characters.
    - Stripping whitespace.
5. **Category Mapping**: Updates old categories and subcategories based on recent entries.
6. **Deduplication**: Removes duplicate product entries with different prices for the same date.
7. **Price Column Addition**: Adds a `price` column, which is the discounted price if available, otherwise the original
   price.
8. **Sorting**: Sorts the data by `name`, `size`, and `date` to compute downstream operations.
9. **Previous Value Columns**: Adds `prev_price` and `prev_original_price` columns to store the previous day's price for
   each product.
10. **Price Variation Columns**: Adds absolute and percentage price variation
    columns (`price_var_abs`, `price_var_%`, `original_price_var_abs`, `original_price_var_%`) based on the prev price
    generated before.
11. **Moving Averages**: Adds 7-day, 15-day, and 30-day moving averages for the `price` column.
12. **Fake Discount Detection**: [WIP]

---

## Output Data

The transformed data is written with the following schema:

| Column Name              | Data Type        | Description                                          |
|--------------------------|------------------|------------------------------------------------------|
| `date`                   | `datetime64[ns]` | The date of the product price record.                |
| `dedup_id`               | `int8`           | Unique identifier for deduplicated records.          |
| `name`                   | `string`         | The name of the product.                             |
| `size`                   | `string`         | The size of the product.                             |
| `category`               | `category`       | The category of the product.                         |
| `subcategory`            | `category`       | The subcategory of the product.                      |
| `price`                  | `float32`        | The current price of the product.                    |
| `prev_price`             | `float32`        | The previous day's price of the product.             |
| `price_ma_7`             | `float32`        | 7-day moving average of the price.                   |
| `price_ma_15`            | `float32`        | 15-day moving average of the price.                  |
| `price_ma_30`            | `float32`        | 30-day moving average of the price.                  |
| `original_price`         | `float32`        | The original price of the product.                   |
| `prev_original_price`    | `float32`        | The previous day's original price of the product.    |
| `discount_price`         | `float32`        | The discounted price of the product.                 |
| `price_var_abs`          | `float32`        | Absolute variation in price day-over-day.            |
| `price_var_%`            | `float32`        | Percentage variation in price day-over-day.          |
| `original_price_var_abs` | `float32`        | Absolute variation in original price day-over-day.   |
| `original_price_var_%`   | `float32`        | Percentage variation in original price day-over-day. |
| `is_fake_discount`       | `boolean`        | Flag indicating if the discount is fake.             |

---

## Data Flow

1. **Data Source**:
    - Reads raw CSV files from a Google Cloud Storage bucket using the `bucket_reader.py` module.

2. **Transformations**:
    - Processes the data through the `transformer.py` module, applying the transformations listed above.

3. **Data Sink**:
    - Writes the transformed data to:
        - **BigQuery**: If running in production mode.
        - **Local Files**: If running in local mode (for debugging or testing).

---

## Configuration

The module uses the following environment variables for configuration:

| Variable Name       | Description                                        |
|---------------------|----------------------------------------------------|
| `DATA_SOURCE`       | The name of the Google Cloud Storage bucket.       |
| `DESTINATION`       | The BigQuery table to write the transformed data.  |
| `TRANSFORMER_LIMIT` | Limits the number of files to process.             |
| `IS_LOCAL_RUN`      | Flag to indicate if the module is running locally. |

If `LOCAL_RUN` is set to `true`, the module will write the output to local files instead of BigQuery.

| Variable Name              | Description                                       |
|----------------------------|---------------------------------------------------|
| TRANSFORMER_OUTPUT_PATH    | Local path where fetched data will be written     |
| GCP_TRANSFORMER_CREDS_PATH | Creds to fetch data from Google Cloud Storage     |
| GCP_PROJECT_ID             | Google Cloud Project ID where the data is located |

---

## Usage

### Local Run

1. Build the Docker image:
    ```shell
    docker buildx build -f Dockerfile -t transformer .
    ```

2. Run the container:
    ```shell
    docker run --rm \
		-v $(TRANSFORMER_OUTPUT_PATH):/app/data/ \
		-v $(GCP_TRANSFORMER_CREDS_PATH):/app/key.json \
		-e GOOGLE_APPLICATION_CREDENTIALS=/app/key.json \
		-e DATA_SOURCE=infass-merc \
		-e DESTINATION=$(GCP_PROJECT_ID).infass.merc \
		-e TRANSFORMER_LIMIT=7 \
		-e IS_LOCAL_RUN=true \
		transformer:latest
    ```

### Local Unit Test

1. Build the test Docker image:
    ```shell
    docker buildx build -f Dockerfile.test -t transformer-test .
    ```

2. Run the tests:
    ```shell
    docker run --rm transformer-test:latest
    ```

---

## Dependencies

The module uses the following dependencies, which can be found in the `requirements.txt` file. These are included in the
dockerfiles, so it is unnecessary to install them manually.

### Python Libraries

- `pandas`
- `google-cloud-storage`
- `google-cloud-bigquery`
- `pyarrow`
