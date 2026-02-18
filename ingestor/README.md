# Ingestor Module

This module is responsible for extracting product price data from a web source, processing it into structured data, and
uploading it to a Google Cloud Storage bucket. It is designed to handle dynamic web scraping, data cleaning, and
efficient data storage.

---

## Table of Contents

1. [Overview](#overview)
2. [Transformation](#transformation)
3. [Output Data](#output-data)
4. [Data Flow](#data-flow)
5. [Configuration](#configuration)
6. [Usage](#usage)
7. [Dependencies](#dependencies)

---

## Overview

The Ingestor module automates the process of extracting product data from a web source, transforming it into a
structured format, and uploading it to a Google Cloud Storage bucket. It includes the following steps:

- Web scraping using Selenium and BeautifulSoup.
- Data transformation and cleaning using Pandas.
- Uploading the processed data as CSV files to a Google Cloud Storage bucket.

---

## Transformation

The following transformations are applied to the extracted data:

1. **Date Addition**: Adds a `date` column with the current date.
2. **Data Cleaning**: Removes unnecessary whitespace and formats string fields.
3. **Data Structuring**: Converts the raw extracted data into a DataFrame for further processing.

---

## Output Data

The processed data is uploaded to a Google Cloud Storage bucket in CSV format. The output schema is as follows:

| Column Name      | Data Type | Description                           |
|------------------|-----------|---------------------------------------|
| `date`           | `string`  | The date of the product price record. |
| `name`           | `string`  | The name of the product.              |
| `size`           | `string`  | The size of the product.              |
| `category`       | `string`  | The category of the product.          |
| `original_price` | `string`  | The original price of the product.    |
| `discount_price` | `string`  | The discounted price of the product.  |
| `image_url`      | `string`  | The product image url.                |

---

## Data Flow

1. **Web Scraping**:
    - Uses Selenium to navigate the web source and extract product data.
    - BeautifulSoup is used to parse the HTML and extract relevant fields.

2. **Data Transformation**:
    - Processes and clean the extracted data into a structured DataFrame.
    - Adds a `date` column.

3. **Data Upload**:
    - Uploads the processed data as CSV files to a Google Cloud Storage bucket using the Google Cloud Storage API.

---

## Configuration

The module uses the following environment variables for configuration:

| Variable Name          | Description                                                                                                                                |
|------------------------|--------------------------------------------------------------------------------------------------------------------------------------------|
| `TEST_MODE`            | Controls scraping scope and output destination. See table below for accepted values.                                                       |
| `INGESTION_MERC_PATH`  | The Google Cloud Storage bucket URI for data upload.                                                                                       |
| `INGESTOR_OUTPUT_PATH` | [Optional] Required when running locally (any `TEST_MODE` value) to mount the output directory for the local CSV.                         |

### `TEST_MODE` values

| Value      | Scraping scope       | Output              |
|------------|----------------------|---------------------|
| _(not set)_| All categories       | GCS Parquet (production) |
| `full`     | All categories       | Local CSV           |
| Any other  | First category only  | Local CSV           |

---

## Usage

### Local Run

Four `make` targets are available for local runs. All of them build the Docker image first and write output to `INGESTOR_OUTPUT_PATH` as a local CSV instead of uploading to GCS.

| Target | `TEST_MODE` | Scraping scope |
|---|---|---|
| `make ingestor.merc-local-run` | `true` | First category only (quick smoke-test) |
| `make ingestor.carr-local-run` | `true` | First category only (quick smoke-test) |
| `make ingestor.merc-local-full-run` | `full` | All categories (full validation) |
| `make ingestor.carr-local-full-run` | `full` | All categories (full validation) |

Example:
```shell
make ingestor.merc-local-full-run
```

Or manually:
```shell
docker buildx build -f Dockerfile -t ingestor .
docker run --rm --name ingestor \
    -v $(INGESTOR_OUTPUT_PATH):/app/data/ \
    -e TEST_MODE=full \
    ingestor:latest \
    https://tienda.mercadona.es \
    gs://infass-merc/merc
```

### Local Unit Test

1. Build the test Docker image:
    ```shell
    docker buildx build -f Dockerfile.test -t ingestor-test .
    ```

2. Run the tests:
    ```shell
    docker run --rm ingestor-test:latest
    ```

---

## Dependencies

The module uses the following dependencies, which can be found in the `requirements.txt` file. These are included in the
dockerfiles, so it is unnecessary to install them manually.

- `pandas`
- `selenium`
- `webdriver-manager`
- `beautifulsoup4`
- `google-cloud-storage`
