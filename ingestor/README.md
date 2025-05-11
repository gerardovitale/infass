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
| `image`          | `string`  | The product image url.                |

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

| Variable Name             | Description                                                    |
|---------------------------|----------------------------------------------------------------|
| `TEST_MODE`               | Enables test mode to limit the number of categories processed. |
| `INGESTION_MERC_PATH`     | The Google Cloud Storage bucket URI for data upload.           |

---

## Usage

### Local Run

1. Build the Docker image:
    ```shell
    docker buildx build -f Dockerfile -t ingestor .
    ```

2. Run the container:
    ```shell
    docker run --rm --name ingestor \
		-v $(INGESTOR_OUTPUT_PATH):/app/data/ \
   		-e TEST_MODE=true \
		ingestor:latest
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
