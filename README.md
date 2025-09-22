# BPS Wilayah Data Fetcher

This project contains a Python script to fetch Indonesian administrative division data from the official BPS (Badan Pusat Statistik) API. It traverses the administrative levels (province, regency, district, and village), saves the raw JSON responses, processes them into CSV files, and generates a final SQL dump.

## Features

- Fetches data for specified administrative levels.
- Automatically discovers the latest data period (`periode`).
- Normalizes and processes raw data into a clean, tabular format.
- Generates raw JSON, processed CSV, and SQL outputs.
- Supports concurrent requests for faster data extraction.

## Prerequisites

- Python 3.8+
- [Astral `uv`](https://github.com/astral-sh/uv) for environment and package management.

## Setup

1.  **Create a virtual environment:**
    ```sh
    uv venv
    ```

2.  **Activate the environment:**
    ```sh
    source .venv/bin/activate
    ```

3.  **Install dependencies:**
    ```sh
    uv pip install -r requirements.txt
    ```

## Usage

To fetch the data, run the `fetch_bps_wilayah.py` script. You must provide a valid `Cookie` from a browser session on the BPS website.

```sh
python fetch_bps_wilayah.py --cookie "YOUR_BPS_COOKIE_HERE"
```

For more options, see the script's help message:

```sh
python fetch_bps_wilayah.py --help
```

### Using the Makefile

A `Makefile` is provided for convenience.

-   **`make install`**: Sets up the virtual environment and installs dependencies.
-   **`make fetch`**: Fetches the data. You can pass the cookie as a variable:
    ```sh
    make fetch COOKIE="YOUR_BPS_COOKIE_HERE"
    ```
-   **`make clean`**: Removes all generated data files.

## Data

The script generates the following outputs in the `data/` directory:

-   `data/raw/bps/<periode>/`: Raw JSON responses from the API.
-   `data/processed/bps/<periode>/`: Processed data in CSV format, with a `manifest.json` file.
-   `data/sql/bps_wilayah_<periode>.sql`: A SQL dump of the final processed data.
