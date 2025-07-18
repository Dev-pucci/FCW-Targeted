# FWC Targeted Agreement Scraper

## Overview

This project is a powerful, high-performance Python web scraper designed to extract specific document metadata from the Australian Fair Work Commission (FWC) tribunal search website.

Unlike a general-purpose scraper that downloads everything, this tool is **targeted**. It is given a predefined list of document URLs and intelligently navigates through the paginated search results to find and scrape only those specific documents, making it highly efficient.

The scraper is built with Selenium and leverages multiprocessing to run multiple browser instances in parallel, significantly speeding up the search process. It is robust, featuring automatic retries and detailed logging.

## Key Features

-   **Targeted Scraping**: Only processes and extracts data for URLs specified in a configuration file.
-   **High-Performance Multiprocessing**: Utilizes multiple CPU cores to run several scrapers in parallel, drastically reducing search time.
-   **Robust and Resilient**: Includes an automatic retry mechanism that searches deeper into the website if targets are not found on the first pass.
-   **Dynamic Pagination**: Can be configured to start scraping from any specific page number, allowing you to resume or segment large jobs.
-   **Advanced Data Extraction**: Intelligently parses HTML to extract key metadata, including titles, dates, agreement codes, industry, status, and download links.
-   **Fully Configurable**: All scraping parameters are controlled via an external `config.json` file, requiring no code changes for new jobs.
-   **Detailed Logging**: Outputs a comprehensive `scraper.log` file to monitor progress and diagnose issues.
-   **Headless Operation**: Runs browsers in the background for efficient execution on servers or local machines.
-   **Automatic Driver Management**: Uses `webdriver-manager` to automatically download and manage the correct ChromeDriver.

## Prerequisites

-   Python 3.7+
-   Google Chrome browser installed

## Setup Instructions

1.  **Clone the Repository**
    ```bash
    git clone https://github.com/Dev-pucci/FCW-Targeted.git
    cd FCW-Targeted
    ```

2.  **Create a Virtual Environment** (Recommended)
    ```bash
    # For Windows
    python -m venv venv
    .\venv\Scripts\activate

    # For macOS/Linux
    python3 -m venv venv
    source venv/bin/activate
    ```

3.  **Install Dependencies**
    Create a file named `requirements.txt` in the project root with the following content:
    ```txt
    selenium
    webdriver-manager
    ```
    Then, install the packages using pip:
    ```bash
    pip install -r requirements.txt
    ```

## Configuration (`config.json`)

The scraper's behavior is controlled by a JSON configuration file (e.g., `config1.json`).

```json
{
    "startUrls": [
      "https://tribunalsearch.fwc.gov.au/document-search?q=*&options=SearchType_3%2CSortOrder_agreement-date-desc&page=2400"
    ],
    "maxPages": 1000000,
    "targetPage": 2400,
    "agreementType": null,
    "status": null,
    "downloadDocuments": false,
    "targetUrls": [
      "https://tribunalsearch.fwc.gov.au/document-search/view/3/aHR0c...",
      "https://tribunalsearch.fwc.gov.au/document-search/view/3/aHR0c..."
  ]
}
Use code with caution.
Markdown
Parameter Descriptions:
"startUrls" (array): The initial search result URL(s) to begin scraping from.
"maxPages" (integer): A safety limit on the total number of pages to scrape to prevent infinite loops.
"targetPage" (integer): The starting page number. This is a key feature for resuming scrapes or starting deep within the search results. For a fresh run, this should be 1.
"agreementType" (string | null): Optional filter. Can be set to "Single-enterprise Agreement", "Multi-enterprise Agreement", or "Greenfields Agreement" to narrow the search.
"status" (string | null): Optional filter. Can be set to "Approved", "Current", "Terminated", etc.
"downloadDocuments" (boolean): If true, the scraper will attempt to download the associated PDF for each found target. Defaults to false.
"targetUrls" (array): This is the most important field. A list of the exact document view URLs that the scraper needs to find and extract metadata for.
How to Run
The script is executed from the command line, pointing to your configuration file.
Basic Usage:
Generated bash
python main.py --config config1.json
Use code with caution.
Bash
Command-Line Arguments:
--config (required): Path to the JSON configuration file.
--workers (optional): The number of parallel processes to run. Defaults to 4.
Generated bash
python main.py --config config1.json --workers 8
Use code with caution.
Bash
--pages-per-worker (optional): The maximum number of pages each worker will process in its initial run. Defaults to 5.
--debug (optional): Enables more verbose logging for troubleshooting.
Generated bash
python main.py --config config1.json --debug
Use code with caution.
Bash
Output
The script generates the following in an output/ directory:
CSV File: A file named target_agreements_[timestamp].csv containing the scraped metadata with the following columns:
Title
Approval Date
Expiry Date
Agreement status
Agreement Type
Agreement reference code
Industry
Citation(FWCA Code)
Download URL
Page Number (The page where the item was found)
Worker ID (Which parallel process found the item)
Log File: A scraper.log file in the root directory containing detailed information about the scraping process, including pages visited, targets found, and any errors encountered.
Debug Files (optional): Screenshots (.png) and page source files (.html) are saved to the output/ directory for debugging purposes, especially if an error occurs.
