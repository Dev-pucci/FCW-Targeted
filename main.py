import os
import time
import json
import random
import csv
import re
import logging
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse, urljoin
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import Manager, Lock

import argparse
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('FWCScraper')

class FWCTargetedScraper:
    def __init__(self, config, worker_id=0, shared_data=None):
        """
        Initialize the scraper with configuration
        
        Args:
            config: Dictionary containing scraper configuration
            worker_id: ID of this worker for logging and tracking
            shared_data: Shared data structures between workers
        """
        self.worker_id = worker_id
        self.log_prefix = f"Worker-{worker_id}"
        
        self.start_urls = config.get('startUrls', ['https://tribunalsearch.fwc.gov.au/document-search?q=*&options=SearchType_3%2CSortOrder_agreement-date-desc'])
        self.max_pages = config.get('maxPages', 5)
        self.target_page = config.get('targetPage', 1)  # Start page (defaults to 1 if not provided)
        self.agreement_type = config.get('agreementType', None)
        self.status = config.get('status', None)
        self.download_documents = config.get('downloadDocuments', False)
        self.target_urls = config.get('targetUrls', [])
        self.base_url = "https://tribunalsearch.fwc.gov.au"
        
        # Log target page information
        if self.target_page > 1:
            logger.info(f"{self.log_prefix}: Starting from target page {self.target_page}")
        
        # Use shared data if provided, otherwise create local ones
        if shared_data:
            self.processed_targets = shared_data['processed_targets']
            self.visited_pages = shared_data['visited_pages']
            self.results = shared_data['results']
            self.lock = shared_data['lock']
        else:
            # For single-worker mode
            self.processed_targets = []
            self.visited_pages = []
            self.results = []
            self.lock = Lock()
        
        # Create output directory
        self.output_dir = 'output'
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Create downloads directory if needed
        if self.download_documents:
            os.makedirs(os.path.join(self.output_dir, "downloads"), exist_ok=True)
        
        # Log configuration
        logger.info(f"{self.log_prefix}: Initialized with {len(self.target_urls)} target URLs")
        
        # Set up Chrome options
        self.setup_driver()
        
    def setup_driver(self):
        """Set up the Chrome WebDriver with appropriate options"""
        chrome_options = Options()
        
        # Run headless for workers
        chrome_options.add_argument('--headless')
        
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        
        # Set unique user agent with worker ID to avoid detection
        chrome_options.add_argument(f'--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 FWCWorker/{self.worker_id}')
        
        # Set download directory if downloading documents
        if self.download_documents:
            prefs = {
                "download.default_directory": os.path.abspath(os.path.join(self.output_dir, "downloads", f"worker_{self.worker_id}")),
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "plugins.always_open_pdf_externally": True
            }
            chrome_options.add_experimental_option("prefs", prefs)
        
        # Initialize WebDriver
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        
    def wait_for_element(self, selector, timeout=30, by=By.CSS_SELECTOR):
        """Wait for an element to appear on the page"""
        try:
            element = WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((by, selector))
            )
            return element
        except TimeoutException:
            logger.warning(f"{self.log_prefix}: Timeout waiting for element: {selector}")
            return None
    
    def wait_for_elements(self, selector, timeout=30, by=By.CSS_SELECTOR):
        """Wait for elements to appear on the page"""
        try:
            elements = WebDriverWait(self.driver, timeout).until(
                EC.presence_of_all_elements_located((by, selector))
            )
            return elements
        except TimeoutException:
            logger.warning(f"{self.log_prefix}: Timeout waiting for elements: {selector}")
            return []
    
    def random_delay(self, min_seconds=1, max_seconds=3):
        """Add a random delay to mimic human behavior"""
        delay = random.uniform(min_seconds, max_seconds)
        time.sleep(delay)
        
    def clean_url(self, url):
        """Remove query parameters like '?sid=&q=' from URLs"""
        if not url:
            return url
            
        # Split at the first question mark and keep just the base part
        if '?' in url:
            return url.split('?')[0]
        return url
    
    def take_screenshot(self, name):
        """Take a screenshot and save it to the output directory"""
        filename = f"{self.output_dir}/worker_{self.worker_id}_screenshot_{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        self.driver.save_screenshot(filename)
        logger.debug(f"{self.log_prefix}: Screenshot saved: {filename}")
    
    def save_page_source(self, name):
        """Save the page source HTML for debugging"""
        filename = f"{self.output_dir}/worker_{self.worker_id}_pagesource_{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(self.driver.page_source)
        logger.debug(f"{self.log_prefix}: Page source saved: {filename}")
    
    def is_target_url(self, url):
        """Check if a URL is in the target URLs list (exact match)"""
        if not url or not self.target_urls:
            return False
            
        # Clean the URL first (remove query parameters)
        clean_url = self.clean_url(url)
        
        # Return True if the URL is in the target URLs list
        return clean_url in self.target_urls
    
    def create_paginated_url(self, base_url, page_num):
        """Create a URL for a specific page number"""
        parsed_url = urlparse(base_url)
        query_params = parse_qs(parsed_url.query)
        
        # Add or update the page parameter
        if page_num > 1:
            query_params['page'] = [str(page_num)]
        elif 'page' in query_params:
            # Remove page parameter for page 1
            del query_params['page']
        
        # Reconstruct the URL
        new_query = urlencode(query_params, doseq=True)
        paginated_url = urlunparse((
            parsed_url.scheme,
            parsed_url.netloc,
            parsed_url.path,
            parsed_url.params,
            new_query,
            parsed_url.fragment
        ))
        
        return paginated_url
    
    def process_page(self, url, current_page_num=1):
        """Process a single page of search results"""
        # Acquire lock to check visited pages
        with self.lock:
            # Check if we've already visited this page
            if url in self.visited_pages:
                logger.info(f"{self.log_prefix}: Skipping already visited page: {url}")
                return None
            
            # Mark this page as visited
            self.visited_pages.append(url)
            
            # Check if we've already found all target URLs
            if len(self.processed_targets) >= len(self.target_urls) and self.target_urls:
                logger.info(f"{self.log_prefix}: All target URLs have been processed. Ending search.")
                return None
        
        logger.info(f"{self.log_prefix}: Processing page {current_page_num}: {url}")
        
        try:
            # Navigate to the URL
            self.driver.get(url)
            
            # Take a screenshot before waiting for elements
            self.take_screenshot(f"page-{current_page_num}")
            
            # Log the page title
            logger.info(f"{self.log_prefix}: Page title: {self.driver.title}")
            
            # Wait for search results to load
            result_items = self.wait_for_elements('.fwc-results-item', timeout=60)
            
            if not result_items:
                # If results aren't found, try to perform a search
                try:
                    search_input = self.driver.find_element(By.ID, "input-query")
                    logger.info(f"{self.log_prefix}: On search page, attempting to perform a search for all agreements...")
                    search_input.send_keys("*")
                    search_button = self.driver.find_element(By.CSS_SELECTOR, ".fwc-input-search-icon")
                    search_button.click()
                    
                    self.random_delay(5, 8)
                    
                    # Try again to wait for results
                    result_items = self.wait_for_elements('.fwc-results-item', timeout=60)
                    if not result_items:
                        logger.warning(f"{self.log_prefix}: Still no results after attempting search")
                        return False
                except NoSuchElementException:
                    logger.warning(f"{self.log_prefix}: Search input not found")
                    return False
            
            logger.info(f"{self.log_prefix}: Found {len(result_items)} .fwc-results-item elements")
            
            # Save page source for debugging PDF links
            self.save_page_source(f"page-{current_page_num}")
            
            # Process result items
            found_target_on_page = self.extract_agreements(result_items, current_page_num)
            
            # If we have targets and didn't find any on this page, log it
            if self.target_urls and not found_target_on_page:
                logger.info(f"{self.log_prefix}: No target URLs found on this page.")
            
            # Check for no results (could be at the end of pagination)
            if len(result_items) == 0:
                logger.info(f"{self.log_prefix}: No results found on this page. May have reached the end of results.")
                return None
            
            return True
            
        except Exception as e:
            logger.error(f"{self.log_prefix}: Error processing page: {e}", exc_info=True)
            self.take_screenshot(f"error-page-{current_page_num}")
            return False
    
    def extract_agreements(self, result_items, current_page_num):
        """Extract agreement data from search results, but only for target URLs"""
        logger.info(f"{self.log_prefix}: Extracting data from search results...")
        
        found_target = False
        
        for item in result_items:
            try:
                # First, extract the PDF download URL to see if this is a target
                download_url = None
                
                # STEP 1: Look for the PDF link with img[alt="PDF"]
                try:
                    pdf_links = item.find_elements(By.CSS_SELECTOR, 'a[href^="/document-search/view/"] img[alt="PDF"]')
                    if pdf_links:
                        # Get the parent <a> tag of the first PDF image
                        pdf_link = pdf_links[0].find_element(By.XPATH, '..')
                        pdf_href = pdf_link.get_attribute('href')
                        if pdf_href:
                            # Combine with base URL if it's a relative URL
                            if pdf_href.startswith('/'):
                                full_url = urljoin(self.base_url, pdf_href)
                            else:
                                full_url = pdf_href
                                
                            # Clean the URL by removing query parameters
                            download_url = self.clean_url(full_url)
                except Exception as e:
                    logger.warning(f"{self.log_prefix}: Error extracting PDF link: {e}")
                
                # STEP 2: If not found, try fallback method with .fwc-button
                if not download_url:
                    try:
                        download_button = item.find_element(By.CSS_SELECTOR, '.fwc-button')
                        onclick_attr = download_button.get_attribute('onclick')
                        if onclick_attr:
                            # Parse document ID from onclick attribute
                            match = re.search(r"downloadDocument\(['\"]([\d]+)['\"],[\\s]*['\"](.*?)['\"]\\)", onclick_attr)
                            if match and match.group(1) and match.group(2):
                                document_id = match.group(1)
                                document_name = match.group(2)
                                # Create URL and clean it
                                full_url = f"{self.base_url}/document-search/view/{document_id}/{document_name}"
                                download_url = self.clean_url(full_url)
                    except NoSuchElementException:
                        pass
                
                # Acquire lock to check if this is a target URL and update processed targets
                with self.lock:
                    # Check if this URL is a target URL
                    if not download_url or not self.is_target_url(download_url):
                        # Skip this item if it's not a target
                        continue
                    
                    # Check if we've already processed this target in another worker
                    if download_url in self.processed_targets:
                        logger.info(f"{self.log_prefix}: Target URL already processed by another worker: {download_url}")
                        continue
                    
                    # Record that we've processed this target
                    self.processed_targets.append(download_url)
                    logger.info(f"{self.log_prefix}: Found target URL: {download_url}")
                
                # Track that we found a target
                found_target = True
                
                # Now extract all metadata since this is a target URL
                agreement = {
                    'agreementTitle': "",     # Title
                    'approvalDate': "",       # Approval Date
                    'nominalExpiry': "",      # Expiry Date
                    'status': "",             # Agreement status
                    'agreementType': "",      # Agreement Type
                    'agreementCode': "",      # Agreement reference code
                    'industry': "",           # Industry
                    'fwcaCode': "",           # Citation (FWCA Code)
                    'downloadUrl': download_url,  # Download URL
                    'pageNumber': current_page_num,
                    'workerID': self.worker_id  # Track which worker found this
                }
                
                # Extract title from h3 element
                try:
                    h3_element = item.find_element(By.TAG_NAME, 'h3')
                    agreement['agreementTitle'] = h3_element.text.strip()
                    logger.info(f"{self.log_prefix}: Found title: {agreement['agreementTitle']}")
                    
                    # Extract FWCA code from title if present
                    fwca_match = re.search(r'\[\d{4}\]\s*FWCA\s*\d+', agreement['agreementTitle'])
                    if fwca_match:
                        agreement['fwcaCode'] = fwca_match.group()
                        logger.info(f"{self.log_prefix}: Found FWCA code from title: {agreement['fwcaCode']}")
                except NoSuchElementException:
                    pass
                
                # Extract all chip text and attributes
                chips = item.find_elements(By.CSS_SELECTOR, '.fwc-chip')
                
                for chip in chips:
                    try:
                        text = chip.text.strip()
                        onclick_attr = chip.get_attribute('onclick') or ""
                        
                        logger.debug(f"{self.log_prefix}: Processing chip: {text}")
                        
                        # Check for approval date with or without prefix
                        if "Approved:" in text:
                            agreement['approvalDate'] = text.replace('Approved:', '').strip()
                            logger.info(f"{self.log_prefix}: Found approval date: {agreement['approvalDate']}")
                        # Match date patterns for approval date if no specific label
                        elif not agreement['approvalDate'] and re.match(r'^\d{1,2}\s+[A-Za-z]+\s+\d{4}$', text):
                            agreement['approvalDate'] = text
                            logger.info(f"{self.log_prefix}: Found date (likely approval date): {agreement['approvalDate']}")
                        
                        # Check for nominal expiry / expiry date
                        if "Nominal expiry:" in text:
                            agreement['nominalExpiry'] = text.replace('Nominal expiry:', '').strip()
                            logger.info(f"{self.log_prefix}: Found nominal expiry: {agreement['nominalExpiry']}")
                        
                        # Check for agreement code (AE number)
                        if re.match(r'^AE\d+$', text):
                            agreement['agreementCode'] = text
                            logger.info(f"{self.log_prefix}: Found agreement code: {agreement['agreementCode']}")
                        
                        # Check for FWCA code if not already found in title
                        if not agreement['fwcaCode'] and re.match(r'^\[\d{4}\]\s*FWCA\s*\d+$', text):
                            agreement['fwcaCode'] = text
                            logger.info(f"{self.log_prefix}: Found FWCA code from chip: {agreement['fwcaCode']}")
                        
                        # Check for agreement type
                        if text in ['Single-enterprise Agreement', 'Multi-enterprise Agreement', 'Greenfields Agreement']:
                            agreement['agreementType'] = text
                            logger.info(f"{self.log_prefix}: Found agreement type: {agreement['agreementType']}")
                        
                        # Check for industry
                        industry_keywords = ['industry', 'Building', 'Construction', 'Metal', 'Health', 'Education', 'Mining','services']
                        if any(keyword in text for keyword in industry_keywords):
                            agreement['industry'] = text
                            logger.info(f"{self.log_prefix}: Found industry: {agreement['industry']}")
                        
                        # Check for status
                        status_values = ['Approved', 'Current', 'Terminated', 'Superseded']
                        if text in status_values or "Status:" in text:
                            # Clean up status text if it has a prefix
                            agreement['status'] = text.replace('Status:', '').strip() if "Status:" in text else text
                            logger.info(f"{self.log_prefix}: Found status: {agreement['status']}")
                        
                        # Extract filter information from onclick attribute if present
                        if "applyTagAsFilter" in onclick_attr:
                            filter_match = re.search(r"applyTagAsFilter\(['\"](.*?)['\"],[\\s]*['\"](.*?)['\"]\\)", onclick_attr)
                            if filter_match:
                                filter_type = filter_match.group(1)
                                filter_value = filter_match.group(2)
                                
                                if filter_type == 'Status' and not agreement['status']:
                                    agreement['status'] = filter_value
                                elif filter_type == 'AgreementType' and not agreement['agreementType']:
                                    agreement['agreementType'] = filter_value
                                elif filter_type == 'Industry' and not agreement['industry']:
                                    agreement['industry'] = filter_value
                    except StaleElementReferenceException:
                        continue
                
                # Add the agreement data to results
                with self.lock:
                    self.results.append(agreement)
                logger.info(f"{self.log_prefix}: Added target agreement: {agreement['agreementTitle']}")
                
                # Check if we've found all target URLs (with lock to avoid race condition)
                with self.lock:
                    if len(self.processed_targets) >= len(self.target_urls) and self.target_urls:
                        logger.info(f"{self.log_prefix}: All target URLs have been processed. Can stop searching.")
                        break
                
            except Exception as e:
                logger.error(f"{self.log_prefix}: Error processing result item: {e}", exc_info=True)
        
        # Report results
        with self.lock:
            logger.info(f"{self.log_prefix}: Found {len(self.processed_targets)} out of {len(self.target_urls)} target URLs so far")
        
        return found_target
    
    def apply_filters(self, url):
        """Apply agreement type and status filters to the URL if specified"""
        if not (self.agreement_type or self.status):
            return url
        
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        
        # Add or update options parameter
        options = query_params.get('options', [''])[0]
        options_list = options.split(',') if options else []
        
        if self.agreement_type:
            # Remove any existing agreement type filters
            options_list = [opt for opt in options_list if not opt.startswith('AgreementType_')]
            # Add the new filter
            options_list.append(f"AgreementType_{self.agreement_type.replace(' ', '_')}")
        
        if self.status:
            # Remove any existing status filters
            options_list = [opt for opt in options_list if not opt.startswith('Status_')]
            # Add the new filter
            options_list.append(f"Status_{self.status.replace(' ', '_')}")
        
        # Update the options parameter
        query_params['options'] = [','.join(options_list)]
        
        # Reconstruct the URL
        new_query = urlencode(query_params, doseq=True)
        filtered_url = urlunparse((
            parsed_url.scheme,
            parsed_url.netloc,
            parsed_url.path,
            parsed_url.params,
            new_query,
            parsed_url.fragment
        ))
        
        return filtered_url
    
    def process_url_range(self, base_url, start_page, end_page):
        """Process a range of pages and return found results"""
        logger.info(f"{self.log_prefix}: Processing URL range from page {start_page} to {end_page} for {base_url}")
        
        for current_page in range(start_page, end_page + 1):
            # Generate URL for current page
            page_url = self.create_paginated_url(base_url, current_page)
            
            # Process the page
            result = self.process_page(page_url, current_page)
            
            # Break if there was an error or we reached the end of results
            if result is None:
                logger.info(f"{self.log_prefix}: Stopping pagination at page {current_page}")
                break
            
            # Check if we've found all target URLs
            with self.lock:
                if len(self.processed_targets) >= len(self.target_urls) and self.target_urls:
                    logger.info(f"{self.log_prefix}: All target URLs have been processed. Stopping search.")
                    break
            
            # Add a delay between page requests to avoid being rate-limited
            self.random_delay(2, 4)
        
        # Clean up the driver when done with this range
        self.driver.quit()
        
        return len(self.processed_targets)
    
    def run(self):
        """Run the scraper using URL-based pagination (single worker mode)"""
        if not self.target_urls:
            logger.warning("No target URLs provided. The scraper will not extract any agreements.")
            return
        
        try:
            # Process start URLs
            for start_url in self.start_urls:
                # Apply filters if specified
                base_url = self.apply_filters(start_url)
                logger.info(f"{self.log_prefix}: Starting with base URL: {base_url}")
                
                # Initialize page counter with the target page
                current_page = self.target_page
                logger.info(f"{self.log_prefix}: Beginning from page {current_page} (configured target page)")
                
                # Process pages until we reach max_pages or find all targets
                while current_page <= self.max_pages:
                    # Generate URL for current page
                    page_url = self.create_paginated_url(base_url, current_page)
                    
                    # Process the page
                    result = self.process_page(page_url, current_page)
                    
                    # Break if there was an error or we reached the end of results
                    if result is None:
                        logger.info(f"{self.log_prefix}: Stopping pagination at page {current_page}")
                        break
                    
                    # Check if we've found all target URLs
                    if len(self.processed_targets) >= len(self.target_urls):
                        logger.info(f"{self.log_prefix}: All target URLs have been processed. Stopping search.")
                        break
                    
                    # Move to next page
                    current_page += 1
                    
                    # Add a delay between page requests
                    self.random_delay(2, 4)
                
                # Check if we've found all target URLs across all start URLs
                if len(self.processed_targets) >= len(self.target_urls):
                    logger.info(f"{self.log_prefix}: All target URLs have been processed. No need to process additional start URLs.")
                    break
            
            # Summary of results
            logger.info(f"{self.log_prefix}: Completed scraping. Found {len(self.results)} agreements out of {len(self.target_urls)} target URLs.")
            
            # List of target URLs that weren't found
            if self.target_urls:
                missing_targets = [url for url in self.target_urls if url not in self.processed_targets]
                if missing_targets:
                    logger.warning(f"{self.log_prefix}: Could not find {len(missing_targets)} target URLs:")
                    for url in missing_targets:
                        logger.warning(f" - {url}")
            
            # Export results to CSV
            self.export_to_csv()
            
        except Exception as e:
            logger.error(f"{self.log_prefix}: Error during scraping: {e}", exc_info=True)
        finally:
            # Clean up
            self.driver.quit()
    
    def export_to_csv(self):
        """Export results to CSV file"""
        if not self.results:
            logger.warning(f"{self.log_prefix}: No results to export")
            return
        
        output_file = f"{self.output_dir}/target_agreements_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        headers = [
            'Title', 
            'Approval Date', 
            'Expiry Date', 
            'Agreement status', 
            'Agreement Type',
            'Agreement reference code', 
            'Industry', 
            'Citation(FWCA Code)', 
            'Download URL',
            'Page Number',
            'Worker ID'  # Add worker ID for debugging
        ]
        
        field_mapping = {
            'agreementTitle': 'Title',
            'approvalDate': 'Approval Date',
            'nominalExpiry': 'Expiry Date',
            'status': 'Agreement status',
            'agreementType': 'Agreement Type',
            'agreementCode': 'Agreement reference code',
            'industry': 'Industry',
            'fwcaCode': 'Citation(FWCA Code)',
            'downloadUrl': 'Download URL',
            'pageNumber': 'Page Number',
            'workerID': 'Worker ID'
        }
        
        try:
            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                writer.writeheader()
                
                for result in self.results:
                    # Rename fields according to the mapping
                    row = {field_mapping.get(k, k): v for k, v in result.items() if k in field_mapping}
                    writer.writerow(row)
            
            logger.info(f"{self.log_prefix}: Exported {len(self.results)} records to {output_file}")
        except Exception as e:
            logger.error(f"{self.log_prefix}: Error exporting results to CSV: {e}", exc_info=True)


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Fair Work Commission Targeted Agreement Scraper')
    parser.add_argument('--config', required=True, help='Path to JSON configuration file')
    parser.add_argument('--workers', type=int, default=4, help='Number of worker processes')
    parser.add_argument('--pages-per-worker', type=int, default=5, help='Maximum pages per worker')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode (more logging)')
    return parser.parse_args()


def worker_task(worker_id, config, base_url, page_range, shared_data):
    """Task function for a worker process"""
    try:
        # Create a worker-specific scraper
        worker_scraper = FWCTargetedScraper(config, worker_id=worker_id, shared_data=shared_data)
        
        # Process the assigned page range
        start_page, end_page = page_range
        result = worker_scraper.process_url_range(base_url, start_page, end_page)
        
        return f"Worker {worker_id} processed pages {start_page}-{end_page}, found {result} targets"
    except Exception as e:
        logger.error(f"Worker {worker_id} encountered an error: {e}", exc_info=True)
def run_multiprocessing_scraper(config, num_workers=4, pages_per_worker=5):
    """
    Run the scraper with multiple workers processing different page ranges
    
    Args:
        config: Scraper configuration dictionary
        num_workers: Number of worker processes to use
        pages_per_worker: Maximum number of pages each worker will process
    """
    if not config.get('targetUrls', []):
        logger.warning("No target URLs provided. The scraper will not extract any agreements.")
        return
    
    # Create shared data structures for workers to communicate
    with Manager() as manager:
        # Create shared collections for workers
        # Note: Manager doesn't have a direct set() method, we need to use list() and manage it as a set-like structure
        shared_data = {
            'processed_targets': manager.list(),  # We'll use this as a set-like structure
            'visited_pages': manager.list(),      # We'll use this as a set-like structure
            'results': manager.list(),
            'lock': manager.Lock()
        }
        
        # Get the target page from config (default to 1)
        target_page = config.get('targetPage', 1)
        
        # Calculate the total number of pages to process
        total_pages = min(config.get('maxPages', 100), target_page + (num_workers * pages_per_worker))
        
        # Apply filters to start URL
        start_url = config.get('startUrls', ['https://tribunalsearch.fwc.gov.au/document-search?q=*&options=SearchType_3%2CSortOrder_agreement-date-desc'])[0]
        
        # Create a temporary scraper to apply filters
        temp_scraper = FWCTargetedScraper({**config, 'maxPages': 1})
        filtered_url = temp_scraper.apply_filters(start_url)
        temp_scraper.driver.quit()
        
        logger.info(f"Starting multiprocessing scraper with {num_workers} workers")
        logger.info(f"Base URL: {filtered_url}")
        logger.info(f"Starting from target page: {target_page}")
        logger.info(f"Total pages to process: {total_pages}")
        
        # Create a list of page ranges for workers
        page_ranges = []
        
        # Determine which pages each worker should process
        # We'll use a staggered approach - workers handle interleaved pages to increase chances of finding targets quickly
        for worker_id in range(num_workers):
            # Calculate starting point for this worker based on target page
            worker_start = target_page + worker_id
            
            # Create a list of pages for this worker with interleaving
            worker_pages = [worker_start + (num_workers * i) for i in range(pages_per_worker)]
            
            # Filter out pages beyond the maximum
            worker_pages = [p for p in worker_pages if p <= total_pages]
            
            if worker_pages:
                page_ranges.append((worker_id, (min(worker_pages), max(worker_pages))))
        
        # Create a list of tasks for each worker
        tasks = []
        for worker_id, page_range in page_ranges:
            tasks.append((worker_id, config, filtered_url, page_range, shared_data))
        
        # Use ProcessPoolExecutor to run the workers in parallel
        logger.info(f"Launching {len(tasks)} worker tasks")
        
        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            futures = [executor.submit(worker_task, *task) for task in tasks]
            
            # Process results as they complete
            for future in as_completed(futures):
                try:
                    result = future.result()
                    logger.info(f"Worker result: {result}")
                except Exception as e:
                    logger.error(f"Worker encountered an exception: {e}")
        
        # Convert manager collections to regular Python collections
        results = list(shared_data['results'])
        processed_targets = list(shared_data['processed_targets'])
        
        # Summary of results
        logger.info(f"Completed multiprocessing scraping. Found {len(results)} agreements out of {len(config.get('targetUrls', []))} target URLs.")
        
        # List of target URLs that weren't found
        if config.get('targetUrls', []):
            # Find missing targets - those in target_urls but not in processed_targets
            missing_targets = [url for url in config['targetUrls'] if url not in processed_targets]
            if missing_targets:
                logger.warning(f"Could not find {len(missing_targets)} target URLs:")
                for url in missing_targets:
                    logger.warning(f" - {url}")
        
        # Export results to CSV if any were found
        if results:
            export_results_to_csv(results, config.get('targetUrls', []))
        else:
            logger.warning("No results to export")


def export_results_to_csv(results, target_urls):
    """Export results to CSV file (standalone function for multiprocessing mode)"""
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    
    output_file = f"{output_dir}/target_agreements_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    headers = [
        'Title', 
        'Approval Date', 
        'Expiry Date', 
        'Agreement status', 
        'Agreement Type',
        'Agreement reference code', 
        'Industry', 
        'Citation(FWCA Code)', 
        'Download URL',
        'Page Number',
        'Worker ID'  # Add worker ID for debugging
    ]
    
    field_mapping = {
        'agreementTitle': 'Title',
        'approvalDate': 'Approval Date',
        'nominalExpiry': 'Expiry Date',
        'status': 'Agreement status',
        'agreementType': 'Agreement Type',
        'agreementCode': 'Agreement reference code',
        'industry': 'Industry',
        'fwcaCode': 'Citation(FWCA Code)',
        'downloadUrl': 'Download URL',
        'pageNumber': 'Page Number',
        'workerID': 'Worker ID'
    }
    
    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            
            for result in results:
                # Convert from manager.dict to regular dict
                result_dict = dict(result)
                # Rename fields according to the mapping
                row = {field_mapping.get(k, k): v for k, v in result_dict.items() if k in field_mapping}
                writer.writerow(row)
        
        logger.info(f"Exported {len(results)} records to {output_file}")
    except Exception as e:
        logger.error(f"Error exporting results to CSV: {e}", exc_info=True)


def retry_scraper(config, max_retries=3):
    """
    Run the scraper with retries for any missing target URLs
    
    Args:
        config: Scraper configuration dictionary
        max_retries: Maximum number of retry attempts
    """
    original_targets = list(config.get('targetUrls', []))
    remaining_targets = original_targets.copy()
    
    # First attempt with multiprocessing
    logger.info(f"Initial scraper run - searching for {len(remaining_targets)} targets")
    run_multiprocessing_scraper(config)
    
    # Check output directory for CSV files to determine which targets were found
    output_dir = 'output'
    csv_files = [f for f in os.listdir(output_dir) if f.endswith('.csv') and f.startswith('target_agreements_')]
    
    if not csv_files:
        logger.warning("No results found from initial run. Starting retry attempts.")
    else:
        # Get the most recent CSV file
        latest_csv = sorted(csv_files)[-1]
        found_urls = []
        
        # Read the CSV to find which targets were found
        try:
            with open(os.path.join(output_dir, latest_csv), 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if 'Download URL' in row and row['Download URL']:
                        found_urls.append(row['Download URL'])
            
            # Update remaining targets - remove found URLs from the remaining targets list
            remaining_targets = [url for url in remaining_targets if url not in found_urls]
            logger.info(f"Found {len(found_urls)} targets in initial run. {len(remaining_targets)} targets remain.")
        except Exception as e:
            logger.error(f"Error reading results CSV: {e}", exc_info=True)
    
    # Retry for any remaining targets
    retry_count = 0
    while remaining_targets and retry_count < max_retries:
        retry_count += 1
        logger.info(f"Retry attempt {retry_count}/{max_retries} - searching for {len(remaining_targets)} targets")
        
        # Update config with just the remaining targets
        retry_config = config.copy()
        retry_config['targetUrls'] = remaining_targets
        
        # For retries, increase the target page to search deeper
        if 'targetPage' in config:
            retry_config['targetPage'] = config['targetPage'] + (retry_count * 100)
        else:
            # If no target page was specified, start deeper into the results
            retry_config['targetPage'] = 100 * retry_count
        
        # Run the scraper in single-worker mode with increased max pages for retries
        retry_config['maxPages'] = config.get('maxPages', 100) + (retry_count * 100)
        single_scraper = FWCTargetedScraper(retry_config)
        single_scraper.run()
        
        # Check which targets were found in this retry
        if single_scraper.results:
            logger.info(f"Retry {retry_count} found {len(single_scraper.results)} targets")
            found_in_retry = [result['downloadUrl'] for result in single_scraper.results]
            
            # Update remaining targets - remove newly found URLs from the remaining targets list
            remaining_targets = [url for url in remaining_targets if url not in found_in_retry]
            
            if not remaining_targets:
                logger.info("All targets found! No further retries needed.")
                break
        else:
            logger.warning(f"Retry {retry_count} found no targets")
    
    # Final report
    if remaining_targets:
        logger.warning(f"After {retry_count + 1} attempts, {len(remaining_targets)} targets still not found:")
        for url in remaining_targets:
            logger.warning(f" - {url}")
    else:
        logger.info("Successfully found all target URLs!")


def main():
    """Main function to run the scraper"""
    args = parse_arguments()
    
    # Set logging level based on debug flag
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logger.debug("Debug mode enabled")
    
    # Load configuration from file
    if not os.path.exists(args.config):
        logger.error(f"Error: Configuration file '{args.config}' not found.")
        exit(1)
        
    with open(args.config, 'r') as config_file:
        try:
            config = json.load(config_file)
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing configuration file: {e}")
            exit(1)
    
    # Validate configuration
    if 'targetUrls' not in config or not isinstance(config['targetUrls'], list):
        logger.warning("Warning: No target URLs provided in configuration or 'targetUrls' is not a list.")
        config['targetUrls'] = []
    
    # Update max pages based on argument
    if args.pages_per_worker:
        config['maxPages'] = max(config.get('maxPages', 100), args.workers * args.pages_per_worker)
    
    # Print configuration summary
    logger.info("Running targeted scraper with configuration:")
    logger.info(f"Start URLs: {config.get('startUrls', ['(default)'])}")
    logger.info(f"Target Page: {config.get('targetPage', 1)}")
    logger.info(f"Max Pages: {config.get('maxPages', 5)}")
    logger.info(f"Target URLs: {len(config.get('targetUrls', []))} URLs specified")
    logger.info(f"Number of workers: {args.workers}")
    logger.info(f"Pages per worker: {args.pages_per_worker}")
    
    # Run the scraper with retry mechanism
    retry_scraper(config)


if __name__ == "__main__":
    main()