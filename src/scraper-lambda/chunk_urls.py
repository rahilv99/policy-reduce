'''
 This file will go from the root of the 116th congress (https://www.govinfo.gov/bulkdata/BILLS/116) and traverse the sublinks
 for:
    a) the 1st Session and 2nd Session
        b) hconres, hjres, hr, hres, s, sconres, sjres, sres
 Now it will be on a page like https://www.govinfo.gov/bulkdata/BILLS/116/1/hconres which will have a list of
 XML URLs.
 Then it will group the XML URLs (<link>https://www.govinfo.gov/bulkdata/BILLS/116/1/hconres/BILLS-116hconres12ih.xml</link>) 
 on the page using beautiful soup in groups of 500 and add them to a SQS queue using
 send_to_scraper_queue from common_utils.sqs.py to be processed by the ingest_bills.py file.
'''

import requests
from bs4 import BeautifulSoup
import common_utils.sqs as sqs
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://www.govinfo.gov/bulkdata/BILLS"
CHUNK_SIZE = 500
SESSIONS = [1, 2]
BILL_TYPES = ['hconres', 'hjres', 'hr', 'hres', 's', 'sconres', 'sjres', 'sres']
MAX_RETRIES = 3


def fetch_page(url, retries=MAX_RETRIES):
    """
    Fetch a page with retry logic.
    
    :param url: URL to fetch
    :param retries: Number of retry attempts
    :return: Response object or None if failed
    """
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.warning(f"Attempt {attempt + 1}/{retries} failed for {url}: {e}")
            if attempt == retries - 1:
                logger.error(f"Failed to fetch {url} after {retries} attempts")
                return None
    return None


def extract_xml_urls_from_page(url):
    """
    Extract XML URLs from a bill listing page.
    
    :param url: URL of the page containing bill links
    :return: List of XML URLs found on the page
    """
    logger.info(f"Fetching page: {url}")
    response = fetch_page(url)
    
    if not response:
        return []
    
    try:
        soup = BeautifulSoup(response.text, 'html.parser')
        xml_urls = []
        
        # Find all <link> tags that contain XML URLs
        links = soup.find_all('link')
        
        for link in links:
            link_text = link.get_text(strip=True)
            # Check if it's an XML URL
            if link_text and link_text.endswith('.xml'):
                xml_urls.append(link_text)
                logger.debug(f"Found XML URL: {link_text}")
        
        logger.info(f"Extracted {len(xml_urls)} XML URLs from {url}")
        return xml_urls
        
    except Exception as e:
        logger.error(f"Error parsing page {url}: {e}")
        return []


def chunk_list(items, chunk_size):
    """
    Split a list into chunks of specified size.
    
    :param items: List to chunk
    :param chunk_size: Size of each chunk
    :return: Generator yielding chunks
    """
    for i in range(0, len(items), chunk_size):
        yield items[i:i + chunk_size]


def send_url_chunk_to_queue(urls, congress, session, bill_type):
    """
    Send a chunk of URLs to the SQS queue.
    
    :param urls: List of XML URLs to send
    :param congress: Congress number
    :param session: Session number
    :param bill_type: Type of bill
    """
    message = {
        'action': 'e_ingest_bills',
        'payload': {
            'urls': urls,
            'congress': congress,
            'session': session,
            'bill_type': bill_type
        }
    }
    
    try:
        sqs.send_to_scraper_queue(message)
        logger.info(f"Sent chunk of {len(urls)} URLs to SQS queue (Congress {congress}, Session {session}, Type {bill_type})")
    except Exception as e:
        logger.error(f"Failed to send chunk to SQS queue: {e}")
        raise


def process_congress(congress=116):
    """
    Process all bills for a given congress by traversing sessions and bill types.
    
    :param congress: Congress number (default: 116)
    """
    logger.info(f"Starting to process Congress {congress}")
    
    total_urls = 0
    total_chunks = 0
    
    for session in SESSIONS:
        for bill_type in BILL_TYPES:
            # Construct the URL for this session and bill type
            page_url = f"{BASE_URL}/{congress}/{session}/{bill_type}"
            
            logger.info(f"Processing: Congress {congress}, Session {session}, Type {bill_type}")
            
            # Extract all XML URLs from this page
            xml_urls = extract_xml_urls_from_page(page_url)
            
            if not xml_urls:
                logger.warning(f"No XML URLs found for {page_url}")
                continue
            
            # Chunk the URLs into groups of 500
            chunks = list(chunk_list(xml_urls, CHUNK_SIZE))
            logger.info(f"Created {len(chunks)} chunks from {len(xml_urls)} URLs")
            
            # Send each chunk to the SQS queue
            for chunk in chunks:
                send_url_chunk_to_queue(chunk, congress, session, bill_type)
                total_chunks += 1
                total_urls += len(chunk)
    
    logger.info(f"="*60)
    logger.info(f"SUMMARY: Processed Congress {congress}")
    logger.info(f"  - Total URLs extracted: {total_urls}")
    logger.info(f"  - Total chunks sent to queue: {total_chunks}")
    logger.info(f"="*60)
    
    return total_urls, total_chunks


def handler(payload):
    """
    Lambda handler function for chunking URLs.
    
    :param payload: Dictionary containing 'congress' (optional, defaults to 116)
    :return: Dictionary with processing results
    """
    congress = payload.get('congress', 116)
    logger.info(f"Handler invoked with congress={congress}")
    
    try:
        total_urls, total_chunks = process_congress(congress)
        return {
            'status': 'success',
            'congress': congress,
            'total_urls': total_urls,
            'total_chunks': total_chunks
        }
    except Exception as e:
        logger.error(f"Error in handler: {e}", exc_info=True)
        return {
            'status': 'error',
            'congress': congress,
            'error': str(e)
        }


if __name__ == "__main__":
    # Example usage
    result = process_congress(congress=116)
    print(f"Processing complete: {result}")
