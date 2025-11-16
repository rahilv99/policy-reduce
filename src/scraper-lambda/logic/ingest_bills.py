# This file is used to read a URL (which is an XML URL ie https://www.govinfo.gov/bulkdata/BILLS/118/1/hconres/BILLS-118hconres10ih.xml) of a bill and ingest the text and metadata into a database
'''
    This file will read a task off the SQS queue containing a batch of 500 XML URLs
    and ingest the text and metadata into a database
    Here is the SQS message on the queue
    message = {
        'action': 'e_ingest_bills',
        'payload': {
            'urls': urls,
            'congress': congress,
            'session': session,
            'bill_type': bill_type
        }
    }
    Read the urls in the message and write to the historical_bills table, putting the following fields:
    - id, 
    - type (e.g. bill, resolution, joint-resolution, senate-joint — from root tag or resolution-type / bill-stage),
    - congress (e.g. 117th CONGRESS),
    - session (e.g. 1st Session),
    - title (<dc:title> tag),
    - publisher (dc:publisher),
    - dc_date (dc:date),
    - URL,
    - full_xml
    
    
'''

import os
import xml
import requests
import xml.etree.ElementTree as ET
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import logging
from bs4 import BeautifulSoup
from doc_sanitizer import sanitize_document

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database connection
uri = os.environ.get("DB_URI")
client = MongoClient(uri, server_api=ServerApi('1'))
db = client['auxiom_database']
historical_bills_collection = db['historical_bills']

# XML namespace for Dublin Core
DC_NS = {'dc': 'http://purl.org/dc/elements/1.1/'}

def fetch_xml(url, max_retries=3):
    """
    Fetch XML content from a URL with retries.
    
    :param url: URL to fetch
    :param max_retries: Maximum number of retry attempts
    :return: XML content as string, or None if failed
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return response.text
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed to fetch {url}: {e}")
            if attempt == max_retries - 1:
                logger.error(f"Failed to fetch {url} after {max_retries} attempts")
                return None
    return None

def extract_id_from_url(url):
    """
    Extract a unique ID from the URL.
    Example: https://www.govinfo.gov/bulkdata/BILLS/118/1/hconres/BILLS-118hconres10ih.xml
    Returns: BILLS-118hconres10ih or similar identifier
    """
    filename = url.split('/')[-1]
    # Remove .xml extension
    id_from_url = filename.replace('.xml', '')
    return id_from_url

def extract_type_from_xml(root):
    """
    Extract type from root tag or attributes.
    Root tag can be 'bill', 'resolution', etc.
    Attributes can be 'resolution-type', 'bill-stage', etc.
    """
    # Get root tag name
    root_tag = root.tag.lower()
    
    # Check for resolution-type attribute
    resolution_type = root.get('resolution-type', '')
    if resolution_type:
        # Map resolution types to standardized format
        type_mapping = {
            'house-concurrent': 'joint-resolution',
            'house-joint': 'joint-resolution',
            'senate-concurrent': 'senate-joint',
            'senate-joint': 'senate-joint',
            'house-simple': 'resolution',
            'senate-simple': 'resolution'
        }
        return type_mapping.get(resolution_type, resolution_type)
    
    # Check for bill-stage attribute
    bill_stage = root.get('bill-stage', '')
    if bill_stage:
        return 'bill'
    
    # Fall back to root tag name
    if 'resolution' in root_tag:
        return 'resolution'
    elif 'bill' in root_tag:
        return 'bill'
    
    return root_tag

def parse_xml_bill(xml_content, url):
    """
    Parse XML content and extract required fields.
    
    :param xml_content: XML content as string
    :param url: Original URL of the XML file
    :return: Dictionary with bill data
    """
    try:
        root = ET.fromstring(xml_content)
        
        # Extract ID from URL
        doc_id = extract_id_from_url(url)
        
        # Extract type
        doc_type = extract_type_from_xml(root)
        
        # Extract congress
        congress = ""
        congress_elem = root.find('.//congress')
        if congress_elem is not None and congress_elem.text:
            congress = congress_elem.text.strip()
        
        # Extract session
        session = ""
        session_elem = root.find('.//session')
        if session_elem is not None and session_elem.text:
            session = session_elem.text.strip()
        
        # Extract title from dc:title (with namespace)
        title = ""
        title_elem = root.find('.//dc:title', DC_NS)
        if title_elem is not None and title_elem.text:
            title = title_elem.text.strip()
        
        # Extract publisher from dc:publisher (with namespace)
        publisher = ""
        publisher_elem = root.find('.//dc:publisher', DC_NS)
        if publisher_elem is not None and publisher_elem.text:
            publisher = publisher_elem.text.strip()
        
        # Extract date from dc:date (with namespace)
        dc_date = ""
        date_elem = root.find('.//dc:date', DC_NS)
        if date_elem is not None and date_elem.text:
            dc_date = date_elem.text.strip()
        
        soup = BeautifulSoup(xml_content, "xml")
        full_text = soup.get_text(separator="\n", strip=True)

        # Build document data dictionary
        doc_data = {
            'id': doc_id,
            'type': doc_type,
            'congress': congress,
            'session': session,
            'title': title,
            'publisher': publisher,
            'dc_date': dc_date,
            'URL': url,
            'full_text': full_text
        }
        return doc_data
        
    except ET.ParseError as e:
        logger.error(f"XML parsing error for {url}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error parsing bill from {url}: {e}", exc_info=True)
        return None

def process_bill_url(url):
    """
    Process a single bill URL: fetch XML, parse it, and insert/update in database.
    
    :param url: XML URL of the bill
    :return: True if successful, False otherwise
    """
    try:
        logger.info(f"Processing bill: {url}")
        
        # Fetch XML content
        xml_content = fetch_xml(url)
        if not xml_content:
            logger.error(f"Failed to fetch XML from {url}")
            return False
        
        # Parse XML
        doc_data = parse_xml_bill(xml_content, url)
        if not doc_data:
            logger.error(f"Failed to parse bill from {url}")
            return False

        # Sanitize document using doc_sanitizer
        if not sanitize_document(doc_data, url):
            return False
        
        # Check if document already exists
        existing_doc = historical_bills_collection.find_one({"id": doc_data['id']})
        
        if existing_doc:
            # Update existing document
            result = historical_bills_collection.update_one(
                {"id": doc_data['id']},
                {"$set": doc_data}
            )
            if result.modified_count > 0:
                logger.info(f"Updated document {doc_data['id']}")
                return True
            else:
                logger.debug(f"No changes made to document {doc_data['id']}")
                return True  # Still considered success if no changes needed
        else:
            # Insert new document
            result = historical_bills_collection.insert_one(doc_data)
            if result.inserted_id:
                logger.info(f"Inserted new document {doc_data['id']}")
                return True
            else:
                logger.error(f"Failed to insert document {doc_data['id']}")
                return False
        
    except Exception as e:
        logger.error(f"Error processing bill {url}: {e}", exc_info=True)
        return False

def handler(payload):
    """
    Handler function to process a batch of bill URLs from SQS.
    
    :param payload: Dictionary containing:
        - urls: List of XML URLs
        - congress: Congress number (optional, can be extracted from XML)
        - session: Session number (optional, can be extracted from XML)
        - bill_type: Type of bill (optional, can be extracted from XML)
    """
    urls = payload.get('urls', [])
    
    if not urls:
        logger.warning("No URLs provided in payload")
        return
    
    logger.info(f"Processing batch of {len(urls)} bills")
    
    successful = 0
    failed = 0
    
    for i, url in enumerate(urls, 1):
        logger.info(f"Processing bill {i}/{len(urls)}: {url}")
        if process_bill_url(url):
            successful += 1
        else:
            failed += 1
    
    logger.info(f"="*60)
    logger.info(f"SUMMARY: Processed {len(urls)} bills")
    logger.info(f"  - Successful: {successful}")
    logger.info(f"  - Failed: {failed}")
    logger.info(f"="*60)

if __name__ == "__main__":
    process_bill_url("https://www.govinfo.gov/bulkdata/BILLS/115/2/sconres/BILLS-115sconres35ats.xml")