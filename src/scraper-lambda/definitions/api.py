
from definitions.congress import Bill
import requests
from bs4 import BeautifulSoup
import PyPDF2
import io
import re
import datetime
import time
import random
import common_utils.sqs as sqs

# NOTES
# - add cosponsors to people section

# Constants
DEFAULT_ARTICLE_AGE = 7
MAX_RETRIES = 5
BASE_DELAY = 0.33
MAX_DELAY = 15


class CongressGovAPI:
    BASE_URL = "https://api.congress.gov/v3"

    def __init__(self, api_key):
        self.api_key = api_key

    def _make_request(self, endpoint, params=None):
        if params is None:
                params = {}
        url = f"{self.BASE_URL}/{endpoint}"
        
        params["api_key"] = self.api_key
        # Use exponential backoff for retries
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()  # Raise an exception for HTTP errors
                return response.json()
            except Exception as e:
                print(f"Error making request: {e}")
                print(f'Response: {response}')
                if attempt == MAX_RETRIES - 1:
                    raise e
                # Exponential backoff with jitter
                delay = min(BASE_DELAY * 2 ** attempt + random.uniform(0, 1), MAX_DELAY)
                time.sleep(delay)

    def get_bills(self, congress=None, bill_type=None, date_since_days=1, offset=0):
        endpoint = "bill"
        params = {}
        if congress:
            endpoint += f"/{congress}"
        if bill_type:
            endpoint += f"/{bill_type}"

        date_n_days_ago = datetime.date.today() - datetime.timedelta(days=date_since_days)
        params["fromDateTime"] = date_n_days_ago.strftime("%Y-%m-%dT00:00:00Z")

        params["offset"] = offset
        params["limit"] = 250  # Maximum limit

        print('making request with params', params)
        data = self._make_request(endpoint, params=params)

        bills_data = []
        bills = data.get("bills", [])
        bills_data.extend(bills)

        next_page = data.get("pagination", {}).get("next", "")
        if next_page:
            print(f'Invoking lambda for next page: {offset+250}/{data["pagination"]["count"]}')
            next_page = {
                'action': 'e_ingest',
                'payload': {
                    "offset": offset+250,
                    "date_since_days": date_since_days
                }
            }
            sqs.send_to_scraper_queue(next_page)

        bill_objects = []
        print(f'Found {len(bills_data)} bills. Requesting more info...')
        for bill_summary in bills_data:
            congress_num = bill_summary.get("congress")
            bill_type = bill_summary.get("type")
            bill_number = bill_summary.get("number")

            if congress_num and bill_type and bill_number:
                bill_details = self.get_bill_details(congress_num, bill_type, bill_number)
                bill_objects.append(Bill(self, bill_details['bill']))
            else:
                # Fallback if summary doesn't have full details, try parsing billUri
                bill_uri = bill_summary.get("billUri")
                if bill_uri:
                    parts = bill_uri.split("/")
                    if len(parts) >= 6 and parts[-4] == "bill":
                        congress_num = int(parts[-3])
                        bill_type = parts[-2]
                        bill_number = int(parts[-1])
                        bill_details = self.get_bill_details(congress_num, bill_type, bill_number)
                        bill_objects.append(Bill(self, bill_details['bill']))
        return bill_objects

    def get_bill(self, congress, bill_type, bill_number):
        """
        Get a single bill and return it as a Bill object.
        Mimics the behavior of get_bills but for a specific bill.
        
        Args:
            congress: Congress number (e.g., 119)
            bill_type: Bill type (e.g., 's', 'hr', 'hjres', 'sjres')
            bill_number: Bill number (e.g., 1744)
            
        Returns:
            Bill object or None if the bill cannot be fetched
        """
        try:
            bill_details = self.get_bill_details(congress, bill_type, bill_number)
            if bill_details and 'bill' in bill_details:
                return Bill(self, bill_details['bill'])
            else:
                print(f"No bill data returned for {bill_type}{bill_number}-{congress}")
                return None
        except Exception as e:
            print(f"Error fetching bill {bill_type}{bill_number}-{congress}: {e}")
            return None
    
    def get_bill_details(self, congress, bill_type, bill_number):
        endpoint = f"bill/{congress}/{bill_type}/{bill_number}"
        return self._make_request(endpoint)

    def get_bill_actions(self, congress, bill_type, bill_number):
        endpoint = f"bill/{congress}/{bill_type}/{bill_number}/actions"
        return self._make_request(endpoint).get("actions", [])

    def get_bill_amendments(self, congress, bill_type, bill_number):
        endpoint = f"bill/{congress}/{bill_type}/{bill_number}/amendments"
        return self._make_request(endpoint).get("amendments", [])

    def get_bill_committees(self, congress, bill_type, bill_number):
        endpoint = f"bill/{congress}/{bill_type}/{bill_number}/committees"
        return self._make_request(endpoint).get("committees", [])

    def get_bill_related_bills(self, congress, bill_type, bill_number):
        endpoint = f"bill/{congress}/{bill_type}/{bill_number}/relatedbills"
        return self._make_request(endpoint).get("relatedBills", [])

    def get_bill_subjects(self, congress, bill_type, bill_number):
        endpoint = f"bill/{congress}/{bill_type}/{bill_number}/subjects"
        return self._make_request(endpoint).get("subjects", [])

    def get_bill_summaries(self, congress, bill_type, bill_number):
        endpoint = f"bill/{congress}/{bill_type}/{bill_number}/summaries"
        return self._make_request(endpoint).get("summaries", [])

    def get_bill_text(self, congress, bill_type, bill_number):
        endpoint = f"bill/{congress}/{bill_type}/{bill_number}/text"
        return self._make_request(endpoint).get("textVersions", [])

    # Scraping utilities
    def get_document_text(self, url):
        try:
            response = self.fetch_with_retry(requests.get, url)
            
            if response.status_code == 200:
                # For PDF content
                if 'pdf' in url:
                    text = self._extract_text_from_pdf(response.content)
                else:
                    text = self._extract_text_from_html(response.text)
                
                # Clean the extracted text
                text = self._clean_text(text)
                print("Extracted {} characters".format(len(text)))
                return text
            else:
                raise Exception(f"Failed to retrieve document: {response.status_code}")
                
        except Exception as e:
            print(f"Error retrieving document: {e}")
            return f"Error retrieving document: {e}"


    def _extract_text_from_html(self, html_content):
        try:
            # Parse HTML with BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Remove script and style elements
            for script_or_style in soup(["script", "style"]):
                script_or_style.extract()
            
            # Get text content
            text = soup.get_text()

            # Get links
            links = soup.find_all('a')

            for link in links:
                link = link.get('href')
                if 'pdf' in link:
                    try:
                        response = requests.get(link)
                        if response.status_code == 200:
                            text += f"\n{self._extract_text_from_pdf(response.content)}"
                        else:
                            print(f"Failed to retrieve linked document: {response.status_code}")
                    except Exception as e:
                        print(f"Error retrieving linked document: {e}")

            
            # Clean up text: break into lines and remove leading/trailing space
            lines = (line.strip() for line in text.splitlines())
            
            # Break multi-headlines into a line each
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            
            # Remove blank lines
            text = '\n'.join(chunk for chunk in chunks if chunk)
            
            return text
        except Exception as e:
            print(f"Error extracting text from HTML: {e}")
            return html_content  # Return original content as fallback
    
    def _extract_text_from_pdf(self, pdf_content):
        try:
            pdf_file = io.BytesIO(pdf_content)
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            
            # Check if PDF is encrypted
            if pdf_reader.is_encrypted:
                print("PDF is encrypted, cannot extract text")
                return "PDF is encrypted, cannot extract text"
            
            # Extract text from all pages
            text = ""
            for page_num in range(len(pdf_reader.pages)):
                page = pdf_reader.pages[page_num]
                text += page.extract_text() + "\n"
            
            return text
            
        except Exception as e:
            print(f"Error extracting text from PDF: {e}")
            return "Error extracting text from PDF"

    def _clean_text(self, text):
        """
        Clean extracted text by removing extra spaces, normalizing whitespace,
        handling special characters, and improving readability.
        """
        if not text:
            return ""
            
        # Replace multiple spaces with a single space
        text = re.sub(r'\s+', ' ', text)
        
        # Replace multiple newlines with a single newline
        text = re.sub(r'\n\s*\n', '\n\n', text)
        
        # Fix common PDF extraction issues
        text = re.sub(r'(\w)-\s+(\w)', r'\1\2', text)  # Fix hyphenation
        text = re.sub(r'(\d+)\s*\.\s*(\d+)', r'\1.\2', text)  # Fix decimal numbers
        
        # Replace special characters that might be incorrectly encoded
        text = text.replace('â€™', "'")
        text = text.replace('â€œ', '"')
        text = text.replace('â€', '"')
        text = text.replace('â€"', '-')
        text = text.replace('â€"', '--')
        
        # Remove non-printable characters
        text = ''.join(char for char in text if char.isprintable() or char in '\n\t')
        
        # Trim leading/trailing whitespace
        text = text.strip()
        
        return text

    def fetch_with_retry(self, func, *args, **kwargs):
        for attempt in range(MAX_RETRIES):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == MAX_RETRIES - 1:
                    raise e
                time.sleep(min(BASE_DELAY * 2 ** attempt + random.uniform(0, 1), MAX_DELAY))
