import re
import logging

logger = logging.getLogger(__name__)

# Heuristic thresholds / phrase lists (tune as needed)
MIN_BODY_LEN = 1000           # likely not a policy bill

ALLOWED_TYPES = {
    'hr', 'h.r.', 's', 's.', 'hjres', 'h.j.res.', 'sjres', 's.j.res.'
}

def sanitize_document(doc_data, url):
    full_text = doc_data.get('full_text', '') or ''
    doc_type = (doc_data.get('type') or "").lower()

    if len(full_text) < MIN_BODY_LEN:
        logger.error(f"Full text is too short ({len(full_text)} chars) for {url}")
        return False
    
    if doc_type and not any(x in doc_type for x in ALLOWED_TYPES):
        logger.info(f"Excluding because doc_type '{doc_type}' not in allowed types")
        return False

    logger.info(f"Document passed single-change filters; inserting/updating DB: {url}")
    return True
