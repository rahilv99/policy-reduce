# This file is used to read a URL (which is an XML URL ie https://www.govinfo.gov/bulkdata/BILLS/118/1/hconres/BILLS-118hconres10ih.xml) of a bill and ingest the text and metadata into a database
'''
    This file will read a task off the SQS queue containing a batch of 500 XML URLs
    and ingest the text and metadata into a database
'''