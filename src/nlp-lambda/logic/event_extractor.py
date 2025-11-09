import boto3
import os
import json
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import common_utils.database as database
import anthropic
from anthropic.types.message_create_params import MessageCreateParamsNonStreaming
from anthropic.types.messages.batch_create_params import Request

ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY')
anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

uri = os.environ.get("DB_URI")

client = MongoClient(uri, server_api=ServerApi('1'))
db = client['auxiom_database']
bills_collection = db['bills']
events_collection = db['events']

# AWS clients
events_client = boto3.client('events')


def create_batch_requests(bills):

    requests = []
    
    for bill in bills:
        if len(bill.get('text')) < 10000:
            model = "claude-3-5-haiku-latest"
            max_tokens=8192
        else:
            model = "claude-sonnet-4-20250514"
            max_tokens=12000
    
        request = Request(
            custom_id=bill['bill_id'],
            params=MessageCreateParamsNonStreaming(
                model=model,
                max_tokens=max_tokens,
                temperature=0.7,
                system="You are an expert legislative analyst. Your task is to extract policy events from the text of a U.S. legislative bill.\n\nDefinition of an event:\n- A substantive policy changes that affect how government programs, funding, or regulations operate.\n- Multiple sentences of bill text that constitutes a change in policy for one or more topics. Include all related sentences in the bill.\n- Include enough context to determine what the change is and what it applies to.\n- All details related to the event should be encapsulated in the text excerpt.\n\nExtraction Procedure\n- The goal is to maximize the number of events extracted and minimize noise (negligible events).\n- Collect all events that have unique results in the bill. Merge events that are related to the same result.\n- Prune events that are simply minor, technical, or procedural details of the bill (such as budget scoring rules, effective dates, definitions, or clerical amendments).\n- There is no minimum or maximum number of events. Be sure all events meet the requirements outlined. Return an empty array if there is no event that meets the guidelines above. \n- Only output valid JSON as a list of objects (no commentary, no explanation).\n\nFor each event, return a JSON object in the following format:\n\nJSON\n{\n\"text\": \"<exact excerpt of bill text describing the policy change>\",\n\"topics\": [\"<broad policy areas impacted>\"],\n\"tags\": [\"<specific descriptors within the topics>\"],\n\"summary\": \"<analysis of text contextualizing the main idea of the event in the goal of the bill>\",\n\"title\": \"<concise descriptor of event>\"\n}\n\nGuidelines:\n- Text is excerpt of bill text that constitutes a change in policy and all related details. Include any other excerpts of text from the bill that add valuable context. \n- Topics are broad policy areas where the U.S. government takes a stance (e.g., \"Healthcare\", \"Defense\", \"Education\", \"Energy\", \"Immigration\"). Topics are one word.\n- Tags are narrower descriptors that specify the scope within a topic (e.g., for Healthcare → \"Medicare\", \"drug pricing\"; for Energy → \"renewable energy\", \"oil subsidies\"). Tags should be just one level more specific than the topic, but still broad.\n- Summary is a summary of the bill's overall goal, specifying what the event achieves. Define any unknown entities. Include all information in the bill outside of the event that contextualizes the event.\n- Title is a short, concise, and specific descriptor with metrics included when possible.\n\nExample output:\n\n[\n    {\n        \"text\": \"Notwithstanding any other provision of law, the Secretary of Health and Human Services shall, beginning on January 1, 2026, negotiate directly with manufacturers of insulin products with respect to the prices that may be charged to prescription drug plans under part D of title XVIII of the Social Security Act for such products furnished to individuals entitled to benefits under such title.\",\n        \"topics\": [\"Healthcare\"],\n        \"tags\": [\"Medicare\", \"drug pricing\", \"insulin\"],\n        \"summary\": \"The Secretary of Health and Human Services will negotiate the price of insulin for Medicare beneficiaries.\",\n        \"title\": \"Insulin Prices to be Negotiated\"\n    },\n    {\n        \"text\": \"Of the amounts authorized to be appropriated for the Department of Defense for fiscal year 2026, the Secretary of Defense shall allocate not less than $500,000,000 for the purposes of planning, developing, and sustaining cybersecurity infrastructure, including but not limited to network modernization, threat detection systems, and defensive cyber operations.\",\n        \"topics\": [\"Defense\", \"Technology\"],\n        \"tags\": [\"cybersecurity\", \"infrastructure funding\"],\n        \"summary\": \"The Department of Defense allocates $500 million for cybersecurity infrastructure.\",\n        \"title\": \"$500M allocated for cybersecurity\"\n    }\n]",
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": f"Bill text to analyze:\n{bill.get('text')}\nStructure your response as a list of JSONs with the following keys: text (string), topics (list), tags (list), summary(string), title (string). Only include this list, no comments or introduction.\n\n"
                            }
                        ]
                    },
                    {
                        "role": "assistant",
                        "content": [
                            {
                                "type": "text",
                                "text": "["
                            }
                        ]
                    }
                ]
            )
        )
        requests.append(request)
    
    return requests

def create_eventbridge_rule(batch_id, bill_ids):
    """
    Create EventBridge rules with backoff strategy to check batch status.
    Creates multiple rules with different schedules and automatic cleanup.
    """
    
    # Create the rule
    events_client.put_rule(
        Name=f'batch-check-{batch_id}',
        ScheduleExpression=f"rate(2 minutes)",
        Description=f"Check batch {batch_id} status every 2 minutes",
        State='ENABLED'
    )
    
    # Create the target (SQS queue message)
    message_payload = {
        "action": "e_event_retriever",
        "payload": {
            "batch_id": batch_id,
            "bill_ids": bill_ids
        }
    }
    
    # Add target to the rule - send message to nlp queue via SQS utils
    events_client.put_targets(
        Rule=f'batch-check-{batch_id}',
        Targets=[
            {
                'Id': f'nlp-queue-target-{batch_id}',
                'Arn': os.environ.get('NLP_QUEUE_ARN'),
                'Input': json.dumps(message_payload)
            }
        ]
    )


def submit_batch_for_processing(bills):
    """Submit batch requests to Anthropic and return batch tracking information"""
    requests = create_batch_requests(bills)
    
    print(f"Creating batch with {len(requests)} requests")
    
    message_batch = anthropic_client.messages.batches.create(requests=requests)
    
    print(f"Batch created with ID: {message_batch.id}")
    print(f"Processing status: {message_batch.processing_status}")
    print(f"Request counts: {message_batch.request_counts}")
    
    try:
        ids = [bill['bill_id'] for bill in bills]
        create_eventbridge_rule(message_batch.id, ids)
    except Exception as e:
        print(f"Error creating eventbridge rule: {e}")
    
    return {
        'batch_id': message_batch.id,
        'processing_status': message_batch.processing_status,
        'request_counts': message_batch.request_counts,
        'created_at': message_batch.created_at,
        'expires_at': message_batch.expires_at,
        'results_url': message_batch.results_url,
        'bill_ids': [bill['bill_id'] for bill in bills]
    }


def main(bill_ids):
    """Process multiple bills using batch API"""
    bills = []
    
    # Get all bills from database
    for bill_id in bill_ids:
        bill = database.get_bill(bills_collection, bill_id)
        if bill:
            bills.append(bill)
        else:
            print(f"Warning: Bill {bill_id} not found in database")
    
    if not bills:
        raise Exception("No valid bills found for processing")
    
    print(f"Processing {len(bills)} bills for batch event extraction")
    
    # Submit batch for processing
    batch_info = submit_batch_for_processing(bills)
    
    return batch_info


def handler(payload):
    """Handle requests for batch event extraction"""
    
    # Handle batch extraction request
    bill_ids = payload.get('ids')
    type = payload.get('type', '')

    if type == 'updated_bill':
        # Clear all events for these bills
        for bill_id in bill_ids:
            database.clear_events(events_collection, bill_id)
            bill_data = {
                'bill_id': bill_id,
                'events': []
            }
            database.update_bill(bills_collection, bill_data)
    
    print(f"Processing batch event extraction for {len(bill_ids)} bills: {bill_ids}")
    
    batch_info = main(bill_ids)
    return batch_info


if __name__ == "__main__":
    # Example usage for batch processing
    payload = {
        "bill_ids": ["S2806-119"],
    }
    
    print(f"Processing batch event extraction for bills: {payload['bill_ids']}")
    
    result = handler(payload)
    print("Batch submission result:", result)
