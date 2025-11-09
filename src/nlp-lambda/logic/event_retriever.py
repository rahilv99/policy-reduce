from google import genai
import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import common_utils.database as database
import json
import numpy as np
import uuid
import anthropic
from datetime import datetime
import boto3
import common_utils.sqs as sqs


GOOGLE_API_KEY = os.environ.get('GOOGLE_API_KEY')
ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY')
genai_client = genai.Client(api_key=GOOGLE_API_KEY)
anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

uri = os.environ.get("DB_URI")

client = MongoClient(uri, server_api=ServerApi('1'))
db = client['auxiom_database']
bills_collection = db['bills']
events_collection = db['events']

events_client = boto3.client('events')

def process_event(bill, event):
    def _get_embedding(content):
        result = genai_client.models.embed_content(
        model="gemini-embedding-001",
        contents=content,
        config=genai.types.EmbedContentConfig(output_dimensionality=768))

        [embedding_obj] = result.embeddings
        embedding_values_np = np.array(embedding_obj.values)
        normed_embedding = embedding_values_np / np.linalg.norm(embedding_values_np)

        return normed_embedding.tolist()

    content = ' '.join(event['topics']) + ' ' + ' '.join(event['tags']) + ' ' + event['summary']

    event['embedding'] = _get_embedding(content)

    actions = bill['actions']

    if actions and len(actions) > 0:
        latest = actions[-1]
        latest_action = latest.get('text', None)
    else:
        latest_action = None

    # Add bill data to event
    event['bill'] = {
        'id': bill['bill_id'],
        'title': bill['title'],
        'date': bill['latest_action_date'],
        'latest_action': latest_action
    }

    # add id and status to event
    event['id'] = bill['bill_id'] + '-' + str(uuid.uuid4())
    event['status'] = bill.get('status', 'pending')

    return event

def process_batch_results(batch_id):
    """Process results from a completed batch - to be called separately when batch is done"""
    try:
        # Retrieve batch results
        batch = anthropic_client.messages.batches.retrieve(batch_id)
        
        if batch.processing_status == 'expired' or batch.processing_status == 'cancelled':
            return {
                'status': batch.processing_status,
            }
        if batch.processing_status != 'ended':
            return {
                'status': 'not_ready',
                'processing_status': batch.processing_status,
                'errored': batch.request_counts.errored,
                'succeeded': batch.request_counts.succeeded,
                'processing': batch.request_counts.processing,
                'message': f'Batch {batch_id} has {batch.request_counts.succeeded} succeeded requests, {batch.request_counts.errored} errored requests, {batch.request_counts.processing} processing requests.'
            }

        # Log batch processing time
        started_at = batch.created_at
        ended_at = batch.ended_at
        if started_at and ended_at:
            duration = (ended_at - started_at).total_seconds()
            print(f"Batch {batch_id} processing duration: {duration} seconds")
        
        processed_bills = []
        
        for result in anthropic_client.messages.batches.results(batch_id):
            bill_id = result.custom_id
            
            if result.result.type == 'succeeded':
                try:
                    # Parse the events from the response
                    events_json = result.result.message.content[0].text
                    events_json = '[' + events_json # Add opening bracket from prefill

                    try:
                        events = json.loads(events_json)
                    except json.JSONDecodeError as e:
                        print(f"Error parsing json of events for bill {bill_id}: {e}")
                        processed_bills.append({
                                    'bill_id': bill_id,
                                    'status': 'decode_error',
                                    'error': str(e)
                                })
                        continue
                    
                    # Get bill from database
                    bill = database.get_bill(bills_collection, bill_id)
                    
                    if not bill:
                        print(f"Bill {bill_id} not found in database")
                        processed_bills.append({
                            'bill_id': bill_id,
                            'status': 'bill_not_found'
                        })
                        continue
                    
                    event_ids = []
                    event_errors = []
                    
                    for i, event in enumerate(events):
                        try:
                            event = process_event(bill, event)
                            success = database.insert_event(events_collection, event)
                        
                            if success:
                                print(f"Inserted event id {event['id']} for bill {bill_id}")
                                event_ids.append(event['id'])
                            else:
                                print(f"Failed to insert event id {event['id']} for bill {bill_id}")
                                event_errors.append(f"Event {i}: insert failed")
                        except Exception as e:
                            print(f"Error processing event {i} for bill {bill_id}: {e}")
                            event_errors.append(f"Event {i}: {str(e)}")

                    # Update bill with successfully processed events
                    bill['events'] = event_ids
                    success = database.update_bill(bills_collection, bill)
                    
                    if success:
                        print(f"Updated bill {bill_id} with {len(event_ids)} events")
                        processed_bills.append({
                            'bill_id': bill_id,
                            'status': 'success',
                            'events_count': len(event_ids),
                            'event_errors': event_errors if event_errors else None
                        })
                    else:
                        print(f"Failed to update bill {bill_id} with events")
                        processed_bills.append({
                            'bill_id': bill_id,
                            'status': 'database_update_failed'
                        })
                        
                except Exception as e:
                    print(f"Error processing events for bill {bill_id}: {str(e)}")
                    processed_bills.append({
                        'bill_id': bill_id,
                        'status': 'processing_error',
                        'error': str(e)
                    })
            else:
                # Handle different error/failure result types
                error_msg = "Unknown API error"
                if hasattr(result.result, 'error') and result.result.error:
                    error_msg = str(result.result.error)
                    print(f"Batch request failed for bill {bill_id}: {error_msg}")
                else:
                    print(f"Batch request failed for bill {bill_id}: {error_msg}")
                
                processed_bills.append({
                    'bill_id': bill_id,
                    'status': 'api_error',
                    'error': error_msg
                })
        
        return {
            'status': 'completed',
            'batch_id': batch_id,
            'processed_bills': processed_bills,
            'total_processed': len(processed_bills)
        }
        
    except Exception as e:
        print(f"Error processing batch results for {batch_id}: {str(e)}")
        return {
            'status': 'error',
            'batch_id': batch_id,
            'error': str(e)
        }

def cleanup_eventbridge_rule(batch_id):
    """
    Clean up EventBridge rules for a completed or timed-out batch.
    """
    
    # Remove targets first
    events_client.remove_targets(
        Rule=f'policy-reduce-batch-check-{batch_id}',
        Ids=[f'nlp-queue-target-{batch_id}']
    )
    
    # Delete the rule
    events_client.delete_rule(Name=f'policy-reduce-batch-check-{batch_id}')

    print(f"Cleaned up EventBridge: policy-reduce-batch-check-{batch_id}")

def main(batch_id, bill_ids):
    result = process_batch_results(batch_id)

    if result.get('status') == 'completed':
        print(f"Batch {batch_id} completed successfully, cleaning up EventBridge rule")
        cleanup_eventbridge_rule(batch_id)

        # Collect bills that failed and need retry
        retry_bills = []
        for bill in result.get('processed_bills', []):
            if bill.get('status') not in ['success', 'database_update_failed']:
                print(f"Error processing bill {bill.get('bill_id')}: {bill.get('error', 'Unknown error')}. Retrying...")
                retry_bills.append(bill.get('bill_id'))

        if retry_bills:
            print(f"Retrying {len(retry_bills)} failed bills")
            sqs.send_to_nlp_queue({
                "action": "e_event_extractor",
                "payload": {
                    "ids": retry_bills,
                    "type": "new_bill"
                }
            })
    
    elif result.get('status') == 'not_ready':
        print(result.get('message'))
    
    elif result.get('status') in ['errored', 'expired']:
        print(f"Batch {batch_id} {result.get('status')}: {result.get('error', 'Unknown error')}")
        cleanup_eventbridge_rule(batch_id)
        
        # Retry entire batch
        print(f"Retrying all {len(bill_ids)} bills from failed batch")
        sqs.send_to_nlp_queue({
            "action": "e_event_extractor",
            "payload": {
                "ids": bill_ids,
                "type": "new_bill"
            }
        })
    
    elif result.get('status') == 'cancelled':
        print(f"Batch {batch_id} was cancelled. Cleaning up EventBridge rule.")
        cleanup_eventbridge_rule(batch_id)

def handler(payload):
    """Handle event retriever requests from EventBridge or direct calls"""
    batch_id = payload.get('batch_id')
    bill_ids = payload.get('bill_ids')
    
    print(f"Processing batch status check for {batch_id}")
    
    # Process the batch
    main(batch_id, bill_ids)

if __name__ == "__main__":
    # Example usage for batch processing
    payload = {
        "batch_id": "msgbatch_019rntWnPMD5M7dz53qJNT4u",
        "bill_ids": ["S2806-119"]
    }
    
    print(f"Processing batch event extraction for bills: {payload['bill_ids']}")
    
    handler(payload)
