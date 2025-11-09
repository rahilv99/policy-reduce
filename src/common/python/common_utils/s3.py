import boto3
import pickle
import os
import json

bucket_name = os.getenv("BUCKET_NAME")
print(f"Bucket name is {bucket_name}")

# Centralized area to define where various stuff is in S3 bucket
def s3LocationMapping(type, key=''):
    if (type == "requery"):
        return f"requery/{key}"
    else:
        return ""

def get_s3_url(user_id, episode_number, type):
    object_key = s3LocationMapping(user_id, episode_number, type)
    return f'https://{bucket_name}.s3.us-east-1.amazonaws.com/{object_key}'

def save_serialized(type, key, data):
    object_key = s3LocationMapping(type, key)
    # Serialize the data
    serialized_data = pickle.dumps(data)

    # Upload to S3
    try:
        s3 = boto3.client('s3')
        s3.put_object(Bucket=bucket_name, Key=f'{object_key}.pkl', Body=serialized_data)
        print('Saved serialized data')
    except Exception as e:
        print(f"Error saving to bucket {e}")

def restore_serialized(type, key):
    object_key = s3LocationMapping(type, key)
    # Download serialized data from S3
    s3 = boto3.client('s3')

    try:
        response = s3.get_object(Bucket=bucket_name, Key=f'{object_key}.pkl')
        serialized_data = response['Body'].read()
        print('Retrieved serialized data')
    except Exception as e:
        print(f"Error reading from bucket {e}")
        return {}

    # Deserialize the data
    data = pickle.loads(serialized_data)
    return data

def save_json(type, key, data):
    """
    Save articles to json in S3.
    """
    object_key = s3LocationMapping(type, key)

    # Upload to S3
    try:
        s3 = boto3.client('s3')

        params = {
            'Bucket': os.getenv('BUCKET_NAME'),
            'Key': f'{object_key}.json',
            'Body': json.dumps(data),
            'ContentType': 'application/json'
        }

        s3.put_object(**params)
        print('Saved json')
    except Exception as e:
        print(f"Error saving to bucket {e}")

def delete_json(type, key):
    object_key = s3LocationMapping(type, key)
    # Delete data from S3
    s3 = boto3.client('s3')

    try:
        s3.delete_object(Bucket=bucket_name, Key=f'{object_key}.json')
        print('Deleted json data')
    except Exception as e:
        print(f"Error deleting from bucket {e}")


def restore_dir(object_key):
    # Download data from S3
    s3 = boto3.client('s3')

    try:

        # List all objects in the folder
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=object_key)

        # Check if any contents exist
        contents = []
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                if not key.endswith('/'):  # Skip folders
                    try:
                        # Get the file content
                        file_obj = s3.get_object(Bucket=bucket_name, Key=key)
                        content = file_obj['Body'].read().decode('utf-8')
                        
                        # Parse JSON array
                        requery = json.loads(content)
                        contents.append(requery)
                        
                    except Exception as e:
                        print(f"Error processing file {key}: {str(e)}")
                        continue

        print(f'Retrieved {len(contents)} items')

        return contents
    except Exception as e:
        print(f"Error reading from bucket {e}")
        return {}