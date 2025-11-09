def test_connection(client):
    """
    Test the MongoDB connection.
    
    Args:
        client: MongoDB client instance
    
    Returns:
        bool: True if connection successful, False otherwise
    """
    try:
        # Send a ping to confirm a successful connection
        client.admin.command('ping')
        print("Successfully connected to MongoDB!")
        return True
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return False

def get_all_bills(bill_collection):
    try:
        return list(bill_collection.find())
    except Exception as e:
        print(f"Error getting all bills: {e}")
        return []


def get_bill(bills_collection, bill_id):
    """
    Check if a bill already exists in the database.
    
    Args:
        bills_collection: MongoDB collection instance
        bill_id (str): The unique bill ID (e.g., "hr123-118")
    
    Returns:
        dict or None: The existing bill document if found, None otherwise
    """
    try:
        existing_bill = bills_collection.find_one({"bill_id": bill_id})
        return existing_bill
    except Exception as e:
        print(f"Error checking if bill exists: {e}")
        return None


def insert_bill(bills_collection, bill_data):
    """
    Insert a new bill into the database.
    
    Args:
        bills_collection: MongoDB collection instance
        bill_data (dict): The bill data to insert
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        result = bills_collection.insert_one(bill_data)
        print(f"Inserted new bill with ID: {result.inserted_id}")
        return True
    except Exception as e:
        print(f"Error inserting new bill: {e}")
        return False

def delete_bill(bills_collection,  id):
    try:
        result = bills_collection.delete_one({"_id": id})
        print(f"Deleted bill with ID: {id}")
        return True
    except Exception as e:
        print(f"Error deleting bill: {e}")
        return False

def update_bill(bills_collection, bill_data):
    """
    Update an existing bill in the database.
    
    Args:
        bills_collection: MongoDB collection instance
        bill_id (str): The unique bill ID
        bill_data (dict): The updated bill data
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        result = bills_collection.update_one(
            {"bill_id": bill_data['bill_id']},
            {"$set": bill_data}
        )
        
        if result.modified_count > 0:
            print(f"Updated existing bill: {bill_data['bill_id']}")
            return True
        else:
            print(f"No changes made to bill: {bill_data['bill_id']}")
            return False
    except Exception as e:
        print(f"Error updating existing bill: {e}")
        return False


def insert_event(events_collection, event_data):
    """
    Insert a new event into the database.
    
    Args:
        events_collection: MongoDB collection instance
        event_data (json): The event data to insert
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        result = events_collection.insert_one(event_data)
        print(f"Inserted new event with ID: {result.inserted_id}")
        return True
    except Exception as e:
        print(f"Error inserting new event: {e}")
        return False

def clear_events(events_collection, bill_id):
    try:
        result = events_collection.delete_many({"bill_id": bill_id})
        print(f"Deleted {result.deleted_count} events for bill {bill_id}")
        return True
    except Exception as e:
        print(f"Error deleting events for bill {bill_id}: {e}")
        return False

def update_events(events_collection, bill_id, data):
    try:
        result = events_collection.update_many({"bill_id": bill_id}, {"$set": data})
        print(f"Updated {result.modified_count} events for bill {bill_id}")
        return True
    except Exception as e:
        print(f"Error updating events for bill {bill_id}: {e}")
        return False