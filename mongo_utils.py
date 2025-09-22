"""
===================================================
Generalized MongoDB Fetch and Insert Utility Script
===================================================

Purpose:
--------
This script provides reusable functions to:
1. Connect to any MongoDB database.
2. Fetch (pull) documents with optional queries, projections, sorting, and limits.
3. Insert new documents or update existing documents in a collection.
4. Delete documents if needed.

Usage:
------
1. Set your MongoDB credentials and host in a `.env` file:

   MONGO_USERNAME=<your_username>
   MONGO_PASSWORD=<your_password>
   MONGO_HOST=<your_host>            # Can be mongodb+srv:// or standard URI
   MONGO_DATABASE=<your_database>
   MONGO_COLLECTION=<your_collection>

2. Run the script or import functions into your project.

    python mongo.py
"""

import os
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from bson import ObjectId
from typing import List, Dict, Optional, Union

# Load environment variables from .env file
load_dotenv()

MONGO_USERNAME = os.getenv("MONGO_USERNAME")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")
MONGO_HOST = os.getenv("MONGO_HOST")  # MongoDB URI or hostname
MONGO_DATABASE = os.getenv("MONGO_DATABASE")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")


def get_mongo_client(host: str, username: str, password: str) -> MongoClient:
    """
    Connect to MongoDB and return a client object.
    
    Parameters:
    -----------
    host : str
        MongoDB host (can be standard or mongodb+srv URI)
    username : str
        MongoDB username
    password : str
        MongoDB password

    Returns:
    --------
    MongoClient
        Connected MongoDB client
    """
    # Clean host in case it contains URI prefix
    host_clean = host
    if host_clean.startswith("mongodb+srv://"):
        host_clean = host_clean[len("mongodb+srv://") :]
    host_clean = host_clean.split("/")[0]
    host_clean = host_clean.split("?")[0]

    mongo_uri = f"mongodb+srv://{username}:{password}@{host_clean}" if "srv" in host else f"mongodb://{username}:{password}@{host_clean}"
    return MongoClient(mongo_uri)


def fetch_documents(
    client: MongoClient,
    db_name: str,
    collection_name: str,
    query: Optional[Dict] = None,
    projection: Optional[Dict] = None,
    sort: Optional[List[tuple]] = None,
    limit: Optional[int] = None,
) -> List[Dict]:
    """
    Fetch documents from a MongoDB collection.
    
    Parameters:
    -----------
    client : MongoClient
        MongoDB client object
    db_name : str
        Name of the database
    collection_name : str
        Name of the collection
    query : dict, optional
        MongoDB query filter, default fetches all
    projection : dict, optional
        Fields to include or exclude
    sort : list of tuples, optional
        e.g., [("updatedAt", -1)] to sort descending
    limit : int, optional
        Max number of documents to fetch

    Returns:
    --------
    List[Dict]
        List of documents
    """
    db = client[db_name]
    col = db[collection_name]

    if query is None:
        query = {}

    cursor = col.find(query, projection)
    if sort:
        cursor = cursor.sort(sort)
    if limit:
        cursor = cursor.limit(limit)

    return list(cursor)


def insert_documents(
    client: MongoClient,
    db_name: str,
    collection_name: str,
    documents: List[Dict],
) -> None:
    """
    Insert one or multiple documents into a collection.
    
    Parameters:
    -----------
    client : MongoClient
        MongoDB client object
    db_name : str
        Database name
    collection_name : str
        Collection name
    documents : list of dict
        Documents to insert
    """
    if not documents:
        print("No documents to insert.")
        return

    db = client[db_name]
    col = db[collection_name]

    if len(documents) == 1:
        col.insert_one(documents[0])
        print(f"Inserted 1 document into {collection_name}")
    else:
        col.insert_many(documents)
        print(f"Inserted {len(documents)} documents into {collection_name}")


def update_documents(
    client: MongoClient,
    db_name: str,
    collection_name: str,
    updates: List[Dict[str, Union[Dict, str]]],
    upsert: bool = False,
) -> None:
    """
    Update multiple documents using bulk operations.
    
    Parameters:
    -----------
    updates : list of dict
        Each dict should have:
            {
                "filter": {query filter},
                "update": {update query, e.g., {"$set": {"field": value}}}
            }
    upsert : bool
        Insert document if it does not exist
    """
    if not updates:
        print("No updates to perform.")
        return

    db = client[db_name]
    col = db[collection_name]

    operations = [UpdateOne(u["filter"], u["update"], upsert=upsert) for u in updates]
    result = col.bulk_write(operations)
    print(f"Matched {result.matched_count}, Modified {result.modified_count}, Upserted {result.upserted_count}")


def delete_documents(
    client: MongoClient,
    db_name: str,
    collection_name: str,
    query: Dict,
) -> None:
    """
    Delete documents matching the query.
    """
    db = client[db_name]
    col = db[collection_name]
    result = col.delete_many(query)
    print(f"Deleted {result.deleted_count} documents from {collection_name}")


# =========================
# Example usage
# =========================
if __name__ == "__main__":
    # Step 1: Connect to Mongo
    client = get_mongo_client(MONGO_HOST, MONGO_USERNAME, MONGO_PASSWORD)
    print("Connected to MongoDB")

    # Step 2: Fetch documents (last 7 days example)
    from datetime import datetime, timedelta

    seven_days_ago = datetime.utcnow() - timedelta(days=7)
    docs = fetch_documents(
        client,
        MONGO_DATABASE,
        MONGO_COLLECTION,
        query={"updatedAt": {"$gte": seven_days_ago}},
        projection={"_id": 0, "userId": 1, "updatedAt": 1},
        sort=[("updatedAt", -1)],
        limit=100,
    )
    print(f"Fetched {len(docs)} documents")

    # Step 3: Insert a test document
    insert_documents(client, MONGO_DATABASE, MONGO_COLLECTION, [{"name": "Test User", "createdAt": datetime.utcnow()}])

    # Step 4: Update documents
    update_documents(
        client,
        MONGO_DATABASE,
        MONGO_COLLECTION,
        updates=[{"filter": {"name": "Test User"}, "update": {"$set": {"status": "active"}}}],
        upsert=True,
    )

    # Step 5: Delete documents older than 30 days
    thirty_days_ago = datetime.utcnow() - timedelta(days=30)
    delete_documents(client, MONGO_DATABASE, MONGO_COLLECTION, {"updatedAt": {"$lt": thirty_days_ago}})