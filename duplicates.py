from pymongo import MongoClient
import pymongo

# Connect to MongoDB Atlas
client = pymongo.MongoClient("mongodb+srv://zkewed:zkewed123A@vehicalevaluation.d9ufa.mongodb.net/?retryWrites=true&w=majority")

db = client['data_store_dev']
collection = db['combined_tb']

def make_hashable(value):
    if isinstance(value, dict):
        return tuple(sorted((k, make_hashable(v)) for k, v in value.items()))
    elif isinstance(value, list):
        return tuple(make_hashable(v) for v in value)
    else:
        return value

seen = set()
duplicates_removed = 0
processed = 0

# ⚠️ Avoid using no_cursor_timeout — use batch_size instead
cursor = collection.find({}, batch_size=500)

for doc in cursor:
    processed += 1
    doc_id = doc["_id"]
    doc_copy = doc.copy()
    doc_copy.pop("_id", None)
    doc_copy.pop("inserted_datetime", None)  # Ignore this field in comparison

    doc_tuple = tuple(sorted((k, make_hashable(v)) for k, v in doc_copy.items()))

    if doc_tuple in seen:
        collection.delete_one({"_id": doc_id})
        print(f"🗑️ Removed duplicate: {doc_id}")
        duplicates_removed += 1
    else:
        seen.add(doc_tuple)

print(f"\n✅ Processed {processed} documents.")
print(f"✅ Duplicate removal completed. Total duplicates removed: {duplicates_removed}")
