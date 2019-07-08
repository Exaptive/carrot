from pymongo import MongoClient
import ssl

def connect(mongo_config):
    use_ssl = mongo_config.get('ssl', False)
    if use_ssl:
        client = MongoClient(mongo_config.get('url'), ssl = use_ssl, ssl_cert_reqs=ssl.CERT_NONE)
    else:
        client = MongoClient(mongo_config.get('url'))
    return client

def insert_records(client, db_name, collection_name, records):
    db = client[db_name]
    collection = db[collection_name]
    res = collection.insert_many(records)
    return res

def find_records(client, db_name, collection_name, query):
    db = client[db_name]
    collection = db[collection_name]
    res = collection.find(query)
    return res

def delete_records(client, db_name, collection_name, query):
    db = client[db_name]
    collection = db[collection_name]
    res = collection.delete_many(query)
    return res

def update_record(client, db_name, collection_name, query, operation, upsert):
    db = client[db_name]
    collection = db[collection_name]
    res = collection.update_one(query, operation, upsert)
    return res
