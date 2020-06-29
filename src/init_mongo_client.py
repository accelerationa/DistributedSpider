import pymongo

def init_mongo_client(use_local_database = False):
    if use_local_database:
        return pymongo.MongoClient("mongodb://localhost:27017/")
    
    mongodb_node_ip = '34.211.21.127'
    return pymongo.MongoClient("mongodb://{}:27017/".format(mongodb_node_ip))