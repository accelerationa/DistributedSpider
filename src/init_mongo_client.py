import pymongo
from database_enum import DatabaseStack

def init_mongo_client(stack):
    if stack == DatabaseStack.Local:
        return pymongo.MongoClient("mongodb://localhost:27017/")
    elif stack == DatabaseStack.Aws:
        mongodb_node_ip = '34.211.21.127'
        return pymongo.MongoClient("mongodb://{}:27017/".format(mongodb_node_ip))
    else: 
        return pymongo.MongoClient("mongodb+srv://accelerationa:<password>@cluster0.qclg8.mongodb.net/?retryWrites=true&w=majority")