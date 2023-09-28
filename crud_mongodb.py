from pymongo.mongo_client import MongoClient

conn_str= "mongodb+srv://tanisha_verma:tanishaverma@cluster0.okuqfob.mongodb.net/?retryWrites=true&w=majority"

mongo = MongoClient(conn_str)
print(mongo.list_database_names())

"""
1. connect database and connect collection
method_1
    2. Insert single record
    3. insert a whole csv with pandas implementation
method_2
    4. Read: Read a particular record or by default fetch all the data from the given input 
    collection name and reurn a pandas dataframe
metod_3,4
    5. Update/Detele: Updated/delete single record or every record of that particular condition 
"""
