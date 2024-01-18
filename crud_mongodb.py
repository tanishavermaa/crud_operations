import pandas as pd
import pymongo
import json
import logging as lg
lg.basicConfig(filename='mongo_database.log', level=lg.INFO, format='%(asctime)s - %(message)s')


        
class MongoCrud:
    
    def __init__(self, connection_str: str):
        self.client = pymongo.MongoClient(connection_str)
    

    def create_database(self, database_name: str):
        self.database = self.client[database_name]
        
    def create_collection(self,collection_name:str):
        self.collection=self.database[collection_name]
        
    def insert_one_record(self,record_dict:dict):
        self.collection.insert_one(record_dict)
        
        
    def insert_many_records(self,excel_file_path:str):
        
        try:
            
            df = pd.read_excel(excel_file_path)
            df = df.iloc[:10]
            records = json.loads(df.to_json(orient='records'))
            self.collection.insert_many(records)
            
        except Exception as e:
            
            print(f"An error occurred: {str(e)}")
            
    def read_particular_record(self, record: dict):
        try:
            result = self.collection.find_one(record)
            
            if result:
                print(f"Record found: {result}")
            else:
                 print("Record not found.")
        except Exception as e:
            print(f"An error occurred: {str(e)}")

            
    
    def read_all_records(self):
        try:
            result=self.collection.find()
            for record in result:
                print(record)
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            
    
    def update_single_record(self,old_record:dict , new_record:dict):
        try:
            result = self.collection.update_one(old_record, {"$set": new_record})
            if result.matched_count > 0:
                print(f"Record updated successfully.")
            else:
                print("No matching record found.")
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            
        
    
    def update_many_records(self,filter_query:dict,updated_query:dict):
        
        try:
            result = self.collection.update_many(filter_query, {"$set": updated_query})
            print(f"Matched {result.matched_count} documents and modified {result.modified_count} documents.")
        except Exception as e:
            
            print(f"An error occurred: {str(e)}")
    
    def delete_single_record(self,record:dict):
        try:
            result = self.collection.delete_one(record)
            print(f"Deleted {result.deleted_count} document.")
        except Exception as e:
            print(f"An error occurred: {str(e)}")
    
    def delete_many_records(self,record:dict):
        try:
            result = self.collection.delete_many(record)
            print(f"Deleted {result.deleted_count} documents.")
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            
    def delete_all_records(self):
        try:
            result = self.collection.delete_many({})
            print(f"Deleted {result.deleted_count} documents.")
        except Exception as e:
            print(f"An error occurred: {str(e)}")
    
    def delete_collection(self):
        try:
            self.collection.drop()
            print("Collection deleted.")
        except Exception as e:
            print(f"An error occurred: {str(e)}")

    def delete_database(self):
        try:
            self.client.drop_database(self.database.name)
            print("Database deleted.")
        except Exception as e:
            print(f"An error occurred: {str(e)}")





 

# connection=MongoCrud("mongodb+srv://tanisha_verma:<password>@cluster0.okuqfob.mongodb.net/")
# mongo_db_client=connection.client
# connection.create_database("crud_database")
# connection.create_collection("crud_collection")

# data={"name": "John Doe","email": "johndoe@example.com","age": 30}
# connection.insert_one_record(data)
# path= r"Online_Retail.xlsx"
# connection.insert_many_records(path)
# connection.read_particular_record({"name":"John Doe"})
# connection.read_all_records()
# old_record = {"name": "John Doe"}
# new_record = {"age": 31}
# connection.update_single_record(old_record,new_record)
# filter_query = {"age": {"$gt": 25}}  
# updated_query = {"status": "Updated"} 
# connection.update_many_records(filter_query, updated_query)
# filter_query_single = {"name": "John Doe"}
# connection.delete_single_record(filter_query_single)
# filter_query_multiple = {"age": {"$lt": 30}}  
# connection.delete_many_records(filter_query_multiple)