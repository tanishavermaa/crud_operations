
import logging as lg
import mysql.connector as connection
lg.basicConfig(filename='database.log', level=lg.INFO, format='%(asctime)s - %(message)s')

 

class  MyDatabase:
    
    def __init__(self,
                 user : str,
                 host : str,
                 password : str):
        
        self.connection= connection.connect(
            user=user,
            host=host,
            password=password
            
        )  
        
        
    def create_database(self,database_name):
        try:
            mycursor=self.connection.cursor()
            mycursor.execute(f"create database  if not exists {database_name}")
            
        except Exception as e:
            lg.error("Error creating database: ", e )
            print(e)
            
        
    def create_table(self, create_table_query:str):
        """example:
        ```python
            column_and_types = " PersonID int,
            LastName varchar(255),
            FirstName varchar(255),
            Address varchar(255),
            City varchar(255)"
            
            
            """
        try:
            
            mycursor=self.connection.cursor()
            
           
                
            query = f"CREATE TABLE IF NOT EXISTS {create_table_query} "
            print(query)
            
            
            
            
        
        
            mycursor.execute(create_table_query)
        except Exception as e:
            lg.error("Error creating table : ", e)
            print(e)
    
    def read_table(self,table_name:str):
        try:
            mycursor=self.connection.cursor()
            mycursor.execute(f"select * from {table_name}")
            result = mycursor.fetchall()
            for row in result:
                print(row)
        except Exception as e:
            lg.error("Error reading table: ", e)
            print(e)
            
        
    def update_table(self,table_name,new_name,id_to_update):
        try:
            mycursor=self.connection.cursor()
            mycursor.execute(f"UPDATE {table_name} SET name= %s WHERE id= %s",(new_name,id_to_update))
        except Exception as e:
            lg.error("Error updating table : " , e)
            print(e)
            
    def delete_record(self,parameter):
        pass
    def delete_whole_table(self,table_name):
        try:
            mycursor=self.connection.cursor()
            mycursor.execute(f"drop table {table_name}")
        except Exception as e:
            lg.error("Error deleting table : ", e)
            print(e)
            
    
    
conn = MyDatabase(user='root',host='localhost',password='tanisha')
#conn.close()
conn.create_database("test_database")

query1="test(id INT PRIMARY KEY, name VARCHAR(255), marks INT);"
conn.create_table(create_table_query=query1)
