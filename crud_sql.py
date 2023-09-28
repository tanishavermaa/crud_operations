
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
            mycursor.execute(f"create database {database_name}")
            
        except Exception as e:
            lg.error("Error creating database: ", e )
            print(e)
            
        
    def create_table(self, table_name:str,
                     column_and_typs:str):
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
            query = f"create table if not exists {table_name}({column_and_typs})"
        
            mycursor.execute(query)
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
            
    def delete_record(self,parameber):
        pass
    def delete_whole_table(self,table_name):
        try:
            mycursor=self.connection.cursor()
            mycursor.execute(f"drop table {table_name}")
        except Exception as e:
            lg.error("Error deleting table : ", e)
            print(e)
            
    
    
con = MyDatabase(user='root',host='localhost',password='tanisha')
#con.close()
con.create_database("mydb")
con.create_table(table_name="abc",
                 column_and_typs="PersonID int,LastName varchar(255),FirstName varchar(255),Address varchar(255), City varchar(255)")
# con.read_table()
# # con.update_table()
# # con.delete_table()