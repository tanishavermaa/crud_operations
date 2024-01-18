
import logging as lg
lg.basicConfig(filename='mongo_database.log', level=lg.INFO, format='%(asctime)s - %(message)s')

import mysql.connector as connection


 

class  SqlCrud:
    
    def __init__(self,
                 user : str,
                 host : str,
                 password : str):
        
        self.connection= connection.connect(
            user=user,
            host=host,
            password=password
            
        )  
        
        
    def create_database(self,database_name:str):
        try:
            mycursor=self.connection.cursor()
            mycursor.execute(f"create database  if not exists {database_name}")
            
        except Exception as e:
            lg.error("Error creating database: ", e )
            print(e)
            
    def show_databases(self):
        """
        Show a list of all databases on the MySQL server.
        """
        try:
            mycursor = self.connection.cursor()
            mycursor.execute("SHOW DATABASES")
            databases = mycursor.fetchall()
            print("Databases on the MySQL server:")
            for database in databases:
                print(database[0])
        except Exception as e:
            lg.error("Error showing databases: ", e)
            print(e)
            
    def select_database(self, database_name: str):
        try:
            mycursor = self.connection.cursor()
            mycursor.execute(f"USE {database_name}")
            mycursor.close()
        except Exception as e:
            lg.error("Error selecting database: ", e)
            print(e)
            
        
    def create_table(self, create_table_query: str):
        try:
            mycursor = self.connection.cursor()
            query = f"CREATE TABLE IF NOT EXISTS {create_table_query}"
            mycursor.execute(query)
        except Exception as e:
            lg.error("Error creating table: ", e)
            print(e)
            
    def show_tables(self):
        """
        Show a list of all tables in the current database.
        """
        try:
            mycursor = self.connection.cursor()
            mycursor.execute("SHOW TABLES")
            tables = mycursor.fetchall()
            print("Tables in the current database:")
            for table in tables:
                print(table[0])
        except Exception as e:
            lg.error("Error showing tables: ", e)
            print(e)
            
    def insert_data_into_table(self, table_name, data):
        """
        Insert data into a specified table.
        Example:
        {
            "StudentID": 1,
            "Name": "John Doe",
            "Gender": "Male",
            "City": "New York"
        }
        """
        try:
            mycursor = self.connection.cursor()

            columns = ', '.join(data.keys())
            placeholders = ', '.join(['%(' + col + ')s' for col in data.keys()])
            
            query = f"""
            INSERT INTO {table_name} ({columns})
            VALUES ({placeholders})
            """

            mycursor.execute(query, data)
            

        except Exception as e:
            lg.error(f"Error inserting data into {table_name} table: ", e)
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
            
        
    def update_data_in_table(self, table_name, column_to_update, new_value, id_to_update):
        """
        Update data in a specified table.

        :param table_name: The name of the table.
        :param column_to_update: The column to update.
        :param new_value: The new value to set.
        :param id_to_update: The ID to identify the row to update.
        """
        try:
            mycursor = self.connection.cursor()
            
           
            valid_columns = ['Name', 'Gender', 'City'] 
            if column_to_update not in valid_columns:
                raise ValueError(f"Invalid column: {column_to_update}")

            query = f"UPDATE {table_name} SET {column_to_update} = %s WHERE StudentID = %s"
            mycursor.execute(query, (new_value, id_to_update))
           

        except Exception as e:
            lg.error(f"Error updating data in {table_name} table: ", e)
            print(e)
    
    def delete_record(self, table_name, condition_column, condition_value):
        """
        Delete a record from the specified table based on a condition.
        """
        try:
            mycursor = self.connection.cursor()
            valid_columns = ['StudentID', 'Name', 'Gender', 'City']
            if condition_column not in valid_columns:
                raise ValueError(f"Invalid column: {condition_column}")

            query = f"DELETE FROM {table_name} WHERE {condition_column} = %s"
            mycursor.execute(query, (condition_value,))
            self.connection.commit()
            print(f"Record deleted from '{table_name}' where {condition_column} = {condition_value}")
        except Exception as e:
            lg.error(f"Error deleting record from '{table_name}': ", e)
            print(e)
            
            
    def delete_whole_table(self,table_name):
        try:
            mycursor=self.connection.cursor()
            mycursor.execute(f"drop table {table_name}")
            print("Table dropped")
        except Exception as e:
            lg.error("Error deleting table : ", e)
            print(e)
            
    
    
conn = SqlCrud(user='root',host='localhost',password='tanisha')
conn.create_database("mydb")
conn.select_database('mydb')
create_table_query = """
Student (
    StudentID INT PRIMARY KEY,
    Name VARCHAR(255),
    Gender VARCHAR(10),
    City VARCHAR(100)
)
"""

conn.create_table(create_table_query)
student_data = {
    "StudentID": 1,
    "Name": "John",
    "Gender": "Male",
    "City": "New York"
}

conn.insert_data_into_table("Student", student_data)
conn.read_table("Student")
conn.update_data_in_table("Student", "Name", "Michael", 1)


print("\nAfter Update:")
conn.read_table("Student")
conn.delete_record("Student","StudentID",1)
conn.delete_whole_table("Student")