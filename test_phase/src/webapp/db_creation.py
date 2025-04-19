import os
import sqlite3
import pandas as pd

class UserDBManagement:
    """
    A class to manage SQLite database operations such as connecting,
    running queries, and inserting rows.
    """

    def __init__(self, database_location):
        """
        Initialize the database connection.

        Args:
            database_name (str): Location of the SQLite database file.
        """
        self.conn = sqlite3.connect(database_location)
        self.cursor = self.conn.cursor()

    def run_query(self, query):
        """
        Executes a SQL query that does not require parameters.

        Args:
            query (str): The SQL query to execute.
        """
        self.cursor.execute(query)
        self.conn.commit()

    def add_row_in_table(self, query, parameters_tuple):
        """
        Executes a parameterized SQL query to insert a row.

        Args:
            query (str): The SQL insert query with placeholders.
            parameters_tuple (tuple): The values to insert.
        """
        self.cursor.execute(query, parameters_tuple)
        self.conn.commit() 

if __name__ == "__main__":
    # Name of the SQLite database file
    user_database_name = "userdata"
    business_database_name = "business_data"
    user_table_name = "userdata"
    business_table_name = "business_data"

    # Initialize the DBManagement class
    user_db_manage_class = UserDBManagement(os.path.join("Data", f"{user_database_name}.db"))
    business_db_manage_class = UserDBManagement(os.path.join("Data", f"{business_database_name}.db"))

    # SQL query to create the userdata table
    user_create_table_query = f'''
    CREATE TABLE IF NOT EXISTS {user_table_name} (
    user_id TEXT PRIMARY KEY NOT NULL,
    username TEXT NOT NULL,
    password TEXT NOT NULL
    )
   '''
    
    business_create_table_query = f'''
    CREATE TABLE IF NOT EXISTS {business_table_name} (
    business_name TEXT NOT NULL
    )
    '''

    # SQL query to drop the userdata table if it exists
    user_drop_table_query = f'''
    DROP TABLE IF EXISTS {user_table_name}
    '''

    business_drop_table_query = f'''
    DROP TABLE IF EXISTS {business_table_name}
    '''

    # SQL query to insert values into the userdata table
    user_insert_table_query = f'''
        INSERT INTO {user_table_name} (user_id, username, password) VALUES (?, ?, ?)
    '''
    business_insert_table_query = f'''
        INSERT INTO {business_table_name} (business_name) VALUES (?)
    '''

    # Drop the table if it exists and recreate it
    user_db_manage_class.run_query(user_drop_table_query)
    user_db_manage_class.run_query(user_create_table_query)

    business_db_manage_class.run_query(business_drop_table_query)
    business_db_manage_class.run_query(business_create_table_query)


    # Load user data from a CSV file
    existing_users_df = pd.read_csv(os.path.join("test_phase", "src", "webapp", "Data", "users.csv"))

    existing_business_df = pd.read_csv(os.path.join("test_phase", "src", "webapp", "Data", "businesses.csv"))

    # Insert each user from the CSV into the database with a default password
    for i in range(existing_users_df.shape[0]):
        row = list(existing_users_df.loc[i, ['user_id', 'name']])
        row.append('password')  # Default password for each user
        user_db_manage_class.add_row_in_table(user_insert_table_query, tuple(row))

    for i in range(existing_business_df.shape[0]):
        row = list(existing_business_df.loc[i, ['name']])
        business_db_manage_class.add_row_in_table(business_insert_table_query, tuple(row))

