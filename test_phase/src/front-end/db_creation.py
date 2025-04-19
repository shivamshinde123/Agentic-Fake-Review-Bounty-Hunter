import os
import sqlite3
import pandas as pd

class DBManagement:
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
    database_name = "userdata"
    table_name = "userdata"

    # Initialize the DBManagement class
    db_manage_class = DBManagement(os.path.join("Data", f"{database_name}.db"))

    # SQL query to create the userdata table
    create_table_query = f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
    user_id TEXT PRIMARY KEY NOT NULL,
    username TEXT NOT NULL,
    password TEXT NOT NULL
    )
   '''

    # SQL query to drop the userdata table if it exists
    drop_table_query = f'''
    DROP TABLE IF EXISTS {table_name}
    '''

    # SQL query to insert values into the userdata table
    insert_table_query = f'''
        INSERT INTO {table_name} (user_id, username, password) VALUES (?, ?, ?)
    '''

    # Drop the table if it exists and recreate it
    db_manage_class.run_query(drop_table_query)
    db_manage_class.run_query(create_table_query)

    # Load user data from a CSV file
    existing_users_df = pd.read_csv(os.path.join("test_phase", "src", "front-end", "Data", "users.csv"))

    # Insert each user from the CSV into the database with a default password
    for i in range(existing_users_df.shape[0]):
        row = list(existing_users_df.loc[i, ['user_id', 'name']])
        row.append('password')  # Default password for each user
        db_manage_class.add_row_in_table(insert_table_query, tuple(row))

