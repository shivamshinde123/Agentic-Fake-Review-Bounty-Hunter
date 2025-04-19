import os
import time
import hashlib
import sqlite3
import streamlit as st

class LoginSignupClass:

    def __init__(self, database_location, table_name):
        """
        Initialize the database connection.

        Args:
            database_name (str): Location of the SQLite database file.
        """
        self.conn = sqlite3.connect(database_location)
        self.cursor = self.conn.cursor()
        self.table_name = table_name 

    def authenticate_user_for_login(self, username, password):
        
        self.cursor.execute(f"SELECT password FROM {self.table_name} WHERE username = ?", (username,))
        rows = self.cursor.fetchall()

        if len(rows) == 1:  # Login Scenario
            stored_password = rows[0]
            if str(password) == str(stored_password[0]):
                return True
            else:
                st.session_state['password_incorrect_warning'] = "Your password is incorrect!"
                return st.session_state['password_incorrect_warning']

        if len(rows) == 0:
            st.session_state['no_account_warning'] = "You do not have an account. Please create a new one!" 
            return st.session_state['no_account_warning']

    def authenticate_user_for_signup(self, username):

        self.cursor.execute(f"SELECT password FROM {self.table_name} WHERE username = ?", (username,))
        rows = self.cursor.fetchall()

        if len(rows) == 0: # Signup Scenario
            return True
        
        if len(rows) == 1:
            st.session_state['existing_username_warnings'] = "This username is already taken. Use the different one!"
            return st.session_state['existing_username_warnings']
        
        
    def login_signup_page(self):

        col_a, col_b, col_c = st.columns([4, 2, 4])

        col_b.title("Project")

        st.image(os.path.join("test_phase", "src", "front-end","Images", "HomePagePhoto.jpg"))
        
        col1, col2, col3, col4, col5, col6 = st.columns([2, 2, 2, 2, 2, 2])

        login_btn = col3.button('Login')
        signup_btn = col4.button('Sign Up')
    
        if login_btn:
            st.session_state['mode'] = 'login'
        if signup_btn:
            st.session_state['mode'] = 'signup'

        return st.session_state.get('mode', None)


    def create_user_id(self, username):
        base = f"{username}-{time.time()}"
        return hashlib.sha256(base.encode()).hexdigest()[:22]
    
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

    database_name = "userdata"
    table_name = "userdata"

    lsc = LoginSignupClass(os.path.join("Data", f"{database_name}.db"), table_name)

    mode = lsc.login_signup_page()

    if mode == 'signup':
        username = st.text_input("Username")
        password = st.text_input("Enter Your Password")
        confirm_password = st.text_input("Confirm Your Password")
        submit_btn = st.button("Submit")
        if submit_btn:
            if password != confirm_password:
                st.warning("Passwords don't match!")
            else:
                authenticated = lsc.authenticate_user_for_signup(username)
                if authenticated == True:
                    user_id = lsc.create_user_id(username)

                    # SQL query to insert values into the userdata table
                    insert_table_query = f'''
                        INSERT INTO {lsc.table_name} (user_id, username, password) VALUES (?, ?, ?)
                    '''

                    lsc.cursor.execute(insert_table_query, (user_id, username, password))
                    lsc.conn.commit()
                    st.success(f"Account Created! Welcome {username}!")
                else:
                    st.warning(authenticated)

        # *************ADD THE PATTERN CHECKING AND OTHER CODE*************

    if mode == 'login':
        username = st.text_input("Username")
        password = st.text_input("Enter Your Password")
        submit_btn = st.button("Submit")
        if submit_btn:
            authenticated = lsc.authenticate_user_for_login(username, password)
            if authenticated == True:
                st.success(f"Welcome {username}!")
            else:
                st.warning(authenticated)

        # *************ADD THE PATTERN CHECKING AND OTHER CODE*************


