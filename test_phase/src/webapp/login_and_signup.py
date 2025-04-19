# Import necessary modules
import os
import time
import hashlib
import sqlite3
import streamlit as st

# Define a class for handling Login and Signup functionality
class LoginSignupClass:

    def __init__(self, user_database_location, user_table_name, business_database_location, business_table_name):
        """
        Initialize the SQLite connection and set table name.
        
        Args:
            database_location (str): Path to the SQLite database file.
            table_name (str): Name of the table storing user data.
        """
        self.user_database_location = user_database_location
        self.user_table_name = user_table_name
        self.business_database_location = business_database_location
        self.business_table_name = business_table_name

    def authenticate_user_for_login(self, username, password):
        """
        Authenticate user credentials for login.

        Args:
            username (str): Entered username.
            password (str): Entered password.

        Returns:
            True if login successful, otherwise sets warning message in Streamlit session.
        """
        with sqlite3.connect(self.user_database_location, check_same_thread=False) as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT password FROM {self.user_table_name} WHERE username = ?", (username,))
            rows = cursor.fetchall()

            if len(rows) == 1:  # User exists
                stored_password = rows[0]
                if str(password) == str(stored_password[0]):
                    return True
                else:
                    st.session_state['password_incorrect_warning'] = "Your password is incorrect!"
                    return st.session_state['password_incorrect_warning']

            if len(rows) == 0:  # No such user
                st.session_state['no_account_warning'] = "You do not have an account. Please create a new one!" 
                return st.session_state['no_account_warning']

    def authenticate_user_for_signup(self, username):
        """
        Check if the username already exists before signup.

        Args:
            username (str): Desired username.

        Returns:
            True if username is unique, otherwise sets warning message.
        """
        with sqlite3.connect(self.user_database_location, check_same_thread=False) as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT password FROM {self.user_table_name} WHERE username = ?", (username,))
            rows = cursor.fetchall()

            if len(rows) == 0:
                return True  # Username available
            
            if len(rows) == 1:
                st.session_state['existing_username_warnings'] = "This username is already taken. Use the different one!"
                return st.session_state['existing_username_warnings']
        
    def login_signup_page(self):
        """
        Render the login/signup landing page UI.

        Returns:
            str: 'login' or 'signup' based on the button clicked.
        """
        col_a, col_b, col_c = st.columns([4, 2, 4])
        col_b.title("Project")

        # Display homepage image
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
        """
        Generate a unique user ID using username and timestamp.

        Args:
            username (str): The username.

        Returns:
            str: A unique hashed user ID.
        """
        base = f"{username}-{time.time()}"
        return hashlib.sha256(base.encode()).hexdigest()[:22]
    
    def add_row_in_user_table(self, query, parameters_tuple):
        """
        Insert a new row into the user database.

        Args:
            query (str): SQL INSERT query.
            parameters_tuple (tuple): Tuple of values to insert.
        """
        with sqlite3.connect(self.user_database_location, check_same_thread=False) as conn:
            cursor = conn.cursor()
            cursor.execute(query, parameters_tuple)
            conn.commit()

    def add_row_in_business_table(self, query, parameters_tuple):
        """
        Insert a new row into the business database.

        Args:
            query (str): SQL INSERT query.
            parameters_tuple (tuple): Tuple of values to insert.
        """
        with sqlite3.connect(self.business_database_location, check_same_thread=False) as conn:
            cursor = conn.cursor()
            cursor.execute(query, parameters_tuple)
            conn.commit()

# Run the app
if __name__ == "__main__":

    # Database details
    user_database_name = "userdata"
    user_table_name = "userdata"

    business_database_name = "business_data"
    business_table_name = "business_data"

    # Initialize the login/signup class
    lsc = LoginSignupClass(
        os.path.join("Data", f"{user_database_name}.db"),
        user_table_name,
        os.path.join("Data", f"{business_database_name}.db"),
        business_table_name
    )

    # If not logged in, show login/signup page
    if not st.session_state.get('logged_in', False):

        # Render login/signup selection
        mode = lsc.login_signup_page()

        # Handle Signup
        if mode == 'signup':
            username = st.text_input("Username")
            password = st.text_input("Enter Your Password", type="password")
            confirm_password = st.text_input("Confirm Your Password", type="password")

            if st.button("Submit", key='user_signup_submit'):
                if password != confirm_password:
                    st.warning("Passwords don't match!")
                else:
                    authenticated = lsc.authenticate_user_for_signup(username)
                    if authenticated is True:
                        user_id = lsc.create_user_id(username)
                        insert_query = f'''
                            INSERT INTO {lsc.user_table_name} (user_id, username, password) 
                            VALUES (?, ?, ?)
                        '''
                        lsc.add_row_in_user_table(insert_query, (user_id, username, password))
                        st.success(f"Account Created! Welcome {username}!")
                        st.session_state['username'] = username
                        st.session_state['logged_in'] = True
                    else:
                        st.warning(authenticated)

        # Handle Login
        elif mode == 'login':
            username = st.text_input("Username")
            password = st.text_input("Enter Your Password", type="password")

            if st.button("Submit", key='user_login_submit'):
                authenticated = lsc.authenticate_user_for_login(username, password)
                if authenticated is True:
                    st.success(f"Welcome {username}!")
                    st.session_state['username'] = username
                    st.session_state['logged_in'] = True
                else:
                    st.warning(authenticated)

    # Show the homepage after login/signup
    if st.session_state.get('logged_in', False):
        with sqlite3.connect(lsc.business_database_location, check_same_thread=False) as conn:
            cursor = conn.cursor()
            query = f"SELECT DISTINCT business_name FROM {business_table_name}"
            cursor.execute(query)
            business_list = [row[0] for row in cursor.fetchall()]
        
        st.title("Add Review")
        selected_business = st.selectbox("Choose the Business", business_list)
        user_review = st.text_input('Review')

        if st.button('Submit Review', key='submit_review_btn'):
            st.write(f"Comment added by {st.session_state['username']} for the business '{selected_business}'. Review: {user_review}")
            # TODO: Store review in DB and create corresponding graph node if needed

        # Optionally add a logout button
        if st.button("Logout"):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.experimental_rerun()
