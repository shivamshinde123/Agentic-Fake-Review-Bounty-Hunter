# Import necessary modules
import os
import time
import hashlib
import sqlite3
import streamlit as st
from datetime import datetime
from neo4j_db_operations import Neo4jHandler


# Define a class for handling Login and Signup functionality
class LoginSignupClass:

    def __init__(self):
        """
        Initialize the SQLite connection and set table name.

        Args:
            database_location (str): Path to the SQLite database file.
            table_name (str): Name of the table storing user data.
        """
        self.uri = "bolt://localhost:7687"  # Neo4j server URI
        self.user = "neo4j"  # Username
        self.password = "password"  # Password
        self.handler = Neo4jHandler(self.uri, self.user, self.password)

    def authenticate_user_for_login(self, username, password):
        """
        Authenticate user credentials for login.

        Args:
            username (str): Entered username.
            age (int): Entered age
            password (str): Entered password.

        Returns:
            True if login successful, otherwise sets warning message in Streamlit session.
        """

        temp_user_node = self.handler.fetch_node("User", name=username)
        if temp_user_node is not None:
            user_node = self.handler.fetch_node(
                "User", name=username, password=password
            )
            if user_node is None:
                st.session_state["password_incorrect_warning"] = (
                    "Your password is incorrect!"
                )
                return st.session_state["password_incorrect_warning"]
            else:
                return True
        else:
            st.session_state["no_account_warning"] = (
                "You do not have an account. Please create a new one!"
            )
            return st.session_state["no_account_warning"]

    def authenticate_user_for_signup(self, username):
        """
        Check if the username already exists before signup.

        Args:
            username (str): Desired username.

        Returns:
            True if username is unique, otherwise sets warning message.
        """

        user_node = self.handler.fetch_node("User", name=username)
        if user_node is not None:
            st.session_state["existing_username_warnings"] = (
                "This username is already taken. Use the different one!"
            )
            return st.session_state["existing_username_warnings"]
        else:
            return True

    def login_signup_page(self):
        """
        Render the login/signup landing page UI.

        Returns:
            str: 'login' or 'signup' based on the button clicked.
        """
        col_a, col_b, col_c = st.columns([1, 10, 1])
        col_b.title("Fake Review Bounty Hunter")

        # Display homepage image
        st.image(
            os.path.join("test_phase", "src", "webapp", "Images", "HomePagePhoto.jpg")
        )

        col1, col2, col3, col4, col5, col6 = st.columns([2, 2, 2, 2, 2, 2])

        login_btn = col3.button("Login")
        signup_btn = col4.button("Sign Up")

        if login_btn:
            st.session_state["mode"] = "login"
        if signup_btn:
            st.session_state["mode"] = "signup"

        return st.session_state.get("mode", None)
            
    def create_id(self, text):
        """
        Generate a unique user ID using username and timestamp.

        Args:
            username (str): The username.

        Returns:
            str: A unique hashed user ID.
        """
        base = f"{text}-{time.time()}"
        return hashlib.sha256(base.encode()).hexdigest()[:22]

# Run the app
if __name__ == "__main__":

    # Initialize the login/signup class
    lsc = LoginSignupClass()

    # If not logged in, show login/signup page
    if not st.session_state.get("logged_in", False):

        # Render login/signup selection
        mode = lsc.login_signup_page()

        # Handle Signup
        if mode == "signup":
            username = st.text_input("Username")
            age = st.number_input("Age", min_value=0, max_value=100, step=1)
            password = st.text_input("Password", type="password")
            confirm_password = st.text_input("Confirm Password", type="password")

            if st.button("Submit", key="user_signup_submit"):
                if password != confirm_password:
                    st.warning("Passwords don't match!")
                else:
                    authenticated = lsc.authenticate_user_for_signup(username)
                    if authenticated is True:
                        user_id = lsc.create_id(username)
                        st.session_state["user_id"] = user_id

                        _ = lsc.handler.add_node(
                            "User",
                            user_id=user_id,
                            name=username,
                            age=age,
                            average_stars=0,
                            elite=0,
                            friends=0,
                            review_count=0,
                            yelping_since=datetime.now(),
                            password=password,
                        )
                        st.success(f"Account Created! Welcome {username}!")
                        st.session_state["username"] = username
                        st.session_state["logged_in"] = True
                    else:
                        st.warning(authenticated)

        # Handle Login
        elif mode == "login":
            username = st.text_input("Username")
            password = st.text_input("Enter Your Password", type="password")

            if st.button("Submit", key="user_login_submit"):
                authenticated = lsc.authenticate_user_for_login(username, password)
                if authenticated is True:
                    st.success(f"Welcome {username}!")
                    st.session_state["username"] = username
                    user_id = lsc.handler.fetch_node("User", name=username)["user_id"]
                    st.session_state["user_id"] = user_id
                    st.session_state["logged_in"] = True
                else:
                    st.warning(authenticated)

    # Show the homepage after login/signup
    if st.session_state.get("logged_in", False):
        business_node_list = lsc.handler.fetch_all_nodes("Business")
        business_dict = dict()

        for node in business_node_list:
            business_dict[node["name"]] = (node["business_id"], node["stars"])

        st.title("Add Review")
        selected_business = st.selectbox("Business", list(business_dict.keys()))
        user_review = st.text_input("Review")
        user_stars = st.selectbox("Stars", [1, 2, 3, 4, 5])

        if st.button("Submit Review", key="submit_review_btn"):
            review_id = lsc.create_id(user_review)
            rating_deviation = abs(user_stars - business_dict[selected_business][1])
            print(f"User Id: {st.session_state['user_id']}")
            print(f"Business ID: {business_dict[selected_business][0]}")
            _ = lsc.handler.create_relationship(
                st.session_state["user_id"],
                business_dict[selected_business][0],
                date=datetime.now(),
                rating_deviation=rating_deviation,
                review_id=review_id,
                review_stars=user_stars,
                text=user_review,
            )

            user_node_who_posted_review = lsc.handler.fetch_node("User", user_id=st.session_state['user_id'])
            business_node_that_user_reviewed = lsc.handler.fetch_node("Business", business_id=business_dict[selected_business][0])

            review_count_of_user_node_who_posted_review = user_node_who_posted_review['review_count']
            review_count_of_business = business_node_that_user_reviewed['review_count']

            lsc.handler.update_node("User", "user_id", user_node_who_posted_review['user_id'], review_count=review_count_of_user_node_who_posted_review+1)
            print(f"Updated the user review count")
            lsc.handler.update_node("Business", "business_id", business_node_that_user_reviewed['business_id'], review_count=review_count_of_business+1)
            print(f"Updated the business review count")
            st.success("Review Submitted")



