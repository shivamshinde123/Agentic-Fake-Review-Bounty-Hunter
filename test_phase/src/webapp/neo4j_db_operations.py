from neo4j import GraphDatabase

class Neo4jHandler:
    """
    A handler class for Neo4j operations: connecting, adding nodes, and creating relationships.
    """

    def __init__(self, uri, user, password):
        # Initialize the Neo4j driver with connection URI, username, and password
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Close the Neo4j database connection
        self.driver.close()

    def add_node(self, node_label, **node_parameters):
        """
        Create a node with a given label and dynamic properties.
        :param node_label: The label for the node (e.g., 'User' or 'Business')
        :param node_parameters: Arbitrary keyword arguments as node properties
        :return: The created node object
        """
        # Format the properties as Cypher query parameters
        node_properties = ", ".join([f"{key}: ${key}" for key in node_parameters])
        # Prepare the Cypher query string
        query = f"CREATE (n:{node_label} {{ {node_properties} }}) RETURN n"
        # Open a new session and execute the write transaction
        with self.driver.session() as session:
            return session.execute_write(
                lambda tx: self._execute_add_node(tx, query, **node_parameters)
            )
        
        print("Node Created")

    def _execute_add_node(self, tx, query, **node_parameters):
        """
        Internal method to run the node creation query inside a transaction.
        :param tx: The transaction context
        :param query: The Cypher query string
        :param node_parameters: Properties for the node
        :return: The created node object, or None if failed
        """
        result = tx.run(query, **node_parameters)
        record = result.single()
        # Return the created node or None if not found
        return record["n"] if record else None

    def create_relationship(self, user_id, business_id, **review_properties):
        """
        Create a REVIEWED relationship between a User and a Business node with dynamic properties.
        :param user_id: ID of the User node
        :param business_id: ID of the Business node
        :param review_properties: Arbitrary keyword arguments as relationship properties
        :return: The created relationship object
        """
        # Format the relationship properties for Cypher query
        relationship_properties = ", ".join(
            [f"{key}: ${key}" for key in review_properties]
        )
        # Cypher query to match nodes and create the relationship
        query = f"""
        MATCH (u:User {{user_id: $user_id}}), (b:Business {{business_id: $business_id}})
        CREATE (u)-[r:REVIEWED {{ {relationship_properties} }}]->(b)
        RETURN r
        """
        # Open a new session and execute the write transaction
        with self.driver.session() as session:
            return session.execute_write(
                lambda tx: self._execute_create_relationship(
                    tx,
                    query,
                    user_id=user_id,
                    business_id=business_id,
                    **review_properties,
                )
            )
        
        print("Relationship created")

    def _execute_create_relationship(self, tx, query, user_id, business_id, **params):
        """
        Internal method to run the relationship creation query inside a transaction.
        :param tx: The transaction context
        :param query: The Cypher query string
        :param params: Parameters for the relationship and node identifiers
        :return: The created relationship object, or None if not found
        """
        params = {**params, "user_id": user_id, "business_id": business_id}
        result = tx.run(query, **params)
        record = result.single()
        # Return the created relationship or None if not found
        return record["r"] if record else None

    def fetch_node(self, node_label, **match_properties):
        """
        Fetch a node by label and one or more properties.
        :param node_label: The label of the node (e.g., 'User')
        :param match_properties: Keyword arguments for properties to match (e.g., user_id='u12345')
        :return: The node object or None if not found
        """
        if not match_properties:
            raise ValueError("You must specify at least one property to match.")

        # Build property match string for Cypher dynamically
        match_str = ", ".join([f"{key}: ${key}" for key in match_properties])
        query = f"MATCH (n:{node_label} {{ {match_str} }}) RETURN n"
        with self.driver.session() as session:
            return session.execute_write(lambda tx: self._execute_fetch_node(tx, query, **match_properties))
        
        print("node fetched")

    def _execute_fetch_node(self, tx, query, **properties):
        result = tx.run(query, **properties)
        record = result.single()
        return record["n"] if record else None
    
    def update_node(self, node_label, match_key, match_value, **new_properties):
        """
        Update properties of a node identified by a unique key and value.
        :param node_label: Node label (e.g., 'User')
        :param match_key: The name of the property to match (e.g., 'user_id')
        :param match_value: The value to match (e.g., 'u123')
        :param new_properties: Properties to update/add
        :return: The updated node object or None if not found
        """
        # Set clause for properties to update
        set_str = ", ".join([f"n.{key} = ${key}" for key in new_properties])
        query = (
            f"MATCH (n:{node_label} {{{match_key}: $match_value}}) "
            f"SET {set_str} "
            f"RETURN n"
        )
        params = {"match_value": match_value, **new_properties}
        with self.driver.session() as session:
            return session.execute_write(lambda tx: self._execute_update_node(tx, query, **params))
        
        print("node updated")
    
    def _execute_update_node(self, tx, query, **params):
        result = tx.run(query, **params)
        record = result.single()
        return record["n"] if record else None
    
    def fetch_all_nodes(self, node_label):
        """
        Fetch all nodes with a given label.
        :param node_label: The label of the node (e.g. 'User')
        :return: List of node objects
        """
        query = f"MATCH (n:{node_label}) RETURN n"
        with self.driver.session() as session:
            return session.execute_write(lambda tx: self._execute_fetch_all_nodes(tx, query))

    def _execute_fetch_all_nodes(self, tx, query):
        result = tx.run(query)
        return [record["n"] for record in result]

    def _execute_fetch_relationships(self, tx, query, **params):
        """
        Execute the relationship fetching query within a transaction.
        """
        result = tx.run(query, **params)
        relationships = [record["r"] for record in result]
        return relationships
        
    def fetch_relationships(self, user_id, business_id, relationship_type="REVIEWED"):
        """
        Fetch all relationships of a given type between a User and a Business.
        """
        query = f"""
        MATCH (u:User {{user_id: $user_id}})-[r:{relationship_type}]->(b:Business {{business_id: $business_id}})
        RETURN r
        """
        params = {"user_id": user_id, "business_id": business_id}
        with self.driver.session() as session:
            return session.execute_write(lambda tx: self._execute_fetch_relationships(tx, query, **params))
        
        print("relationship fetched")

if __name__ == "__main__":
    # --- Main execution: Replace these credentials as needed ---
    uri = "bolt://localhost:7687"   # Neo4j server URI
    user = "neo4j"                  # Username
    password = "12345678"           # Password

    # Create an instance of the Neo4j handler
    neo4j_handler = Neo4jHandler(uri, user, password)

    try:
        # --- Add a User node with properties ---
        # user_properties = {
        #     "user_id": "u12345",
        #     "name": "Shivam",
        #     "age": 30,
        #     "average_stars": 4.5,
        #     "elite": "2019, 2020",
        #     "friends": 150,
        #     "review_count": 100,
        #     "yelping_since": "2015-06-01",
        # }
        # user_node = neo4j_handler.add_node("User", **user_properties)
        # print(f"User node created: {user_node}")

        # # --- Add a Business node with properties ---
        # business_properties = {
        #     "business_id": "b4567",
        #     "categories": "Coffee, Bakery",
        #     "city": "Downtown",
        #     "name": "XYZ Coffee Shop",
        #     "review_count": 200,
        #     "stars": 4.2,
        #     "state": "CA",
        # }
        # business_node = neo4j_handler.add_node("Business", **business_properties)
        # print(f"Business node created: {business_node}")

        # # --- Create a REVIEWED relationship between user and business ---
        # review_properties = {
        #     "rate": 5,
        #     "rating_deviation": 0.2,
        #     "review_id": "r789",
        #     "review_star": 5,
        #     "text": "Great coffee and atmosphere!",
        # }
        # relationship = neo4j_handler.create_relationship(
        #     user_id="u12345", business_id="b4567", **review_properties
        # )
        # print(f"Relationship created: {relationship}")

        # # Fetch the user node
        user = neo4j_handler.fetch_node("User", user_id="u12345")
        print("Fetched user:", user)

        # # Update the user's name and age
        # updated_user = neo4j_handler.update_node(
        #     "User", "user_id", "u12345", name="Updated Shivam", age=31
        # )
        # print("Updated user:", updated_user)
        #  # Fetch relationships
        # relationships = neo4j_handler.fetch_relationships(user_id="u12345", business_id="b4567")
        # print(f"Fetched relationships: {relationships}")

        # Example 5: Fetching all users
        # all_users = neo4j_handler.fetch_all_nodes("Business")
        # print(f"All users: {all_users[0]['name']}")

    except Exception as e:
        # Print out any errors encountered in the process
        print(f"Error: {e}")

    finally:
        # Ensure the Neo4j connection is closed, even if an error occurs
        neo4j_handler.close()
