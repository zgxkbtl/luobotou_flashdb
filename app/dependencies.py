from neo4j import GraphDatabase, basic_auth

with open("neo4j.txt", "r") as f:
    username, password = f.read().split("\n")
    
driver = GraphDatabase.driver("neo4j+s://5c0c176e.databases.neo4j.io", auth=basic_auth(username, password))

from strawberry.fastapi import GraphQLRouter, BaseContext

class CustomContext(BaseContext):
    def __init__(self):
        self.driver = driver

def custom_context_dependency() -> CustomContext:
    return CustomContext()