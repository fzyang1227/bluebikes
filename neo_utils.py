''' 
    Building a graph database in Neo4j using Python
    based on:
    https://towardsdatascience.com/create-a-graph-database-in-neo4j-using-python-4172d40f89c4

'''

from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable

class Neo4jConnection:

    def __init__(self, uri, user, pwd):
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(uri, auth=(user, pwd))
        except Exception as e:
            print("Failed to create the driver:", e)
        
    def close(self):
        if self.__driver is not None:
            self.__driver.close()
        
    def query(self, query, parameters=None):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try: 
            session = self.__driver.session() 
            response = list(session.run(query, parameters))
        except Exception as e:
            print("Query failed:", e)
        finally: 
            if session is not None:
                session.close()
        return response


def add_station(conn, name, lat, long, docks):
    ''' create a node in the database
    '''
    query = """
    CREATE (a:Station {name: $name, lat: $lat, long: $long, docks: $docks})
    RETURN a.name AS name, a.docks AS docks
    """
    parameters = {"name": name, "lat": lat, "long": long, "docks": docks}
    return conn.query(query, parameters)

def add_trip(conn, start, end, props):
    ''' create a relationship between two nodes
    '''
    query = f"""
    MATCH (s:Station {{name: $sid}}), (e:Station {{name: $eid}})
    MERGE (s)-[r:Trip {props}]->(e)
    RETURN type(r) AS type, r.total AS count, r.avg_duration AS duration, r.distance AS distance, r.anticipated_time AS time
    """
    parameters = {"sid": start, "eid": end}
    return conn.query(query, parameters)

def get_station(conn, name):
    ''' get a node from the database
    returns a dictionary of station properties
    '''
    query = """
    MATCH (a:Station {name: $name})
    RETURN a AS station
    """
    parameters = {"name": name}
    return conn.query(query, parameters)[0].data()['station']

def to_from_station(conn, name):
    ''' get all properties of all inbound and outbound relationships from a node
        returns a list of dictionary of relationship properties
    '''
    query = """
    MATCH (b)-[in]-(a:Station {name: $name})-[out]-(c)
    RETURN PROPERTIES(in) AS inbound, PROPERTIES(out) AS outbound
    """
    parameters = {"name": name}
    return [prop.data() for prop in conn.query(query, parameters)]

def count_trips(conn, name):
    ''' count the number of trips inbound and outbound from a station
        return a dictionary of counts
    '''
    query = """
        MATCH (n:Station {name: $name})
        MATCH ()-[rin]->(n)-[rout]->()
        RETURN sum(rin.total) AS incoming, sum(rout.total) AS outgoing
    """
    parameters = {"name": name}
    return conn.query(query, parameters)[0].data()

def get_trip(conn, station1, station2):
    ''' get a relationship between two stations 
        retruns a dictionary of relationship properties
    '''
    query = """
        MATCH (s:Station {name: $sid})-[r]-(e:Station {name: $eid})
        RETURN PROPERTIES(r) AS details
    """
    parameters = {"sid": station1, "eid": station2}
    return conn.query(query, parameters)[0].data()['details']
    
