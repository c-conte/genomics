from sqlite3 import dbapi2
from neo4j import GraphDatabase
import time
import pandas as pd

GRAPH_NAME = ""
CONNECTION_STRING = "bolt://localhost:7687"
USER = ""
PASSWORD = ""

#Code adapted from https://github.com/cj2001/bite_sized_data_science/blob/main/notebooks/part25.ipynb

class Neo4jConnection:
    
    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
        except Exception as e:
            print("Failed to create the driver:", e)
        
    def close(self):
        if self.__driver is not None:
            self.__driver.close()
        
    def query(self, query, parameters=None, db=None):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try: 
            session = self.__driver.session(database=db) if db is not None else self.__driver.session() 
            response = list(session.run(query, parameters))
        except Exception as e:
            print("Query failed:", e)
        finally: 
            if session is not None:
                session.close()
        return response


def create_constraints(conn):
    conn.query('CREATE CONSTRAINT cells IF NOT EXISTS ON (c:Cell) ASSERT c.cellID IS UNIQUE', db=GRAPH_NAME)

def add_cells(rows, batch_size=10000):
    # Adds category nodes to the Neo4j graph.
  
    query = '''UNWIND $rows AS row
    MERGE (c:Cell {cellID: row.cell_id})
    SET c.cellType = row.cell_type
        , c.donor = row.donor
        , c.day = row.day
        , c.technology = row.technology
    RETURN count(*) as total
    '''
    
    return insert_data(query, rows, batch_size)

def process_and_add_metadata(filename):
    #read in csv to df
    cell_node_df = pd.read_csv(filename)
    
    #add cells to graph
    add_cells(cell_node_df)

def insert_data(query, rows, batch_size = 10000):
    # Function to handle the updating the Neo4j database in batch mode.

    total = 0
    batch = 0
    start = time.time()
    result = None

    while batch * batch_size < len(rows):

        res = conn.query(query, parameters={'rows': rows[batch*batch_size:(batch+1)*batch_size].to_dict('records')}, db=GRAPH_NAME)
        total += res[0]['total']
        batch += 1
        result = {"total":total, "batches":batch, "time":time.time()-start}
        print(result)

    return result


conn = Neo4jConnection(uri=CONNECTION_STRING, user=USER, pwd=PASSWORD)

create_constraints(conn)
process_and_add_metadata('metadata.csv')