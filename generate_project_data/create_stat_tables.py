from postgreslib.reporting_tables import create_tables
from postgreslib.database_connection import DBConnection

def generate_reporting_tables(**kwargs):
    print("generate master data kwargs {}".format(kwargs))
    db = kwargs["db"]
    print("db outta kwargs = {}".format(db))
    dbc = DBConnection(db)
    create_tables(dbc)
