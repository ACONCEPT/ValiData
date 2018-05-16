import os
from config.database_connections import source_databases

def get_url(dbname):
    print("geturl dbname {}".format(dbname))
    conn = source_databases.get(dbname)["connection_details"]
    base = "jdbc:postgresql://{}:{}/{}"
    properties = {}
    all_properties = {x.split("=")[0]:x.split("=")[1].replace("'","") for x in conn.split(" ") if "=" in x}
    url = base.format(all_properties.pop("host"),all_properties.pop("port"),all_properties.pop("dbname"))
    for key, item in all_properties.items():
        properties[key] = item
    return url, properties

