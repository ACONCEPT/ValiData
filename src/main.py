import sys
import os
path_to_append = os.environ.get("PROJECT_HOME")
print("appending {}".format(path_to_append))
sys.path.append(path_to_append)

import json
from config import database_connections
from config.config import TESTING_SERVER, BOOTSTRAP_SERVERS, VALIDATION_FILE
from generate_project_data.generate_master_data import generate_master_data
from generate_project_data.generate_orders import generate_orders
from generate_project_data.create_stat_tables import generate_reporting_tables
from producer import producer
from helpers.validation import ValidationRule

def load_validation_rules(filepath, table, use_rules = False):
    """designed to help the main run_spark function to get validation rules from config"""
    print ("bootstraps {}, db {}".format(bootstrap_servers,db))
    with open(VALIDATION_FILE,"r" ) as f:
        data = json.loads(f.read())
    rulelist = data.get(table)
    print(rulelist)
    if not use_rules:
        return [ValidationRule.from_json(x) for x in rulelist]
    else:
        result = [ValidationRule.from_json(x) for x in rulelist]
        return [x for x in result if x.name in use_rules]

def run_spark(bootstrap_servers,db,table):
    """ to run the main spark streaming job"""
    from StreamValidator.validata import stream_validation
    validation_config = load_validation_rules(VALIDATION_FILE,table,use_rules = ["check_customer_ids","check_lead_time"])
#    validation_config = load_validation_rules(VALIDATION_FILE,table,use_rules = ["check_customer_ids"])
    print("main giving validation config {}".format([rule.name for rule in validation_config]))
    stream_validation(bootstrap_servers,db,table,validation_config)

def order_datagen(bootstrap_servers,db,table,**kwargs):
    """to create mock orders"""
    generate_orders(db = db)

def master_datagen(bootstrap_servers,db,table,**kwargs):
    """to create mock master data"""
    generate_master_data(db = db)

def cache_records(bootstrap_servers,db,table):
    """to preprocess produced data into json"""
    try:
        number = int(sys.argv[2].strip())
    except Exception as e:
        number = False
    producer.cache_records(bootstrap_servers,db,table,number)

def start_producer(bootstrap_servers,db,table):
    """to send data to the injestion kafka topics"""
    topic = sys.argv[1].strip()
    try:
        number = int(sys.argv[2])
    except:
        number = False
    print("main table {}".format(table))
    ingp = producer.IngestionProducer(bootstrap_servers,db)
    ingp.ingest_data(table,number)

def reporting_tables(bootstrap_servers,db,table):
    """separate table reset for the performance reporting tables"""
    generate_reporting_tables(db = db)

if __name__ == "__main__":
    if "joe" in os.environ.get("HOME"):
        print("activating test settings ")
        bootstrap_servers = TESTING_SERVER
        db =  "test_database"
    else:
        print("activating cloud settings")
        db =  "postgres_rds"
        bootstrap_servers = BOOTSTRAP_SERVERS

    table = "sales_orders"
    module = sys.modules[__name__]
    run = sys.argv[1]
    print("activating {} with {} ".format(run, (bootstrap_servers,db,table)))
    getattr(module,run)(bootstrap_servers,db,table)
