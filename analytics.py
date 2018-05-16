import os
import sys
import json
from config.config import VALIDATION_FILE, BOOTSTRAP_SERVERS, PERFORMANCE_LOG, PERFORMANCE_RUNS
from pprint import pprint
from postgreslib.database_connection import DBConnection
import pandas as pd
from dateutil import parser

# Setting up connections to postgres
dbc = DBConnection("postgres_rds")
dbc.get_base_table_descriptions()

def get_run_id():
    query = "select max(run_id) + 1 from run_stats;"
    return dbc.execute_query(query)[0][0]

def parse_p_row(row):
    row = row.split("|")
    time = row[0].strip()
    proc = row[1].strip()
    status = row[2].strip()
    return time,proc,status

def parse_runs():
    with open(PERFORMANCE_LOG,"r") as f:
        data = f.readlines()
    runs = []
    run = []
    for d in data:
        time,proc,status = parse_p_row(d)
        if proc == "driver" and status == "start":
            print("newrun")
            newrun = True
        if newrun:
            if run:
                runs.append(run)
                run = []
                newrun = False
        run.append({"proc":proc,"status":status,"time":time})
    runs.append(run)
    print(len(runs))
    input("press enter to print all runs")
    for run in runs:
        print(type(run))
        input("press enter to print run")
        print(run)
    with open(PERFORMANCE_RUNS,"w+")  as f:
        f.write(json.dumps(runs))

def process_run(run):
    if len(run) < 10:
        return False
    print(len(run))
    all_procs = []
    for log in run:
        p = log["proc"].split(" ")
        p = [x.replace(",","").strip() for x in p]
        if p[0] not in all_procs:
            all_procs.append(p)

    print(all_procs)

def process_runs():
    with open(PERFORMANCE_RUNS,"r") as f:
        data = json.loads(f.read())
    for run in data:
        r = process_run(run)
        if r:
            break



def get_rule_list():
    with open(VALIDATION_FILE,"r" ) as f:
        data = json.loads(f.read())
    return [data.get(k) for k in data.keys()][0]

def get_records(query):
           response , header = dbc.execute_query(query)
           records = [{h.name:v for h,v in zip(header,r)} for r in response]
           return records

def get_parts():
    query = "selecy * from parts;"
    return get_records(query)

def get_stats():
    query = "select * from run_stats where run_id > 27 order by run_id;"
    return get_records(query)


def print_validations():
    with open(VALIDATION_FILE,"r") as f:
        data = json.loads(f.read())

    for table in data.keys():
        print(" table : {}".format(table))
        for rule in data.get(table):
            pprint(rule)

def track_performance():
    pd.set_option("display.max_columns",15)
    pd.set_option("display.max_rows",None)
    pd.set_option("display.width",500)
    stats = get_stats()
    stats_df = pd.DataFrame(stats)
    stats_df = stats_df[stats_df.amount != 0]
    stats_df = stats_df[stats_df.elapsed_time != 0]
    stats_df = stats_df.convert_objects(convert_numeric = True)

    stats_df["r_p_s"] = stats_df["amount"]/stats_df["elapsed_time"]

    average_by_topic  = stats_df.groupby(by = ["topic_name"])["amount","r_p_s"].mean()
    average_by_topic = average_by_topic.rename(columns = {"amount":"average_qty","r_p_s":"average_r_p_s"} )

    average_by_batch_size = stats_df.groupby(by = ["topic_name","batch_size"])["amount","r_p_s"].mean()
    average_by_batch_size = average_by_batch_size.rename(columns = {"amount":"average_qty","r_p_s":"average_r_p_s"} )

    print("\n\n")
    print(average_by_topic)
    print("\n\n")
    print(average_by_batch_size)

from kafka import KafkaConsumer
from time import time
def track_kafka_speed():
    topic = "ingestion.postgres_rds.sales_orders"
    consumer = KafkaConsumer(topic,\
            group_id  = "test",\
            bootstrap_servers=BOOTSTRAP_SERVERS,\
            auto_offset_reset ="smallest",\
            value_deserializer =lambda m: json.loads(m.decode('utf-8')))

    n = 10000
    s = time()
    for i, message in enumerate(consumer):
        if i == 10:
            break
    e = time()
    elapsed = e - s
    r_s = n/elapsed
    print("{} records in {} seconds {} r/s".format(n,e-s,r_s))


if __name__ == "__main__":
    process_runs()
#    parse_runs()
#    parse_performance()
#    track_performance()
#    track_kafka_speed()
