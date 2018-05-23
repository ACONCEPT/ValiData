import os
import sys
import json
from config.config import VALIDATION_FILE, BOOTSTRAP_SERVERS, PERFORMANCE_LOG, PERFORMANCE_RUNS, PERFORMANCE_DATA
from pprint import pprint
from postgreslib.database_connection import DBConnection
import pandas as pd
from dateutil import parser
from collections import defaultdict

# Setting up connections to postgres
#dbc = DBConnection("postgres_rds")
#dbc.get_base_table_descriptions()

def get_run_id():
    query = "select max(run_id) + 1 from run_stats;"
    return dbc.execute_query(query)[0][0]

    with open(PERFORMANCE_DATA,"w+") as f:
        f.write(json.dumps(result))

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

def average_run(run):
    result = {}
    for key,item in run.items():
        if key not in  ["cycles","notes"]:
            result[key] = sum(item)/len(item)
        else:
            result[key] = item
    result["notes"] = run["notes"]
    result["total"] = result["validcount"] + result["invalidcount"]
    result["check_customer_ids_velocity"] = result["total"]/result["check_customer_ids"]
    result["subtract_velocity"] = result["total"]/result["subtract"]
    result["kafkawrite_velocity"] = result["total"]/result["kafkawrite"]
    result["validator_velocity"] = result["total"]/result["validator"]
    return result

def analyze_performance():
    with open(PERFORMANCE_LOG , "r") as f:
        data = f.readlines()

    all_runs = []
    procs = defaultdict(list)
    for line in data:
        sl = line.split("|")
        time = sl.pop(0)
        sl = {x.split(":")[0].strip():x.split(":")[1].strip() for x in sl}
        if sl.get("driver"):
            if procs != {}:
                all_runs.append(procs)
            procs = defaultdict(list)
            sl.pop("driver")
            notes = ", ".join(["{} : {}".format(k,v) for k,v in sl.items()])
            procs["notes"] = notes
        else:
            procs[sl.get("label")].append(float(sl.get("duration")))
            valid_count = sl.get('valid_count',None)
            invalid_count = sl.get('invalid_count',None)
            if valid_count and invalid_count:
                procs["validcount"].append(int(valid_count))
                procs["invalidcount"].append(int(invalid_count))
            cyclecount = int(sl.get("cycle"))
    all_runs.append(procs)
    all_runs = [average_run(x) for x in all_runs]
    for i, run in enumerate(all_runs):
        msg = ["Run number {} in archive ".format(i)]
        msg += ["{} : {}".format(k,v) for k,v in run.items() if "velocity" in k
                or k in ("total","notes")]
        print("\n".join(msg))
        print()

if __name__ == "__main__":
    analyze_performance()
