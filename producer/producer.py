from postgreslib.database_connection import DBConnection
from helpers.kafka import KafkaWriter, get_topic
from config.config import JSON_RECORDS
import json
import time
from json.decoder import JSONDecodeError
import os

class IngestionProducer(KafkaWriter):
    def __init__(self,bootstrap_servers,datasource,outfile = JSON_RECORDS):
        super().__init__(bootstrap_servers)
        self.datasource = datasource

    def get_records(self,table,number):
        self.db = DBConnection(self.datasource)
        print("running get records {}".format(number))
        generator,header = self.db.stream_table(table)
        def format_record(record):
            return {str(h.name):str(v) for h,v in zip(header,record)}
        try:
            out = []
            for i , x in enumerate(generator):
                out.append(format_record(x))
                if i == number:
                    reason = "break"
                    break
        except Exception as e:
            reason = e
        finally:
            resp = input("stopped on {} write to file? (y/n) : ".format(reason))
            write = resp == "y"
            print("\n chosen write {} ".format(write))
            if write:
                path = JSON_RECORDS
                print("out len {}".format(len(out)))
                with open(path,"w+") as f:
                    f.write(json.dumps(out))
                print("wrote {} records to \n{}".format(len(out),path))
            else:
                print("not writing")

    def get_records_json(self):
        path = JSON_RECORDS
        with open(path,"r") as f:
            data = json.loads(f.read())
        return data

    def ingest_data(self,table,number = False):
        print("in ingest data method max {}".format(number))
        records = self.get_records_json()

        print(" got {} records to stream".format(len(records)))
        topic = get_topic(self.datasource,table)

        print("streaming data from table {} to topic {}".format(table,topic))
        input("press enter to start producing")
        print("producing...")
        for i,record in enumerate(records):
            self.produce(record, topic)
            if number:
                if i == number:
                    break
        self.produce_debug("completed producing {}".format(table))

def cache_records(bootstrap_servers,db,table,number):
    print("main table {}".format(table))
    producer = IngestionProducer(bootstrap_servers,db)
    producer.get_records("sales_orders",number)
