import sys
from kafka import KafkaConsumer
import os
import json

def consume_test_topic(bootstrap_servers,topic):
    insert_args = {}

    consumer = KafkaConsumer(topic,\
            group_id  = "test",\
            bootstrap_servers=bootstrap_servers,\
            auto_offset_reset ="smallest",\
            value_deserializer =lambda m: json.loads(m.decode('utf-8')))

    for message in consumer:
        try:
            try:
                val = json.loads(message.value)
            except TypeError as e:
                if isinstance(message.value,dict):
                    val = message.value
                else:
                    raise e
            offset = message.offset
            if isinstance(val,dict):
                for key, item in val.items():
                    print("topic :  {} offset : {] | {} : {} type {}".format(message.topic,message.offset, key,item,type(item)))
            else:
                print(val)
        except Exception as e:
            print("exception {} on offset {} value {}".format(e,message.offset,message.value))

if __name__ == '__main__':
    global DEFINITIONS
    fn = os.environ.get("HOME") +"/clusterinfo"
    topic = sys.argv[1].strip()
    with open(fn ,"r") as f:
        bootstrap_servers = ["{}:9092".format(x) for x in f.readlines()]
    if "joe" in os.environ.get("HOME"):
        print("setting test bootstrap server")
        bootstrap_servers = ["{}:9092".format("localhost")]
    consume_test_topic(bootstrap_servers,topic)

