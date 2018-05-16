from kafka import KafkaProducer
import json
from datetime import datetime

def getjsonproducer(bootstrap_servers):
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers,\
                             value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    print("creating producer for bootstrap servers {}".format(bootstrap_servers))
    return producer

def getstrproducer(bootstrap_servers):
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers,\
                             value_serializer=lambda v: v.encode("utf-8"))
    print("creating producer for bootstrap servers {}".format(bootstrap_servers))
    return producer

def get_topic(datasource, table):
    return "ingestion.{}.{}".format(datasource,table)

class KafkaWriter(object):
    def __init__(self,bootstrap_servers,datasource = None, table = None,stat_interval = 50):
        self.jsonproducer = getjsonproducer(bootstrap_servers)
        self.strproducer = getstrproducer(bootstrap_servers)
        self.datasource = datasource
        self.table = table
        self.counts = {}
        self.stat_interval = stat_interval

    def produce_debug(self,msg):
        self.produce(msg,"debug")

    def produce_stats(self,msg):
        self.produce(msg,"stats",stat = False)

    def produce_reject(self,msg):
        self.produce(msg,"rejection")

    def produce_valid(self,msg):
        self.produce(msg,"validation")

    def produce(self,msg,topic, stat = True):
        try:
            self.jsonproducer.send(topic,json.dumps(msg))
            self.jsonproducer.flush()
        except:
            self.strproducer.send(topic,msg)
            self.strproducer.flush()

        if stat:
            if not self.counts.get(topic):
                self.counts[topic] = 0
                stat = {}
                stat["topic"] = topic
                stat["count"] = self.counts[topic]
                stat["timestamp"] = datetime.utcnow().isoformat()
                self.produce_stats(stat)

            self.counts[topic] += 1
            if self.counts[topic] % self.stat_interval == 0:
                stat = {}
                stat["topic"] = topic
                stat["count"] = self.counts[topic]
                stat["timestamp"] = datetime.utcnow().isoformat()
                self.produce_stats(stat)

    def main_suffix(self,datasource = False,table = False):
        suffix = []
        if datasource:
            suffix.append(datasource)
        else:
            suffix.append(self.datasource)
        if table:
            suffix.append(table)
        else:
            suffix.append(self.table)
        return ".".join(suffix)

    def get_ingestion_topic(self):
        suffix = self.main_suffix()
        return "ingestion.{}".format(".".join(suffix))

    def get_next_topic(self,validity,rejectionrule = False):
        if validity:
            base = "validated.{}"
            suffix = self.main_suffix()
            topic = base.format(suffix)
        else:
            topic = "invalid.{}".format(rejectionrule)
        return topic

    def send_next(self,record,validity,rejectionrule):
        topic = self.get_next_topic(validity,rejectionrule)
        self.produce(record,topic)

    def test_handler(self,message):
        records = message.collect()
        for record in records:
            self.producer.send("sparkout",str(record))
            with open(os.environ["HOME"] + "/sparkout.txt","w+") as f:
                f.write(record)
            self.producer.flush()

    def stat_remnants(self):
        stat = {}
        for k, i in self.counts.items():
                stat["topic"] = k
                stat["count"] = i
                stat["timestamp"] = datetime.utcnow().isoformat()
                self.produce_stats(stat)




