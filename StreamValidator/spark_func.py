from pyspark import SparkContext, SQLContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row,SparkSession
from pyspark.sql.functions import to_json,struct, col, lit
from pyspark.sql.utils import IllegalArgumentException
from helpers.get_data import get_url
from pyspark.sql.types import DateType
from helpers.kafka import KafkaWriter, get_topic, getjsonproducer,getstrproducer
from config import config
import json, math, datetime
import copy
#rules are defined in the project-root/config/methods.py file
from config.methods import validation_functions
from datetime import datetime
from time import time
CYCLES = 0

def getSparkSessionInstance(sparkConf):
    """get a single instance of the spark session """
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

def write_performance_log(proc,status):
    msg  = "{} | {}".format(proc,status)
    with open(config.PERFORMANCE_LOG,"a+") as f:
        f.write("{} | {}\n".format(datetime.now().isoformat(),msg))


def stream_validation(bootstrap_servers,datasource,table,validation_config):
    """
    driver program for ValiData spark job.
    input arguments:
        bootstrap_servers = a list of kafka servers to use for reading streaming data
        datasource = the name of the configured database to use for reading dependencies
        table = the name of the table that data is being streamed from
        validation_config = a list of ValidationRule objects that have been read from file by the main script

    tasks in this function:
        start Spark contexts
        load table dependencies from the source database
        wrap all rules in validation_config into a validator function to be applied to the stream
        apply the validator function to the stream
        get the name of the correct kafka topic for the table being validated
        create direct stream on the topic
        apply rules, and send data to the correct kafka topics
    """

    #start Spark contexts
    sconf = SparkConf()\
            .setMaster("spark://50.112.50.75:7077")\
            .set("spark.executor.cores","4")

    sc = SparkContext(appName="PythonSparkStreamingKafka",conf = sconf)
    sqlc = SQLContext(sc)
    ssc = StreamingContext(sc,config.STREAM_SIZE)
    jdbc_url , jdbc_properties = get_url(datasource)
    write_performance_log("driver","start")
    #create kafka producer in master
    producer = KafkaWriter(bootstrap_servers,datasource,table)


    def get_table_df(table):
        """manages using jdbc to fetch the dependencies"""
        df = sqlc.read.jdbc(
                url = jdbc_url,
                table = table,
                properties = jdbc_properties)
        #df.describe()
        #df.show()
        return df

    #load table dependencies from the source database
    dependencies = {}
    for rule in validation_config:
        dependencies[rule.name] = []
        for d in rule.dependencies:
            name = rule.name
            dependencies[rule.name].append(get_table_df(d))


    global total_valid
    global total_invalid
    total_valid = 0
    total_invalid = 0
    def wrap_validator(rulefuncs):
        """
        turns the rdd into a dataframe
        iterate over the list of rules
        extract the configuration and the function from each
        execute the function on the df
        after iteration, send results to kafka
        """
        def wrapped_rules(time,rdd):
            rowcount = len(rdd.collect())
            write_performance_log("validator","start")
            try:
                stream_df = sqlc.createDataFrame(rdd.map(lambda v:json.loads(v)))
                for arule in rulefuncs:
                    # iterate over the rule functions and update the stream and invalidated dataframes accordingly
                    rulename = arule[0]
                    ruleconfig = arule[1]
                    func = arule[2]
                    ruledependencies = dependencies.get(rulename)

                    write_performance_log("rule {}".format(rulename),"start")
                    new_invalid = func(stream_df,ruleconfig,ruledependencies)
                    write_performance_log("rule {}".format(rulename),"end")

                    #msg = ["\n\nexecuting rule name {}".format(rulename)]
                    #msg += ["func {}".format(func.__name__)]
                    #msg += ["config {}".format(ruleconfig)]
                    #producer.produce_debug("\n".join(msg))

                    try:
                        invalidated = invalidated.union(new_invalid)
                    except UnboundLocalError as e:
                        invalidated = new_invalid
                    joinon = ruleconfig.get("join_cols")
                    stream_df = stream_df.join(invalidated,joinon,"left_anti")

                write_performance_log("write_kafka","start")

                send_valid = stream_df.toJSON().collect()
                for data in send_valid:
                    producer.send_next(record = data, validity = True, rejectionrule = False)

                send_invalid = invalidated.toJSON().collect()
                for data in send_invalid:
                    producer.send_next(record = data, validity = False, rejectionrule = "rejected")
                producer.stat_remnants()

                invalidcount = len(send_invalid)
                validcount = len(send_valid)

                global total_valid
                global total_invalid
                total_valid += validcount
                total_invalid += invalidcount

                write_performance_log("write_kafka , valid {}, invalid {}".format(validcount,invalidcount),"end")
                producer.produce_debug("write_kafka , valid {}, invalid {}, total {}".format(validcount,invalidcount,rowcount))
            except ValueError as e:
                #processing gives an emptyRDD error if the stream producer isn't running
                producer.produce_debug("producer is empty, waiting for data... ")
            except Exception as e:
                producer.produce_debug("unrecovered exception {}".format(e))
                exit()
            finally:
                write_performance_log("validator {}".format(rowcount),"end")
        return wrapped_rules

    #get the name of the correct kafka topic for the table being validated
    topic = get_topic(datasource,table)
    brokerlist = ",".join(bootstrap_servers)

    #create direct stream on the topic
    kafka_properties = {}
    kafka_properties["metadata.broker.list"] = brokerlist
    kafka_properties["auto.offset.reset"] = "smallest"
    kafkaStream = KafkaUtils.createDirectStream(ssc,\
                                                [topic],\
                                                kafka_properties)

    #wrap all rules in validation_config into a validator function to be applied to the stream
    list_of_rules = [(r.name,r.config,validation_functions.get(r.method)) for r in validation_config]
    validator = wrap_validator(list_of_rules)

    data_ds = kafkaStream.map(lambda v:json.loads(v[1]))
    data_ds.foreachRDD(validator)

    #msg = ["\n\n\nabout to start spark StreamingContext.."]
    #msg += ["topic {}".format(topic)]

    #msg += ["list of rules {}".format([x[0] for x in list_of_rules])]
    #msg += ["list of dependencies {}".format(", ".join(list(dependencies.keys())))]
    #producer.produce_debug("\n".join(msg))

    write_performance_log("StreamingContext","start")
    ssc.start()
    ssc.awaitTermination()
    write_performance_log("StreamingContext , total valid {} , total invalid {}".format(total_valid,total_invalid),"end")
