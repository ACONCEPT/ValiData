from pyspark import SparkContext, SQLContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from helpers.get_data import get_url
from pyspark.sql.types import DateType
from helpers.kafka import KafkaWriter, get_topic, getjsonproducer
from config import config
from datetime import datetime

#rules are defined in the project-root/config/methods.py file
from config.methods import validation_functions
CYCLES = 0
import json

def getSparkSessionInstance(sparkConf):
    """get a single instance of the spark session """
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

def write_performance_log(**kwargs):
    start = kwargs.pop("start",None)
    end = kwargs.pop("end",None)
    if start and end:
        kwargs["duration"]  = (end - start).total_seconds()

    if kwargs.get("label") == "validator":
        if kwargs.get("duration") < 1:
            return

    result = [datetime.utcnow().isoformat()]
    result += ["{} : {}".format(k,v) for k,v in kwargs.items()]
    print(result)
    result = " | ".join(result)
    with open(config.PERFORMANCE_LOG,"a+") as f:
        f.write("{}\n".format(result))

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
            .set("spark.executor.cores","4")\
            .setExecutorEnv("PYSPARK_DRIVER_PYTHON")
    write_performance_log(driver = "start", STREAM_SIZE = config.STREAM_SIZE, partitions = 2, notes = "kafka in  batches")
    sc = SparkContext(appName="PythonSparkStreamingKafka",conf = sconf)
    sqlc = SQLContext(sc)
    ssc = StreamingContext(sc,config.STREAM_SIZE)
    jdbc_url , jdbc_properties = get_url(datasource)
    #create kafka producer in master
    producer = KafkaWriter(bootstrap_servers,datasource,table)
    bootstrap_servers = config.BOOTSTRAP_SERVERS

    def get_table_df(table):
        """manages using jdbc to fetch the dependencies"""
        df = sqlc.read.jdbc(
                url = jdbc_url,
                table = table,
                properties = jdbc_properties)
        #df.describe()
        #df.show()
        return df

    def send_to_kafka(iterable,topic):
        from kafka import KafkaProducer
        import os
        producer = KafkaProducer(bootstrap_servers=bootstrap_servers,\
                             value_serializer=lambda v: v.encode("utf-8"))

        for row in iterable:
            producer.send(topic,row)
        producer.flush()

    def send_valid(iterable):
        send_to_kafka(iterable,"validated")

    def send_invalid(iterable):
        send_to_kafka(iterable,"invalidated")

    #load table dependencies from the source database
    dependencies = {}
    for rule in validation_config:
        dependencies[rule.name] = []
        for d in rule.dependencies:
            name = rule.name
            dependencies[rule.name].append(get_table_df(d))

    global total_valid
    global total_invalid
    global validator_cycles
    total_valid = 0
    total_invalid = 0
    validator_cycles = 0

    def wrap_validator(rulefuncs,send_kafka_valid, send_kafka_invalid):
        """
        turns the rdd into a dataframe
        iterate over the list of rules
        extract the configuration and the function from each
        execute the function on the df
        after iteration, send results to kafka
        """
        def wrapped_rules(time,rdd):
            global validator_cycles
            rowcount = len(rdd.collect())
            validator_start = datetime.now()
            try:
                stream_df = sqlc.createDataFrame(rdd.map(lambda v:json.loads(v)))
                for arule in rulefuncs:
                    # iterate over the rule functions and update the stream and invalidated dataframes accordingly
                    rulename = arule[0]
                    ruleconfig = arule[1]
                    func = arule[2]
                    ruledependencies = dependencies.get(rulename)

                    rule_start = datetime.now()
                    new_invalid = func(stream_df,ruleconfig,ruledependencies)
                    rule_end = datetime.now()
                    plog = {"label": rulename,"cycle":validator_cycles,"start":rule_start,"end":rule_end}
                    write_performance_log(**plog)

                    subtract_start = datetime.now()
                    try:
                        invalidated = invalidated.union(new_invalid)
                    except UnboundLocalError as e:
                        invalidated = new_invalid

                    joinon = ruleconfig.get("join_cols")
                    stream_df = stream_df.join(invalidated,joinon,"left_anti")
                    subtract_end = datetime.now()

                    plog = {"label": "subtract","cycle":validator_cycles,"start":subtract_start,"end":subtract_end}
                    write_performance_log(**plog)

                kafkawrite_start = datetime.now()

                stream_df.toJSON().foreachPartition(send_kafka_valid)
                invalidated.toJSON().foreachPartition(send_kafka_invalid)

                kafkawrite_end = datetime.now()
                send_invalid = invalidated.toJSON().collect()
                send_valid = stream_df.toJSON().collect()
                invalidcount = len(send_invalid)
                validcount = len(send_valid)

                plog = {"label": "kafkawrite",\
                        "cycle":validator_cycles,\
                        'valid_count':str(validcount),\
                        'invalid_count':str(invalidcount),\
                        "start":kafkawrite_start,\
                        "end":kafkawrite_end}
                write_performance_log(**plog)

                #gather stats for logging... disable in production

                global total_valid
                global total_invalid

                total_valid += validcount
                validator_cycles += 1
                total_invalid += invalidcount
            except ValueError as e:
                #processing gives an emptyRDD error if the stream producer isn't running
                producer.produce_debug("producer is empty, waiting for data... ")
            except Exception as e:
                producer.produce_debug("unrecovered exception {}".format(e))
                exit()
            finally:
                validator_end = datetime.now()
                plog = {"label": "validator",\
                        "cycle":validator_cycles,\
                        "start":validator_start,\
                        "end":validator_end}

                write_performance_log(**plog)
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
    validator = wrap_validator(list_of_rules,send_valid,send_invalid)

    data_ds = kafkaStream.map(lambda v:json.loads(v[1]))
    data_ds.foreachRDD(validator)

    ssc.start()
    ssc.awaitTermination()
