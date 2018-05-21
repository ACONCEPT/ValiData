# ValiData
## Concept
in enterprise data infrastructure, it is important to remove errors from data as
it is transferred between applications and regions in order to keep the
organization well coordinated.

there are also many benefits to moving from a model of batch daily/weekly data
transfer to continuous, streaming, or event driven ETL. the ValiData platform is
flexible enough to support your data validation needs in batch or continuous
ETL.

## Requirements
in order to run this application, it is necessary to have a pre-installed Apache Spark
cluster as well as an Apache Kafka Cluster

## Configuration  
### configuration steps:
  * adjust the variable for "BOOTSTRAP_SERVERS" in the application config/config.py
  file to match your networked Apache Kafka servers and adjust the script argument

  * setup your database connection string in config/database_connections.py

  * create any methods you would like to use for validation in config/methods.py

  * create validation rule configurations in config/validations.json

  * "master" in "pyspark_submit.sh" in the
  bin directory to match the master ip and port of your Apache Spark cluster. 

### validation rule configuration
the outer key is the table name that the stream should run on. you will set a
reference to this in the config.py file later, and it is the way that the stream
processor knows what topic to read from (in combination with your database name)

Here is an example of a simple json vlalidation rule that would be applied to a
particular tables' stream:
	dependencies ['part_customers']
	name check_customer_ids
	method check_exists
	table sales_orders
	config {'join_cols': ['customer_id', 'part_id']}
	rejectionrule rejections

  * the dependencies key must always be an array, even if there's only one item.

  * the name key is just for identification purposes

  * the method key identifies the function in config/methods.py that will be used to
  run the rule

  * the config is any JSON coniguration that your method will consume as a
  dictionary to know how to apply its method to your specific data

  * rejection rule determines the name of the kafka topic that the rejected data is
  sent to

### database configuration
the database configuration is just a python dictionary. add a key to it with the
name of your new database. the value of this key should be another dictionary
with two keys "type" and "connection details". "type" is just type of database
it is. currently only postgres is supported. "connection_details" is just the
connection string that python would use. This string is used by the producer to
send data to kafka. The JDBC URL that spark uses is automatically parsed using
this data as well

"test_database":{"type":"postgres"
"connection_details" :"host='localhost' port='5432' user='test' dbname='test' password='test'" }

### config.py 
The important variables to set in this file are the following:
  * BOOTSTRAP_SERVERS : set this to a list of the bootstrap servers you want the dstream to read from and send to
  
  * ZOOKEEPER_SERVERS : zookeeper servers for kafka 
  
  * DATABASE : the name of the database configured in the database_connections.py file. according to the above example : "test_database"
  
  * STREAM_SIZE : the number of seconds of data that should be included in the spark microbatch
