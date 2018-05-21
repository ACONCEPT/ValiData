import os
BOOTSTRAP_SERVERS = ['50.112.50.75:9092','18.236.86.69:9092']
ZOOKEEPER_SERVERS = ['50.112.50.75:2181']
TESTING_SERVER = ["localhost:9092"]
DATABASE = "postgres_rds"
STREAM_SIZE = 1


VALIDATION_FILE = config_path + "/validations.json"
config_path = os.environ.get("PROJECT_HOME") + "/config/"
JSON_FILE = os.environ.get("PROJECT_HOME") + "/generate_project_data/json_data/records.json"
PERFORMANCE_LOG = os.environ.get("PROJECT_HOME") + "/logs/performance_log.json"
PERFORMANCE_RUNS = os.environ.get("PROJECT_HOME") + "/logs/performance_runs.json"
PERFORMANCE_DATA = os.environ.get("PROJECT_HOME") + "/logs/performance_data.json"
JSON_RECORDS = os.path.dirname(os.environ.get("PROJECT_HOME")) + "/records.json"
