#! usr/bin/env python3

source_databases = {"rds_1":"",
"test_database":{"type":"postgres",\
                 "connection_details" :"host='localhost' port='5432' user='test' dbname='test' password='test'" } ,
"postgres_rds":{"type":"postgres",\
                "connection_details" : "host='test-ops-data.cfb6v3hjtond.us-west-2.rds.amazonaws.com'  port='5432' user='joe' dbname='MVPDATA' password='postgrespassword'"
                }}

base_queries = {"select_all": "select {} from {};",
                "select_random":" SELECT {} FROM {} ORDER BY random() LIMIT 1; ",
                "select_type":"select typname from pg_type where oid = {};",
                "select_null":"select * from {} where 1 = 2;",
                "select_tables":"select * from information_schema.tables;",
                "so_lead_time": "select delivery_lead_time from part_customers where part_id = {} and  customer_id = {};" ,
                "po_lead_time": "select supply_lead_time from part_suppliers where part_id = {};" }

