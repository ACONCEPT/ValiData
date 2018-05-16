#! usr/bin/env python3

import psycopg2
from psycopg2 import IntegrityError
from psycopg2 import ProgrammingError
import json
import sys
import os
from config.database_connections import source_databases, base_queries

class DBConnection(object):
    def __init__(self,source_name):
#        print("dbconnection got source name {}".format(source_name))
#        print("source database keys {} ".format(source_databases))
        datasource = source_databases.get(source_name)
        self.connection_string =  datasource.get("connection_details",None)
        self.connection_type = datasource.get("type",None)
        self.queries = base_queries
        self.open_connection()
        self.get_base_table_descriptions()

    def open_connection(self):
        if self.connection_type == "postgres":
            self.conn = psycopg2.connect(self.connection_string)
        else:
            print("connection type not implemented")

    def close_connection(self):
        self.conn.close()

    def commit_connection(self):
        self.conn.commit()

    def rollback_connection(self):
        self.conn.rollback()

    def get_cursor(self, name = None):
        return self.conn.cursor(name)

    def execute_cursor(self,stmt,commit = False, ignore_errors = False):
        cursor = self.get_cursor()
        cursor.execute(stmt)
        if commit:
            self.commit_connection()
        cursor.close()

    def execute_query(self,query):
        cursor = self.get_cursor("execute_query")
        try:
            cursor.execute(query)
            data = cursor.fetchall()
            description = cursor.description
        except ProgrammingError as e:
            cursor.close()
            self.close_connection()
            self.open_connection()
            raise e
        cursor.close()
        return data, description

    def stream_query(self,query):
#        print("stream query = {}".format(query))
        cursor = self.get_cursor("stream_query")
        cursor.execute(query)
        i = 0
        while True:
            result = cursor.fetchone()
            if result:
                i += 1
                yield result
            else:
                break

    def stream_table(self,table):
        base_query = self.queries.get("select_all")
        query = base_query.format("*",table)
        description = self.get_table_description(table)
#        print("streaming query {} ".format(query))
        generator = self.stream_query(query)
        return generator,description

    def get_base_tables(self):
        query = self.queries.get("select_tables")
        tables_list,description = self.execute_query(query)
        non_pg_tables = [table for table in tables_list if\
                         "pg" not in table[2] \
                         and table[1] != "information_schema"]
        return non_pg_tables

    def get_column_from_table(self,table,column) :
        base_query = self.queries.get("select_all")
        result = self.execute_query(base_query.format(column,table))[0]
        return  [x[0] for x in result]

    def watch_table(self,table):
        query = "select watch_table('{}','changes');"
        result = execute_query(query.format(table))
        return result

    def get_random_value_from_column(self,table,column):
        base_query = self.queries.get("select_random")
        result = self.execute_query(base_query.format(column,table))[0][0]
        return result

    def get_base_table_names(self):
        base_tables = self.get_base_tables()
        return [table[2] for table in base_tables]

    def get_type_name(self,oid):
        base_query = self.queries.get("select_type")
        names, description = self.execute_query(base_query.format(oid))
        return names[0][0]

    def get_base_table_descriptions(self):
        base_table_names = self.get_base_table_names()
        result = {}
        for table in base_table_names:
            description = self.get_table_description(table)
            description = {desc.name: self.get_type_name(desc.type_code) for desc in description}
            result.update({table:description})
        self.descriptions = result
        return result

    def get_table_description(self,table):
        base_query = self.queries.get("select_null")
        query_result,description = self.execute_query(base_query.format(table))
        return description

    def get_customer_lead_time(self,part,customer):
        base_query = self.queries.get("so_lead_time")
        query_result, description = self.execute_query(base_query.format(part,customer))
        return query_result[0][0]

    def get_supply_lead_time(self,part):
        base_query = self.queries.get("po_lead_time")
        query_result, description = self.execute_query(base_query.format(part))
        return query_result[0][0]



if __name__ == '__main__':
    test = DBConnection("test_database")
    test.open_connection()
    generator,desc = test.stream_table("sales_orders")
    import time
    s = time.time()
    count = 0
    for i  in generator:
        pass
    e = time.time()
    speed = 7063000/(e - s)
    print(speed)
    with open("~/speed.txt","w+") as f:
        f.write("{} in {}s, {}r/s".format(703000,(e-s),speed))

