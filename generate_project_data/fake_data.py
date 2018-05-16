#! usr/bin/env python3
from faker import Faker
import json
import string
import random as rd
from postgreslib.database_connection import DBConnection
from psycopg2 import IntegrityError
from datetime import datetime, timedelta
import os

def random_part():
    global fake_factory
    name = product_name()
    description = fake_factory.sentence()
    return {"name":name,"description":description}

def random_customer():
    result = {}
    global fake_factory
    b2b_or_b2c = rd.uniform(0,1)
    if b2b_or_b2c > .2:
        first_name = fake_factory.first_name()
        last_name = fake_factory.last_name()
        result["name"] ="{} {}".format(first_name,last_name)
        result["type"] ="b2c"
    else:
        result["name"] =fake_factory.company()
        result["type"] ="b2b"
    result["address"] = fake_factory.address()
    return result

def random_supplier():
    result = {}
    global fake_factory
    result["name"] = fake_factory.company()
    result["address"] = fake_factory.address()
    return result

def random_sales_order(dbc,**kwargs):
    global fake_factory
    possible_status = ["000-000",\
                       #new open order
                       "111-111",\
                       #confirmed order
                       "222-222",\
                       #completed order
                       "000-222",\
                       #canceled order
                       "222-111"]
                       #partially late order
    result  ={}
    customer_id, part_id = dbc.get_random_value_from_column("part_customers","customer_id, part_id")
    result["part_id"] = part_id
    result["customer_id"] = customer_id
    ocd = kwargs.get("order_creation_date")
    if not ocd:
        ocd = fake_factory.past_date()
    result["order_creation_date"] = ocd
    result["order_status"] = rd.choice(possible_status)
    result["site_id"] = dbc.get_random_value_from_column("sites","id")[0]
    result["quantity"] = rd.randint(0,100)
    if result["order_status"]  not in ["222-222","000-222","222-111"]:
        oed = kwargs.get("order_expected_delivery")
        if not oed:
            lt = dbc.get_customer_lead_time(part_id,customer_id) + rd.randint (-1,10)
            oed = ocd + timedelta(days = lt)
        result["order_expected_delivery"] =  oed
    else:
        result["order_expected_delivery"] = fake_factory.past_date()
    return result

def random_purchase_order(dbc,**kwargs):
    global fake_factory
    possible_status = ["open","closed","partial"]
    result = {}
    supplier_id, part_id = dbc.get_random_value_from_column("part_suppliers","supplier_id, part_id")
    result["part_id"] = part_id
    result["supplier_id"] = supplier_id
    result["order_creation_date"] = fake_factory.past_date()
    ocd = kwargs.get("order_creation_date")
    if not ocd:
        ocd = fake_factory.past_date()
    result["order_status"] = rd.choice(possible_status)
    result["site_id"] = dbc.get_random_value_from_column("sites","id")[0]
    result["quantity"] = rd.randint(0,100)
    if result["order_status"] not in ["closed","partial"]:
        oer = kwargs.get("order_expected_receipt",False)
        if not oer:
            lt = dbc.get_supply_lead_time(part_id) + rd.randint (-1,10)
            oer = ocd + timedelta(days = lt)
        result["order_expected_receipt"] = oer
    else:
        result["order_expected_receipt"] = fake_factory.past_date()
    return result

def get_fake_value(value_type):
    if hasattr(value_type,"__call__"):
        return value_type()
    elif isinstance(value_type,str):
        global fake_factory
        return getattr(fake_factory,value_type)()

def product_name():
    global product_data
    product = rd.choice(product_data)
    return product

def read_product_data(datafile ="json_data/nounlist.txt"):
    datafile = "{}/generate_project_data/json_data/product_data_clean.json".format(os.environ["PROJECT_HOME"])
    with open(datafile,"r") as f:
        data = json.loads(f.read())
    data_header = data.pop(0)
    data = [d["name"].replace("'","") for d in data]
    return data

def random_site():
    global fake_factory
    beginning = "".join([rd.choice(string.ascii_uppercase) for x in range(2)])
    end = "".join([rd.choice(string.digits) for x in range(4)])
    result = beginning + "-" +  end
    loc = fake_factory.city()
    return {"name":result,"location":loc}

class mockData(object):
    def __init__(self,database = "test_database"):
        self.config = {\
                       "sites": (10,random_site),\
                       "parts":(50,random_part),\
                       "customers":(15,random_customer),\
                       "suppliers":(5,random_supplier),\
                       "sales_orders":(50,random_sales_order),\
                       "purchase_orders":(50,random_purchase_order)}
        self.commands = ["sites","parts","customers","sales_orders","purchase_orders","suppliers"]
        print("mockdata got database name {}".format(database))
        self.dbc = DBConnection(database)
        self.current_table = None
        self.insert_statements = []
        global fake_factory
        global product_data
        product_data = read_product_data()
        fake_factory = Faker()

    def set_quantity(self,configitem,amount):
        current_config = self.config.get(configitem)
        new_config = (amount,current_config[1])
        print("updating {}  from {} to {} ".format(configitem,current_config, new_config))
        self.config[configitem] = new_config

    def generate_insert_statement(self,**kwargs):
        base_stmt = "insert into {} ({}) values ({});"
        column_definitions = self.dbc.descriptions.get(self.current_table)
        cols = []
        vals = []
        for column,data in kwargs.items():
            column_type = column_definitions.get(column,False)
            if not column_type:
                print("column type for column {} not found for table {}".format(column,self.current_table) )
            else:
                cols.append(column)
                if "CHAR" in column_type.upper():
                    val = "'{}'".format(data)
                elif "INT" in column_type.upper():
#                    print ("making int {} for column {}".format(data,column))
                    val = "{}".format(str(int(data)))
                elif "TIME" in column_type.upper():
#                    print("got time col {} of type{} ".format(data,type(data)))
                    val = "TIMESTAMP '{}'".format(data.isoformat())
                else:
                    val  = data
                vals.append(val)
        columns = ", ".join(cols)
        values = ", " .join(vals)
        return base_stmt.format(self.current_table,columns, values)

    def part_customer_data(self):
        self.current_table = "part_customers"
        part_list = self.dbc.get_column_from_table("parts","id")
        customer_list =self.dbc.get_column_from_table("customers","id")
        print("making part_customers data for {} customers and {} parts".format(len(customer_list),len(part_list)))

        for customer in customer_list:
            rd.shuffle(part_list)
            parts = rd.randint(0,len(part_list))
            for i in range(parts):
                record = {}
                record["part_id"] = part_list[i]
                record["customer_id"] = customer
                record["delivery_lead_time"] = rd.randint(2,5)
                insert_stmt = self.generate_insert_statement(**record)
                self.insert_statements.append(insert_stmt)
        self.run_insert_statements()

    def part_supplier_data(self):
        print("making part_supplier data")
        self.current_table = "part_suppliers"
        part_list = self.dbc.get_column_from_table("parts","id")
        supplier_list = self.dbc.get_column_from_table("suppliers","id")
        for customer in supplier_list:
            rd.shuffle(part_list)
            parts = rd.randint(0,len(part_list))
            for i in range(parts):
                record = {}
                record["part_id"] = part_list[i]
                record["supplier_id"] = customer
                record["supply_lead_time"] = rd.randint(1,30)
                insert_stmt = self.generate_insert_statement(**record)
                self.insert_statements.append(insert_stmt)
        self.run_insert_statements()

    def generate_starting_inventory(self):
        result = {}
        possible_status = ["available","expired","qa_hold"]
        result["site_id"] = get_random_value_from_column("sites","id")[0]
        result["part_id"] = part_id
        result["status"] = rd.choice(possible_status)
        result["quantity"] = rd.randint(0, 20000)
        insert_stmt = self.generate_insert_statement(**result)
        self.insert_statements.append(insert_stmt)

    def data_generator(self):
        n,func = self.config.get(self.current_table)
        for i in range(n):
            row = func()
            yield self.generate_insert_statement(**row)

    def mock_data_from_config(self):
        self.insert_statements = []
        for command in self.commands:
            self.current_table = command
            table_data_generator = self.data_generator()
            self.insert_statements += [stmt for stmt in table_data_generator ]

    def run_insert_statements(self):
        if self.insert_statements:
            for stmt in self.insert_statements:
                if isinstance(stmt,dict):
                    print(stmt)
                    stmt = self.generate_insert_statement(**stmt)
                self.dbc.execute_cursor(stmt,commit = True)
        self.insert_statements = []


def main():
    mocker = mockData(data_to_mock)
    mocker.commands = ["sites","parts","customers","suppliers"]
    mocker.mock_data_from_config()
    mocker.run_insert_statements()
    mocker.part_customer_data()
    mocker.part_supplier_data()
    mocker.run_insert_statements()
    mocker.commands = ["sales_orders","purchase_orders"]
    mocker.mock_data_from_config()
    mocker.run_insert_statements()

if __name__ == '__main__':
    pass

