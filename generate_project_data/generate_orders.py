#! usr/bin/env python3
from datetime import datetime,timedelta
import random as rd
from generate_project_data.fake_data import mockData, random_sales_order,random_purchase_order

def daterange(start_date,end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

def generate_orders(**kwargs):
    #instantiate mocker object with database connection
    db = kwargs.get("db","test_database")
    mocker = mockData(db)
    now = kwargs.get("now",datetime.utcnow())

    year = 365

    days_interval = kwargs.get("days_interval",year * 5)
    interval = timedelta(days = days_interval)
    start = now - interval

    sales_orders_per_day = int(kwargs.get("so_per_day",2000))
    purchase_orders_per_day = int(kwargs.get("po_per_day",700))
    first = True

    so_variation = int(kwargs.get("so_variation",500))
    po_variation = int(kwargs.get("po_variation",500))
    for i, day in enumerate(daterange(start,now)):
        #determine daily order amount variation
        sales_orders_today = sales_orders_per_day + rd.randint((0 - so_variation),so_variation)
        purchase_orders_today = purchase_orders_per_day + rd.randint((0 - po_variation),po_variation)

        for o in range(sales_orders_today):
            mocker.current_table = "sales_orders"
            sales_order_record = random_sales_order(mocker.dbc,order_creation_date = day)
            stmt = mocker.generate_insert_statement(**sales_order_record)
            mocker.insert_statements.append(stmt)

        for o in range(purchase_orders_today):
            mocker.current_table = "purchase_orders"
            purchase_order_record = random_purchase_order(mocker.dbc,order_creation_date = day)
            stmt = mocker.generate_insert_statement(**purchase_order_record)
            mocker.insert_statements.append(stmt)

        if i % 10 == 0:
            print ("on day {}".format(day.strftime("%d/%m/%y")))
            if first:
                first = False
            else:
                print( "running {} inserts".format(len(mocker.insert_statements)))
                mocker.run_insert_statements()

    rem = len(mocker.insert_statements)
    if rem > 0:
        print("{} inserts left".format(rem))
        mocker.run_insert_statements()


if __name__ == '__main__':
    pass
