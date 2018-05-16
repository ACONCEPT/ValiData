import psycopg2
import sys
import os


def create_tables(dbc):
    """ create tables in the PostgreSQL database"""
    commands = (
        """
        DROP TABLE IF EXISTS part_customers;
        DROP TABLE IF EXISTS customers;
        DROP TABLE IF EXISTS part_suppliers;
        DROP TABLE IF EXISTS suppliers;
        DROP TABLE IF EXISTS sales_orders;
        DROP TABLE IF EXISTS purchase_orders;
        DROP TABLE IF EXISTS parts;
        DROP TABLE IF EXISTS sites;
        DROP TABLE IF EXISTS inventory;
        DROP TABLE IF EXISTS inventory_movements;
        DROP TABLE IF EXISTS work_orders;
        """,
        """
        CREATE TABLE IF NOT EXISTS sites (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            location VARCHAR(100) NOT NULL)
        """,
        """
        CREATE TABLE IF NOT EXISTS parts (
                id SERIAL PRIMARY KEY,
                name VARCHAR(200) NOT NULL,
                description VARCHAR(200))
        """,
        """
        CREATE TABLE IF NOT EXISTS sales_orders (
                id SERIAL PRIMARY KEY,
                part_id INTEGER NOT NULL,
                customer_id INTEGER NOT NULL,
                site_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                order_creation_date  TIMESTAMP default NOW(),
                order_expected_delivery TIMESTAMP,
                order_status CHAR(7),
                order_active BOOLEAN default TRUE,
                FOREIGN KEY(site_id)
                REFERENCES sites (id)
                ON UPDATE CASCADE ON DELETE CASCADE)
        """,
        """
        CREATE TABLE IF NOT EXISTS inventory (
                site_id INTEGER NOT NULL,
                part_id INTEGER NOT NULL,
                status VARCHAR(25) NOT NULL,
                quantity INTEGER NOT NULL)
        """,
        """
        CREATE TABLE IF NOT EXISTS inventory_movements (
                source_site_id INTEGER NOT NULL,
                part_id INTEGER NOT NULL,
                source_status VARCHAR(25) NOT NULL,
                target_site_id  INTEGER NOT NULL,
                target_status VARCHAR(25) NOT NULL,
                transaction_time TIMESTAMP)
        """,
        """
        CREATE TABLE IF NOT EXISTS purchase_orders (
                id SERIAL PRIMARY KEY,
                part_id INTEGER NOT NULL,
                supplier_id INTEGER NOT NULL,
                site_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                order_status VARCHAR(10),
                order_expected_receipt TIMESTAMP,
                order_creation_date TIMEStAMP default NOW(),
                FOREIGN KEY(site_id)
                REFERENCES sites (id)
                ON UPDATE CASCADE ON DELETE CASCADE)
        """,
        """
        CREATE TABLE IF NOT EXISTS work_orders(
                id SERIAL PRIMARY KEY,
                part_id INTEGER NOT NULL,
                site_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                work_type VARCHAR(25),
                order_creation_date TIMESTAMP,
                order_expected_date TIMESTAMP,
                order_status VARCHAR(25))

        """,
        """
        CREATE TABLE IF NOT EXISTS suppliers (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                address VARCHAR(100)
                )
        """,
        """
        CREATE TABLE IF NOT EXISTS part_suppliers (
                supplier_id INTEGER NOT NULL,
                part_id INTEGER NOT NULL,
                supply_lead_time INTEGER,
                PRIMARY KEY (supplier_id , part_id),
                FOREIGN KEY (supplier_id)
                    REFERENCES suppliers (id)
                    ON UPDATE CASCADE
                    ON DELETE CASCADE,
                FOREIGN KEY (part_id)
                    REFERENCES parts (id)
                    ON UPDATE CASCADE
                    ON DELETE CASCADE)
        """,
        """
        CREATE TABLE IF NOT EXISTS customers (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                address VARCHAR(100) NOT NULL,
                type char(3) NOT NULL)
        """,
        """
        CREATE TABLE IF NOT EXISTS part_customers (
                customer_id INTEGER NOT NULL,
                part_id  INTEGER NOT NULL,
                delivery_lead_time INTEGER,
                PRIMARY KEY (customer_id, part_id),
                FOREIGN KEY (part_id)
                    REFERENCES parts (id)
                    ON UPDATE CASCADE
                    ON DELETE CASCADE,
                FOREIGN KEY (customer_id)
                    REFERENCES customers (id)
                    ON UPDATE CASCADE
                    ON DELETE CASCADE)
        """)
    conn = None
    try:
        # create table one by one
        print("got connection")
        for command in commands:
            dbc.execute_cursor(command)
            print("executed command")
        # close communication with the PostgreSQL database server
        print("closed the cursor")
        # commit the changes
        dbc.commit_connection()
        print("committed the connection")

    except (Exception) as error:
        print(error)
        raise error
    finally:
        if conn is not None:
            dbc.close_connection()
            print("closed the connection")

if __name__ == '__main__':
    create_tables()

