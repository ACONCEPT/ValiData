import sys
import os


def create_tables(dbc):
    """ create tables in the PostgreSQL database"""
    commands = (
        """
        DROP TABLE IF EXISTS tracked_topics;
        DROP TABLE IF EXISTS stats;
        """,

        """
        CREATE TABLE IF NOT EXISTS tracked_topics(
            topic_name VARCHAR(100) NOT NULL,
            quantity INTEGER NOT NULL,
            timestamp TIMESTAMP DEFAULT NOW())
        """,

        """
        CREATE TABLE IF NOT EXISTS stats (
                id integer,
                topic_name VARCHAR(100) NOT NULL,
                quantity INTEGER NOT NULL,
                PRIMARY KEY (id , topic_name))
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

