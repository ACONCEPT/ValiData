from postgreslib.queries import get_supply_lead_time
from postgreslib.postgres_cursor import get_connection,get_cursor,close_cursor,close_connection


if __name__ == '__main__':
    get_connection()
    get_cursor()
    test = get_supply_lead_time(3)
    print(test)
    close_cursor()
    close_connection()
