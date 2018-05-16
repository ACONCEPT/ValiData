from generate_project_data.fake_data import mockData
from postgreslib.create_tables import create_tables

def generate_master_data(**kwargs):
    print("generate master data kwargs {}".format(kwargs))
    db = kwargs["db"]
    print("db outta kwargs = {}".format(db))
    mocker = mockData(db)
    create_tables(mocker.dbc)
    print("tables created on {}".format(db))
    print("opened mocker")
    mocker.commands = ["sites","parts","customers","suppliers"]
    print("setting quantities")

    siq = kwargs.get("sites",10)
    mocker.set_quantity("sites",siq)

    pq = kwargs.get("parts",50)
    mocker.set_quantity("parts",pq)

    cq = kwargs.get("customers",20)
    mocker.set_quantity("customers",cq)

    suq = kwargs.get("suppliers",15)
    mocker.set_quantity("suppliers",suq)

    print("mocking data from config")
    mocker.mock_data_from_config()

    print("running inserts")
    mocker.run_insert_statements()

    print("generating partcust")
    mocker.part_customer_data()

    print("generating supplier")
    mocker.part_supplier_data()

    print("closing connection")
    mocker.dbc.close_connection()

if __name__ == "__main__":
    pass
