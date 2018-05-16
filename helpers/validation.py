import json
import os
import sys

class ValidationRule(object):
    def __init__(self,\
                 name,\
                 table,\
                 column = None,\
                 dependencies = [],\
                 method = None,\
                 config = {},\
                 rejectionrule = "ignore"):
        self.table = table
        self.column = column
        self.name = name
        self.dependencies = dependencies
        self.method = method
        self.config = config
        self.rejectionrule = rejectionrule


    def __repr__(self):
        return "validation rule {}".format(self.name)

    def to_dict(self):
        output = {}
        for attr in dict(self):
            if attr[0] != "_":
                output.update({attr:getattr(self,attr)})
        self.dict = output

    def to_json(self):
        self.to_dict()
        return json.dumps(self.dict)

    @classmethod
    def from_json(cls, json_string):
        try:
            attributes = json.loads(json_string)
        except TypeError as e:
            attributes = json_string
        return cls(**attributes)

if __name__ == "__main__":
    sample = {"name": "check_customer_ids",\
    "table" : "sales_orders",\
    "dependencies" : ["part_customers"],\
    "method" : "check_exists",\
    "config" : {"join_cols":["customer_id", "part_id"]},\
    "rejectionrule" : "rejections"}

    sample2 = {"name" : "check_lead_time",\
    "table" : "sales_orders",\
    "dependencies" : ["part_customers"],\
    "config":{"join_cols":["customer_id", "part_id"],\
              "check_col":"delivery_lead_time",\
              "order_start":"order_creation_date",\
              "order_end":"order_expected_delivery"},\
    "method" : "check_lead_time",\
    "rejectionrule" : "rejections.so_lead_time"}

    ph = os.environ["PROJECT_HOME"]
    filepath = ph + "/config/validations.json"
    output = {}
    output["sales_orders"] = [sample,sample2]
    with open(filepath,"w+") as f:
        f.write(json.dumps(output))
#    test = validation_rules_from_file(filepath)



