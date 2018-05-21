from config import config
import json
print(dir(config))


with open(config.VALIDATION_FILE,"r") as f:
    v = json.loads(f.read())


for k,i in v["sales_orders"][0].items():
    print(k,i)
