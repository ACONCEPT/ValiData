#! usr/bin/env python3
#from pyspark.sql.functions import date_add

def check_exists(stream_df,ruleconfig,dependencies):
    # getting join cols 
    joinon = ruleconfig.get("join_cols")

    # only using one dependency for this rule
    dependency = dependencies[0]

    # invalid records are definded by a left anti join on the dependency table
    new_invalid = stream_df.join(dependency,joinon,"left_anti")
    return new_invalid

def check_lead_time(stream_df,ruleconfig,dependencies):
    # getting config variables out
    joinon = ruleconfig.get("join_cols")
    check_col = ruleconfig.get("check_col")
    order_start = ruleconfig.get("order_start")
    order_end = ruleconfig.get("order_end")

    # only using one dependency for this method
    dependency = dependencies[0]

    expression = "date_add({},{}) as min_delivery".format(order_start,check_col)
    where = "{} < {}".format("min_delivery",order_end)

    new_invalid = stream_df.join(dependency,joinon,"inner").selectExpr("*",expression).where(where)
    sc = stream_df.columns
    ni_drop = [x for x in new_invalid.columns if x  not in sc]
    new_invalid = new_invalid.drop(*ni_drop)
    return new_invalid

validation_functions = {"check_exists":check_exists ,\
                        "check_lead_time":check_lead_time}
