# Databricks notebook source
from pyspark.sql.functions import current_date,current_timestamp
from datetime import *
import sys, os
sys.path.append('/Workspace/Users/vinoth.nirmal@gmail.com/')
import math_add_library as ad_nu

# COMMAND ----------

c = ad_nu.add(10,20)
print(c)

# COMMAND ----------

def datetime_calc() :
    df = spark.range(1).select(current_timestamp().alias('datecol')).collect()
    strdate = str(df[0][0])
    print('function : ' + strdate)
    return strdate

try:
    dbutils.widgets.text('datetime',datetime_calc(), 'DateTime :')
    _input_datetime = dbutils.widgets.get('datetime')
    print(_input_datetime)
    if('T' in _input_datetime ):
        date_time_obj = datetime.strptime(_input_datetime,"%Y-%m-%dT%H:%M:%S.%f")
    else:
        date_time_obj = datetime.strptime(_input_datetime,"%Y-%m-%d %H:%M:%S.%f")
    date_part = date_time_obj.strftime("%Y-%m-%d")
    time_part = date_time_obj.strftime("%H:%M")
    print(date_part)
except Exception as e:
    print(f"An error occurred: {str(e)}")

# COMMAND ----------

dbutils.jobs.taskValues.set("input_date",date_part)
dbutils.jobs.taskValues.set("input_time",time_part)
