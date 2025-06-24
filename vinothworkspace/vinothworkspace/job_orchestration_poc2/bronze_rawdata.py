# Databricks notebook source
import sys
sys.path.append('/Workspace/Users/vinoth.nirmal@gmail.com/job_orchestration_poc2/')
from commonultility import *
from pyspark.sql.functions import when,lit,regexp_replace,col,current_timestamp,input_file_name
from delta.tables import *
from datetime import *
import re
directory_path = spark.conf.get("directory_path_bronze")
logpath = spark.conf.get("logpath")
bronze_flag = "success"

# COMMAND ----------

def datecheck(filename):
    file_flag = False
    dt = datetime.now().date()
    dt_obj = datetime.strptime(str(dt), "%Y-%m-%d")
    dt_formated = dt_obj.strftime("%Y%m%d")
    filename_parts = filename.split('_')
    for part in filename_parts:
        if str(dt_formated) in part:
            file_flag = True
    return file_flag

# COMMAND ----------

def filenamecheck(csv_filelist):
    file_names = csv_filelist
    lst_valid = []
    lst_invalid = []

    # Regular expression pattern to match the format "EmployeeData_YYYYMMDD.csv" or "Parimala_YYYYMMDD.csv"
    pattern = r"EmployeeData_\d\d\d\d\d\d\d\d.csv|Parimala_\d\d\d\d\d\d\d\d.csv|Vivek_\d\d\d\d\d\d\d\d.csv"
    
    # Check if the file name matches the pattern
    for file_name in file_names:
        filename = file_name.name
        file_flag = datecheck(filename)
        if ((re.match(pattern, filename)) and (file_flag)):
            lst_valid.append(file_name)
        else:
            lst_invalid.append(file_name)
    return lst_valid,lst_invalid

# COMMAND ----------

notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/')[-1]
logger = ologger(spark.conf.get("logpath"))
logger.own_logger(f"Execution started : {notebook_name}")

# COMMAND ----------

try:
    logger.own_logger(f"Loading from {directory_path} to 'bronze.employee table'")
    valid_csv_filelist = []
    invalid_csv_filelist = []
    filelist = dbutils.fs.ls(directory_path)
    csv_filelist = [file_path for file_path in filelist if file_path.name.endswith(".csv")]
    valid_csv_filelist, invalid_csv_filelist = filenamecheck(csv_filelist)
    
    if len(invalid_csv_filelist) > 0:
        for i in invalid_csv_filelist:
            source_path = i.path.replace("dbfs:/", "/")
            destination_path = "/FileStore/tables/InvalidFilename/" + i.name
            logger.own_logger(f"{i.name} is a invalid file name and filename should be of format EmployeeData_YYYYMMDD or Parimala_YYYYMMDD or Vivek_YYYYMMDD and it should be current days date.since didnt match the naming convention moved the files to location {destination_path}")
            dbutils.fs.cp(source_path, destination_path, True)
            dbutils.fs.rm(source_path)

    if len(valid_csv_filelist) <= 0:
        bronze_flag = "failed"
        logger.own_logger(f"No files to process in the {directory_path} and exiting the flow with bronze_flag {bronze_flag}")
        dbutils.notebook.exit('Exited')
except Exception as e:
    bronze_flag = "failed"
    print(e)
    logger.own_logger(f"Error while doing filename check: {e} ")
    

# COMMAND ----------

valid_filelist_name = []
if len(valid_csv_filelist) > 0:
    try:
        valid_filelist_name = [valid_csv_file.name for valid_csv_file in valid_csv_filelist]
        logger.own_logger(f"Started loading of valid files : {'; '.join(valid_filelist_name)} to bronze.employee' table")
        dt = DeltaTable.forName(spark, 'bronze.employee')
        df = spark.read.format("csv").option("header", "true").option("sep", ",").load(directory_path)
        df = df.withColumn("FileName", input_file_name()).withColumn("datetimestamp", current_timestamp())
        df.write.format("delta").insertInto('bronze.employee')
        bronze_flag = "success"
    except Exception as e:
        bronze_flag = "failed"
        logger.own_logger(f"Exception loading table bronze.employee : {e}")
       
    finally:
        logger.own_logger(f"'bronze.employee' table loading Ended, flag is set as {bronze_flag}")
else:
    bronze_flag = "failed"
    logger.own_logger(f"No files to process in the {directory_path}, Exiting the flow with bronze_flag {bronze_flag}")


# COMMAND ----------

print(valid_filelist_name)
dbutils.jobs.taskValues.set('valid_csv_filelist',valid_filelist_name)
flag = bronze_flag
dbutils.jobs.taskValues.set('flag',bronze_flag)
logger.own_logger(f"Execution completed with bronze_flag set as {bronze_flag}")
