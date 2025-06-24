# Databricks notebook source
#dbutils.fs.rm("/FileStore/tables/silver_Employee/",True)
#dbutils.fs.rm("/FileStore/tables/gold_Employee/",True)
#dbutils.fs.rm("/FileStore/tables/poc_log/",True)
#dbutils.fs.rm("/FileStore/tables/bronze_Employee/",True)
#dbutils.fs.unmount('/mnt/blobstorage/')
#dbutils.fs.ls('/mnt/blobstorage/')
#dbutils.fs.ls('/FileStore/tables/silver_Employee/')

# COMMAND ----------

import sys
sys.path.append('/Workspace/Users/vinoth.nirmal@gmail.com/job_orchestration_poc2/')
from commonultility import *

# COMMAND ----------

notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/')[-1]
logger = ologger(spark.conf.get("logpath"))
logger.own_logger(f"Delta Table and folder Creation started : {notebook_name}")

# COMMAND ----------

directory_paths = ['/FileStore/tables/bronze_Employee/','/FileStore/tables/silver_Employee/','/FileStore/tables/gold_Employee/', '/mnt/blobstorage/',"/FileStore/tables/InvalidFilename/","/FileStore/tables/ValidFilename/","/FileStore/tables/poc_log/","/mnt/blobstorage/InputFiles/","/FileStore/tables/mnt/blobstorage/OutputFiles/","/mnt/blobstorage/OutputFiles/temp/"]
counter = 0

for ipath in directory_paths:
    try:
        # Try to list the contents of the directory
        dbutils.fs.ls(ipath)
        logger.own_logger(f"Directory {ipath} exists")
    except Exception as e:
        if (('mnt' in ipath) & (counter == 0)):
          logger.own_logger(f"Mounting directory {ipath}")
          dbutils.fs.mount(source = "wasbs://dtbrcontainer@dtbrstracct.blob.core.windows.net",
                 mount_point = ipath,
                 extra_configs = {"fs.azure.account.key.dtbrstracct.blob.core.windows.net": "<Access_Token>"})
          counter +=1
        elif (('mnt' in ipath) & (counter > 0)):
          dbutils.fs.mkdirs(ipath)
          logger.own_logger(f"Mount point {ipath} created")
        else:
          dbutils.fs.mkdirs(ipath)
          logger.own_logger(f"Directory {ipath} created")

# COMMAND ----------

# MAGIC %sql
# MAGIC     CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC     CREATE TABLE IF NOT EXISTS bronze.Employee
# MAGIC     (
# MAGIC       id INT NOT NULL,
# MAGIC       Name STRING NOT NULL,
# MAGIC       DOB INT NOT NULL,
# MAGIC       Gender STRING NOT NULL,
# MAGIC       Country STRING,
# MAGIC       Email STRING,
# MAGIC       FileName STRING,
# MAGIC       datetimestamp TIMESTAMP
# MAGIC     ) USING DELTA
# MAGIC     LOCATION '/FileStore/tables/bronze_Employee'

# COMMAND ----------

# MAGIC %sql
# MAGIC     CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC     CREATE TABLE IF NOT EXISTS silver.Employee
# MAGIC     (
# MAGIC       Id INT NOT NULL,
# MAGIC       Name STRING NOT NULL,
# MAGIC       DOB INT NOT NULL,
# MAGIC       Gender STRING NOT NULL,
# MAGIC       Country STRING,
# MAGIC       Email STRING,
# MAGIC       ESKey INT,
# MAGIC       Status STRING,
# MAGIC       StartDate TIMESTAMP,
# MAGIC       EndDate TIMESTAMP,
# MAGIC       InputFileName STRING,
# MAGIC       CreatedDate TIMESTAMP
# MAGIC     ) USING DELTA
# MAGIC     LOCATION '/FileStore/tables/silver_Employee'

# COMMAND ----------

# MAGIC %sql
# MAGIC     CREATE SCHEMA IF NOT EXISTS gold;
# MAGIC     CREATE TABLE IF NOT EXISTS gold.Employee
# MAGIC     (
# MAGIC       Id INT NOT NULL,
# MAGIC       Name STRING NOT NULL,
# MAGIC       DOB INT NOT NULL,
# MAGIC       Gender STRING NOT NULL,
# MAGIC       Country STRING,
# MAGIC       Email STRING,
# MAGIC       Status STRING,
# MAGIC       StartDate TIMESTAMP,
# MAGIC       EndDate TIMESTAMP,
# MAGIC       InputFileName STRING,
# MAGIC       CreatedDate TIMESTAMP
# MAGIC     ) USING DELTA
# MAGIC     LOCATION '/FileStore/tables/gold_Employee/'

# COMMAND ----------

try:
    table_exists_bronze = spark.catalog.tableExists("bronze.Employee")
    table_exists_silver = spark.catalog.tableExists("silver.Employee")
    table_exists_gold = spark.catalog.tableExists("gold.Employee")
    if(table_exists_bronze):
        logger.own_logger(f"'bronze.Employee' table exists")
    if(table_exists_silver):
        logger.own_logger(f"'silver.Employee' table exists")
    if(table_exists_gold):
        logger.own_logger(f"'gold.Employee' table exists")
    for out_path in directory_paths:
        dbutils.fs.ls(out_path)
        flag = "success"
except Exception as e:
    flag = "failed"
    logger.own_logger(f"Exception at DeltaTable creation :{e}")

# COMMAND ----------

dbutils.jobs.taskValues.set("deltatable_flag",flag)
logger.own_logger(f"Delta Table and folder Creation Ended, 'flag' is set as \"{flag}\"")
