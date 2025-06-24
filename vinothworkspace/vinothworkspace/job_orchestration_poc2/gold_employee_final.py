# Databricks notebook source
jdbc_url = "jdbc:sqlserver://dtbsserver.database.windows.net:1433;database=dtbsDB;user=admin123@dtbsserver;password=admin@123;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30"

# COMMAND ----------

import sys
sys.path.append('/Workspace/Users/vinoth.nirmal@gmail.com/job_orchestration_poc2/')
from commonultility import *
from delta.tables import *
from datetime import *
from pyspark.sql.functions import when,col,lit,regexp_replace
output_path_dbfs = spark.conf.get("output_path_dbfs")
output_path_temp = spark.conf.get("output_path_temp")
logpath = spark.conf.get("logpath")

# COMMAND ----------

notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/')[-1]
logger = ologger(spark.conf.get("logpath"))

# COMMAND ----------

logger = ologger(logpath)
notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/')[-1]

# COMMAND ----------

#dbutils.fs.rm(output_path_dbfs,True)
# dbutils.fs.ls(output_path_dbfs)
# dbutils.fs.ls(output_path_temp)

# COMMAND ----------

try:
    logger.own_logger(f"Execution for gold layer started : {notebook_name}")
    current_dt = datetime.now().date()
    df_silver_df = DeltaTable.forName(spark, "silver.employee").toDF().filter((col("Status") == "Active") & (col("CreatedDate").contains(current_dt)))
    pattern = r'(?<=..).*(?=@)'
    df_silver_masked_df = df_silver_df.withColumn("Email", regexp_replace(col("Email"), pattern, "xxx"))
    dt_gold = DeltaTable.forName(spark, "gold.employee")
    dt_gold.alias("t").merge(df_silver_masked_df.alias("s"), "t.Id = s.Id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    df_gold = dt_gold.toDF()
    logger.own_logger("Merge to gold.employee completed")

except Exception as e:
    logger.own_logger(f"Error while merging details to gold.employee : {e}")

# COMMAND ----------

try:
    df_gold_emp_vivek = df_gold.filter(((df_gold.InputFileName.like("EmployeeData%")) | (df_gold.InputFileName.like("Vivek%"))) & (col("CreatedDate").contains(current_dt)))
    display(df_gold_emp_vivek)
    df_gold_emp_vivek.coalesce(1).write.format("csv").option('sep', ',').option("header","true").mode('overwrite').save(output_path_temp)
    for dbfs_file in dbutils.fs.ls(output_path_temp):
        if(dbfs_file.name.endswith(".csv")):
            source_location = dbfs_file.path.replace("dbfs:/", "/")
            destination_location = output_path_dbfs + "Emp_Vivek_" + datetime.now().strftime("%Y-%m-%dT%H-%M-%S" + ".csv")
            dbutils.fs.cp(source_location, destination_location)
            logger.own_logger(f"File {dbfs_file.name} moved successfully to {destination_location} for Emp and vivek data")
            dbutils.fs.rm(source_location, True)
            logger.own_logger(f"File removed successfully from {source_location} for Emp and vivek data")
        else:
            dbfs_file = dbfs_file.path.replace('dbfs:/', '/')
            dbutils.fs.rm(dbfs_file)

except Exception as e:
    logger.own_logger(f"Error while moving file to outputlocation for Emp and vivek data {e}")

# COMMAND ----------

try:
    df_gold_parimala = df_gold.filter((df_gold.InputFileName.like("Parimala%")) & (col("CreatedDate").contains(current_dt)))
    df_gold_parimala.write.format("jdbc").option("url",jdbc_url).option("dbtable","Employee").mode("append").save()
    own_logger(dbutils, spark, logpath, f"Loading data to SQL table for Parimala data completed")
except Exception as e:
    logger.own_logger(f"Error while loading data to SQL table for Parimala data : {e}")
finally:
    logger.own_logger(f"Execution completed: {notebook_name}")
