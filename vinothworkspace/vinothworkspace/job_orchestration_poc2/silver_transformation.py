# Databricks notebook source
import sys
sys.path.append('/Workspace/Users/vinoth.nirmal@gmail.com/job_orchestration_poc2/')
from commonultility import *
from pyspark.sql.functions import when,col,lit,regexp_replace,current_timestamp,input_file_name,split,xxhash64
from delta.tables import *
from datetime import *
from pyspark.sql.types import StringType
import re
directory_path = spark.conf.get("directory_path_bronze")
logpath = spark.conf.get("logpath")
silver_flag = "success"
valid_csv_filelist = dbutils.jobs.taskValues.get(taskKey= "Bronze_rawdata_load",key = "valid_csv_filelist",default=None)
print(valid_csv_filelist)

# COMMAND ----------

notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/')[-1]
logger = ologger(spark.conf.get("logpath"))
logger.own_logger(f"Execution started : {notebook_name}")

# COMMAND ----------

def extract_filename(full_path):
    return full_path.split('/')[-1]

# COMMAND ----------

# MAGIC  %sql
# MAGIC --select * from silver.employee;
# MAGIC --drop table if exists silver.employee;
# MAGIC --delete from silver.employee;

# COMMAND ----------

#dbutils.fs.rm('/FileStore/tables/InvalidFilename/',True)
#dbutils.fs.mkdirs('/FileStore/tables/InvalidFilename/')
#dbutils.fs.rm('/FileStore/tables/mnt/blobstorage/Parimala_20250527.csv')
#dbutils.fs.rm('/FileStore/tables/mnt/blobstorage/EmployeeData_20250527.csv')

# COMMAND ----------

# DBTITLE 1,Reading data from source csv file
logger.own_logger(f"Checking csv file from directory{directory_path} and loaded it as dataframe 'df_csv'")

# Register the UDF
extract_filename_udf = udf(extract_filename, StringType())

df_csv = spark.read.format("csv").option("header","true").option("sep",",").load(directory_path).withColumn("ESKey",lit(0)).withColumn("Status",lit("Active")).withColumn("InputFileName", extract_filename_udf(input_file_name()))

display(df_csv)

# COMMAND ----------

# DBTITLE 1,Creating Deltainstance for Dletatable
dt_employee = DeltaTable.forName(spark,"silver.employee")
df_employee = dt_employee.toDF()
logger.own_logger(f"created delta table instance 'dt_employee' for existing employee details in silver.employee table")

# COMMAND ----------

# display(dt_employee.history())
# df_ver_tt = spark.read.format("delta").option("versionAsOf","2").table("silver.employee")
# display(df_ver_tt)
# dt_employee.restoreToVersion(2)

# COMMAND ----------

# DBTITLE 1,creating outer join between df_csv and dt_employee
try:
  df_all = df_employee.join(df_csv,df_employee.Id == df_csv.ID, "outer").select(df_employee['*'],df_csv.ID.alias('sID').cast('int'),df_csv.Name.alias('sName'),df_csv.DOB.alias('sDOB').cast('int'),df_csv.Gender.alias('sGender'),df_csv.Country.alias('sCountry'),df_csv.Email.alias('sEmail'),df_csv.ESKey.alias('sESKey'),df_csv.Status.alias('sStatus'),df_csv.InputFileName.alias('sInputFileName'))

  #display(df_all)
  
  df_all_ps = df_all.withColumn("pStatus",when((df_all.Id.isNull() & df_all.sID.isNotNull()),lit("New")).when(df_all.Id.isNotNull() & df_all.sID.isNotNull() & (xxhash64(df_all.Id,df_all.Name,df_all.DOB,df_all.Gender,df_all.Country,df_all.Email,df_all.ESKey,df_all.Status,df_all.InputFileName)  != xxhash64(df_all.sID,df_all.sName,df_all.sDOB,df_all.sGender,df_all.sCountry,df_all.sEmail,df_all.sESKey,df_all.sStatus,df_all.sInputFileName)) ,lit("Update")).otherwise("Exist"))

  logger.own_logger("Joined both df_csv and dt_employee using outer join and with new column pStatus")
  
except Exception as e:
    silver_flag = "failed"
    logger.own_logger(f"Error while creating join between deltatable and dataframe of csv file : {e}")

# COMMAND ----------

# DBTITLE 1,Working on New rows
try:
  logger.own_logger("Checking for new row insertion in to 'silver_employee'")
  df_all_ps_New = df_all_ps.filter(df_all_ps.pStatus == "New").select(df_all_ps.sID,df_all_ps.sName,df_all_ps.sDOB,df_all_ps.sGender,df_all_ps.sCountry,df_all_ps.sEmail,df_all_ps.sESKey,df_all_ps.sStatus,df_all_ps.sInputFileName).withColumns({"StartDate":current_timestamp(),"Enddate":lit("9999-12-31 00:00:00"),"CreatedDate":current_timestamp()})

  df_all_ps_New = df_all_ps_New.select(df_all_ps_New.sID,df_all_ps_New.sName,df_all_ps_New.sDOB,df_all_ps_New.sGender,df_all_ps_New.sCountry,df_all_ps_New.sEmail,df_all_ps_New.sESKey,df_all_ps_New.sStatus,df_all_ps_New.StartDate,df_all_ps_New.Enddate,df_all_ps_New.sInputFileName,df_all_ps_New.CreatedDate)

  #display(df_all_ps_New)

  if(df_all_ps_New.count() > 0):
    df_all_ps_New.write.format("delta").mode("append").insertInto("silver.employee")
    logger.own_logger("New rows inserted in to 'silver_employee'")
  else:
    logger.own_logger("No new rows to be inserted")
  
#display(dt_employee.toDF())
except Exception as e:
    silver_flag = "failed"
    logger.own_logger(f"Error while inserting new rowsin to 'silver.employee' table {e}")

# COMMAND ----------

try:
    df_all_ps_Update = df_all_ps.filter(df_all_ps.pStatus == "Update").select(
        df_all_ps.sID, df_all_ps.sName, df_all_ps.sDOB, df_all_ps.sGender, df_all_ps.sCountry, df_all_ps.sEmail, df_all_ps.sESKey, df_all_ps.sStatus, df_all_ps.sInputFileName
    ).dropDuplicates()

    if df_all_ps_Update.count() > 0:
        logger.own_logger("Found updates for existing employees in 'silver.employee table'")

        df_newversion = df_employee.join(df_all_ps_Update, df_employee.Id == df_all_ps_Update.sID, "left")
        df_newversion = df_newversion.withColumn("ESKey_1", when((df_newversion.ESKey == df_newversion.sESKey), lit(1)).when((df_newversion.ESKey != df_newversion.sESKey), lit(2)))
        #display(df_newversion)
        df_newversion = df_newversion.filter(df_newversion.ESKey_1 == 1).select(
            df_newversion.Id, df_newversion.Name, df_newversion.DOB, df_newversion.Gender, df_newversion.Country, df_newversion.Email, df_newversion.ESKey_1, df_newversion.Status, df_newversion.InputFileName
        ).withColumn("Status", when(col("ESKey_1") == 1, lit('InActive')).otherwise(df_newversion.Status))
        
        df_updated_rows = df_all_ps_Update.unionAll(df_newversion)

        df_updated_rows = df_employee.join(
            df_updated_rows, 
            (df_employee.Id == df_updated_rows.sID) & (df_employee.Status == df_updated_rows.sStatus), 
            "right"
        ).select(
            df_updated_rows.sID, df_updated_rows.sName, df_updated_rows.sDOB, df_updated_rows.sGender, 
            df_updated_rows.sCountry, df_updated_rows.sEmail, df_updated_rows.sESKey, df_updated_rows.sStatus, 
            df_employee.StartDate.alias('sStartDate'), df_employee.EndDate.alias('sEnddate'), df_employee.CreatedDate.alias('sCreatedDate'), df_updated_rows.sInputFileName
        )

        #display(df_updated_rows)

        df_updated_rows_null_startdate = df_updated_rows.filter(df_updated_rows.sStartDate.isNull())

        df_updated_rows_notnull_startdate = df_updated_rows.filter(df_updated_rows.sStartDate.isNotNull()).select(
            df_updated_rows.sID.alias('ssID'), 
            df_updated_rows.sName.alias('ssName'), 
            df_updated_rows.sDOB.alias('ssDOB'), 
            df_updated_rows.sGender.alias('ssGender'), 
            df_updated_rows.sCountry.alias('ssCountry'), 
            df_updated_rows.sEmail.alias('ssEmail'), 
            df_updated_rows.sESKey.alias('ssESKey'), 
            df_updated_rows.sStatus.alias('ssStatus'),
            df_updated_rows.sStartDate.alias('ssStartDate'),
            df_updated_rows.sEnddate.alias('ssEnddate'),
            df_updated_rows.sCreatedDate.alias('ssCreatedDate'),
            df_updated_rows.sInputFileName.alias('ssInputFileName')
        )

        df_updated_rows_startdate_full = df_updated_rows_notnull_startdate.join(
            df_updated_rows_null_startdate,
            df_updated_rows_notnull_startdate.ssID == df_updated_rows_null_startdate.sID,
            "inner"
        ).select(
            df_updated_rows_null_startdate.sID, 
            df_updated_rows_null_startdate.sName, 
            df_updated_rows_null_startdate.sDOB, 
            df_updated_rows_null_startdate.sGender, 
            df_updated_rows_null_startdate.sCountry, 
            df_updated_rows_null_startdate.sEmail, 
            df_updated_rows_null_startdate.sESKey, 
            df_updated_rows_null_startdate.sStatus,
            df_updated_rows_notnull_startdate.ssStartDate,
            df_updated_rows_null_startdate.sEnddate,
            df_updated_rows_null_startdate.sCreatedDate,
            df_updated_rows_null_startdate.sInputFileName
        )

        df_updated_rows = df_updated_rows.unionAll(df_updated_rows_startdate_full)
        df_updated_rows = df_updated_rows.filter(df_updated_rows.sStartDate.isNotNull())

        logger.own_logger("Merging started for 'silver.employee table'")

        dt_employee.alias("T").merge(
            df_updated_rows.alias("S"),
            "T.Id = S.sID AND T.Status = S.sStatus"
        ).whenMatchedUpdate(
            set={
                "Name": col("S.sName"),
                "DOB": col("S.sDOB"),
                "Gender": col("S.sGender"),
                "Country": col("S.sCountry"),
                "Email": col("S.sEmail"),
                "ESKey": col("S.sESKey"),
                "Status": col("S.sStatus"),
                "StartDate": col("S.sStartDate"),
                "Enddate": col("S.sEnddate"),
                "InputFileName": col("S.sInputFileName"),
                "CreatedDate": current_timestamp()
            }
        ).whenNotMatchedInsert(
            values={
                "Id": col("S.sID"),
                "Name": col("S.sName"),
                "DOB": col("S.sDOB"),
                "Gender": col("S.sGender"),
                "Country": col("S.sCountry"),
                "Email": col("S.sEmail"),
                "ESKey": col("S.sESKey"),
                "Status": col("S.sStatus"),
                "StartDate": col("S.sStartDate"),
                "Enddate": current_timestamp(),
                "InputFileName": col("S.sInputFileName"),
                "CreatedDate": current_timestamp()
            }
        ).execute()
        silver_flag = "success"
        logger.own_logger("Merging completed for 'silver.employee table'")
    else:
        logger.own_logger("No data to merge into 'silver.employee table'")

except Exception as e:
    silver_flag = "failed"
    logger.own_logger(f"Error while merging {e}")

# COMMAND ----------

try:
  if silver_flag == "success":
    archive = "/FileStore/tables/ValidFilename/"
    for valid_csv_file in valid_csv_filelist:
      source_path = spark.conf.get("directory_path_bronze") + valid_csv_file
      #print(f"source_path : {source_path}")
      destination_path = archive + valid_csv_file
      #print(f"destination_path : {destination_path}")
      dbutils.fs.cp(source_path,destination_path,True)
      dbutils.fs.rm(source_path)
      logger.own_logger(f"Execution completed moving the valid file {valid_csv_file} to {destination_path}")
except Exception as e:
  silver_flag = "failed"
  print(e)
  logger.own_logger(f"Error while file from {directory_path} to {archive}, Exception : {e}")

# COMMAND ----------

logger.own_logger(f"Execution completed for silver layer silver_flag is set as {silver_flag}")
dbutils.jobs.taskValues.set('silver_flag',silver_flag)
