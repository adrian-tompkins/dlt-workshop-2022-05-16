# Databricks notebook source
# MAGIC %md
# MAGIC ## Step 1 - Initialise and Download Sample Data
# MAGIC 
# MAGIC Sample data comes from Azure SQL table with CDC enabled.
# MAGIC 
# MAGIC The entire table dump can be found here:
# MAGIC 
# MAGIC https://github.com/adrian-tompkins/dlt-workshop-2022-05-16/raw/main/data/dlt_example.parquet
# MAGIC 
# MAGIC 
# MAGIC And the coresponding cdc dump can be found here:
# MAGIC 
# MAGIC https://github.com/adrian-tompkins/dlt-workshop-2022-05-16/raw/main/data/dlt_example.parquet
# MAGIC 
# MAGIC 
# MAGIC The SQL statements used to build the table and create the data can be found here:
# MAGIC 
# MAGIC **todo**

# COMMAND ----------

# MAGIC %run ./Utils/Fetch-User-Metadata

# COMMAND ----------

DLT_TARGET_DATABASE = f'cdc_{DATABASE_NAME}'
DLT_JOB_NAME = PROJECT_ID

print(ROOT_PATH)
print(DLT_TARGET_DATABASE)
print(DLT_JOB_NAME)


CDC_ORDER_COLS = ['__$start_lsn', '__$command_id', '__$seqval', '__$operation']

def cdc_order(df, colname):
    return df.withColumn(colname, F.concat(*CDC_ORDER_COLS).cast('binary')).orderBy(colname)



# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

DOWNLOAD_LOCATION = f'{ROOT_PATH}/downloads'

# COMMAND ----------

dbutils.fs.cp('https://github.com/adrian-tompkins/dlt-workshop-2022-05-16/raw/main/data/dlt_example.parquet', f'{DOWNLOAD_LOCATION}/dlt_example.parquet')
dbutils.fs.cp('https://github.com/adrian-tompkins/dlt-workshop-2022-05-16/raw/main/data/dlt_example_cdc.parquet', f'{DOWNLOAD_LOCATION}/dlt_example_cdc.parquet')

# COMMAND ----------

display(spark.read.parquet(f'{DOWNLOAD_LOCATION}/dlt_example.parquet'))

# COMMAND ----------

display(spark.read.parquet(f'{DOWNLOAD_LOCATION}/dlt_example_cdc.parquet'))

# COMMAND ----------

CDC_PRESTAGE_LOCATION = f'{ROOT_PATH}/cdc_prestage'
NUM_UNLOAD_FILES = 4

# COMMAND ----------

dbutils.fs.rm(CDC_PRESTAGE_LOCATION, True)
df = cdc_order(spark.read.parquet(f'{DOWNLOAD_LOCATION}/dlt_example_cdc.parquet'), '_cdc_order')
df = df.rdd.sortBy(lambda x: x[-1], numPartitions=NUM_UNLOAD_FILES).toDF(df.schema) # using RDD to force a specific number of files to be writtin with ordering. Normally you should avoid RDDs
df.drop('_cdc_order').write.mode('overwrite').format('parquet').save(CDC_PRESTAGE_LOCATION)
display(dbutils.fs.ls(CDC_PRESTAGE_LOCATION))

# COMMAND ----------

df = spark.read.parquet(CDC_PRESTAGE_LOCATION).withColumn('_filename', F.input_file_name())
df = cdc_order(df, '_cdc_order').dropDuplicates(['_filename']).orderBy('_cdc_order').select('_filename')
file_names = [row[0] for row in df.collect()]

# COMMAND ----------

for i in range(0, len(file_names)):
    dbutils.fs.mv(file_names[i], f'{CDC_PRESTAGE_LOCATION}/unload_{i}.parquet')

# COMMAND ----------

display(dbutils.fs.ls(CDC_PRESTAGE_LOCATION))

# COMMAND ----------

df = spark.read.parquet(CDC_PRESTAGE_LOCATION).withColumn('_filename', F.input_file_name())
df = cdc_order(df, '_cdc_order')
display(df)

# COMMAND ----------

CDC_ORDERED_STAGED_LOCATION = f'{ROOT_PATH}/cdc_staged/ordered_example'
CDC_UNORDERED_STAGED_LOCATION = f'{ROOT_PATH}/cdc_staged/unordered_example'
dbutils.fs.rm(CDC_ORDERED_STAGED_LOCATION, True)
dbutils.fs.rm(CDC_UNORDERED_STAGED_LOCATION, True)
dbutils.fs.mkdirs(CDC_ORDERED_STAGED_LOCATION)
dbutils.fs.mkdirs(CDC_UNORDERED_STAGED_LOCATION)

# COMMAND ----------

dbutils.fs.cp(f'{CDC_PRESTAGE_LOCATION}/unload_0.parquet', CDC_ORDERED_STAGED_LOCATION)
dbutils.fs.cp(f'{CDC_PRESTAGE_LOCATION}/unload_3.parquet', CDC_UNORDERED_STAGED_LOCATION)

# COMMAND ----------

display(dbutils.fs.ls(CDC_ORDERED_STAGED_LOCATION) + dbutils.fs.ls(CDC_UNORDERED_STAGED_LOCATION))

# COMMAND ----------

import json
from pathlib import Path
path = Path(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
notebook_target = str(path.parent.absolute().joinpath('06 - Delta Live Tables (Python - CDC)'))

setup_template = {
    "clusters": [
        {
            "label": "default",
            "num_workers": 1
        }
    ],
    "development": True,
    "continuous": True,
    "edition": "advanced",
    "photon": False,
    "libraries": [
        {
            "notebook": {
                "path": notebook_target
            }
        }
    ],
    "name": DLT_JOB_NAME,
    "storage": f"{ROOT_PATH}/cdc_ingested",
    "configuration": {
        "dltPipeline.stagedDataRoot": f"{ROOT_PATH}/cdc_staged",
        "pipelines.applyChangesPreviewEnabled": "true",
        "dltPipeline.table.0.name": "ordered_example",
        "dltPipeline.table.0.identityCols": "Id",
        "dltPipeline.table.1.name": "unordered_example",
        "dltPipeline.table.1.identityCols": "Id"
    },
    "target": DLT_TARGET_DATABASE
}

print(json.dumps(setup_template, indent=4))

# COMMAND ----------

spark.sql(f"use {DLT_TARGET_DATABASE}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_ordered_example

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_scd_1_ordered_example

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_scd_2_ordered_example

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_unordered_example

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_scd_1_unordered_example

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_scd_2_unordered_example

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_scd_1_unordered_example EXCEPT select * from silver_scd_1_ordered_example
# MAGIC union
# MAGIC select * from silver_scd_1_ordered_example EXCEPT select * from silver_scd_1_unordered_example

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_scd_2_unordered_example EXCEPT select * from silver_scd_2_ordered_example
# MAGIC union
# MAGIC select * from silver_scd_2_ordered_example EXCEPT select * from silver_scd_2_unordered_example

# COMMAND ----------

dbutils.fs.cp(f'{CDC_PRESTAGE_LOCATION}/unload_1.parquet', CDC_ORDERED_STAGED_LOCATION)
dbutils.fs.cp(f'{CDC_PRESTAGE_LOCATION}/unload_2.parquet', CDC_UNORDERED_STAGED_LOCATION)

# COMMAND ----------

dbutils.fs.cp(f'{CDC_PRESTAGE_LOCATION}/unload_2.parquet', CDC_ORDERED_STAGED_LOCATION)
dbutils.fs.cp(f'{CDC_PRESTAGE_LOCATION}/unload_1.parquet', CDC_UNORDERED_STAGED_LOCATION)

# COMMAND ----------

dbutils.fs.cp(f'{CDC_PRESTAGE_LOCATION}/unload_3.parquet', CDC_ORDERED_STAGED_LOCATION)
dbutils.fs.cp(f'{CDC_PRESTAGE_LOCATION}/unload_0.parquet', CDC_UNORDERED_STAGED_LOCATION)
