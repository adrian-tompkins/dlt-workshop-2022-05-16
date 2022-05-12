# Databricks notebook source
# MAGIC %run ./lib

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
dbutils.fs.ls(CDC_PRESTAGE_LOCATION)

# COMMAND ----------

df = spark.read.parquet(CDC_PRESTAGE_LOCATION).withColumn('_filename', F.input_file_name())
df = cdc_order(df, '_cdc_order').dropDuplicates(['_filename']).orderBy('_cdc_order').select('_filename')
file_names = [row[0] for row in df.collect()]

# COMMAND ----------

for i in range(0, len(file_names)):
    dbutils.fs.mv(file_names[i], f'{CDC_PRESTAGE_LOCATION}/unload_{i}.parquet')

# COMMAND ----------

dbutils.fs.ls(CDC_PRESTAGE_LOCATION)

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

clean_up()
