# Databricks notebook source
ROOT_PATH = 'dbfs:/tmp/dlt20220511'
CDC_ORDER_COLS = ['__$start_lsn', '__$command_id', '__$seqval', '__$operation']

# COMMAND ----------

def cdc_order(df, colname):
    return df.withColumn(colname, F.concat(*CDC_ORDER_COLS).cast('binary')).orderBy(colname)

# COMMAND ----------

def convert_dbfs_prefix(path):
    if path.lower().startswith('dbfs:/'):
        return f'/dbfs/{path[6:]}'
    return path

# COMMAND ----------

def clean_up():
    dbutils.fs.rm(ROOT_PATH, True)
