# Databricks notebook source
def clean_up():
    spark.sql(f"drop database if exists {DLT_TARGET_DATABASE} cascade")
    spark.sql(f"drop databse if exists {DATABASE_NAME} cascade")
    spark.sql(f"drop database if exists {f'apjuice_{DATABASE_NAME}'} cascade")
    spark.sql(f"drop database if exists {f'cdc_{DATABASE_NAME}'} cascade")
    dbutils.fs.rm(ROOT_PATH, True)

# COMMAND ----------

clean_up()
