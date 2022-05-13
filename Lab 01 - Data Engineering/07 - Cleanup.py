# Databricks notebook source
# MAGIC %md
# MAGIC ## Step 1 - Manual Cleanup
# MAGIC Go to DLT pipelines. Stop and delete the pipelines you created in this workshop.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Automated Cleanup
# MAGIC Run the below cells to remove data and tables created by this workshop.

# COMMAND ----------

# MAGIC %run ./Utils/Fetch-User-Metadata

# COMMAND ----------

spark.sql(f"drop database if exists {DATABASE_NAME} cascade")
spark.sql(f"drop database if exists {f'apjuice_{DATABASE_NAME}'} cascade")
spark.sql(f"drop database if exists {f'cdc_{DATABASE_NAME}'} cascade")
dbutils.fs.rm(ROOT_PATH, True)
