# Databricks notebook source
# MAGIC %run ./Fetch-User-Metadata

# COMMAND ----------

spark.sql(f"DROP DATABASE IF EXISTS {DATABASE_NAME} CASCADE")

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")

# COMMAND ----------

spark.sql(f"USE {DATABASE_NAME}")

# COMMAND ----------

# get datasets from git
def get_datasets_from_git():
    import os
    import zipfile
    
    download_location = f'{APJUICE_DATA_ASSET_PATH}/download'
    local_apjuice_asset_path = f"/dbfs/{APJUICE_DATA_ASSET_PATH[len('dbfs:/'):]}"
    local_download_location = f"/dbfs/{download_location[len('dbfs:/'):]}"

    dbutils.fs.cp('https://github.com/adrian-tompkins/dlt-workshop-2022-05-16/raw/main/data/dimensions.zip', f'{download_location}/dimensions.zip')
    dbutils.fs.cp('https://github.com/adrian-tompkins/dlt-workshop-2022-05-16/raw/main/data/sales2021.zip', f'{download_location}/sales2021.zip')
    dbutils.fs.cp('https://github.com/adrian-tompkins/dlt-workshop-2022-05-16/raw/main/data/sales2022.zip', f'{download_location}/sales2022.zip')

    with zipfile.ZipFile(f"{local_download_location}/sales2021.zip","r") as zip_ref:
        zip_ref.extractall(local_apjuice_asset_path)
    with zipfile.ZipFile(f"{local_download_location}/sales2022.zip","r") as zip_ref:
        zip_ref.extractall(local_apjuice_asset_path)
    with zipfile.ZipFile(f"{local_download_location}/dimensions.zip","r") as zip_ref:
        zip_ref.extractall(local_apjuice_asset_path)

# COMMAND ----------

# Return to the caller, passing the variables needed for file paths and database
get_datasets_from_git()

response = APJUICE_DATA_ASSET_PATH + " " + DATABASE_NAME

dbutils.notebook.exit(response)
