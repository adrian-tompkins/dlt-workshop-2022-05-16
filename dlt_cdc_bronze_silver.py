# Databricks notebook source
import dlt
import pyspark.sql.functions as F
staged_data_root = {{STAGED_DATA_ROOT}}

# COMMAND ----------

def get_tables_to_ingest():
    # this could be pulled directly from the SQL metastore by
    # connecting to it via JBDC, or it could be passed in
    # as parameters when the pipeline is created.
    # In production, it would be best to scope this pipeline
    # to a collection of tables, rather than all the tables available,
    # eg don't ingest 100 tables at once here; limit it to a logical grouping
    return [['name_location_cdc', ['id']]]

# COMMAND ----------

def generate_tables(table, keys):
    # this is the function for dynamically generating tables,
    # using the table metadata as the parameters.
    # in a more traditional silver/gold tables where you define
    # the sql for the table, you wouldn't need to do this
    
    bronze_name = f'bronze_{table}'
    pretty_name = f'cdc_pretty_view_{table}'
    silver_name = f'silver_{table}'
    silver_scd_1_name = f'silver_scd_1_{table}'
    silver_scd_2_name = f'silver_scd_2_{table}'
    
    cdc_columns = [
        '__$start_lsn',
        '__$end_lsn',
        '__$seqval',
        '__$operation',
        '__$update_mask',
        '__$command_id'
    ]
    
    cdc_columns_pretty = [c.replace('__$', '__') for c in cdc_columns]
    
    # auto loader currently does not support schema inference from parquet
    # this will be coming soon, but for the time being the schema will need to be managed
    # the way this is done below is not a good example of what should be done in production
    # it will get slower as the size of the staging location increases
    # instead, consider infering the schema on a smaller subset of the staging data,
    # or from managing it within the ingestion metadata database
    schema = spark.read.format('parquet').load(f'{staging_path_prefix}/{table}/*').schema
    
    @dlt.table(
      name=bronze_name,
      comment=f'raw bronze cdc data for {table}'
    )
    def create_bronze_table():
        return spark.readStream.format('cloudfiles').schema(schema).option('cloudfiles.format', 'parquet').load(f'{staging_path_prefix}/{table}/*')
    
    @dlt.view(
        name=pretty_name,
        comment=f'cleaner formatted version of raw cdc table {table}'
    )
    def create_cdc_pretty_view():
        return (dlt
             .read_stream(bronze_name)
             .select(["*"] + [F.col(cdc_col).alias(cdc_col_pretty) for cdc_col, cdc_col_pretty in zip(cdc_columns, cdc_columns_pretty)])
             .drop(*cdc_columns)
             .filter(F.col('__operation') != 3)
             .withColumn("__operation", F.when(F.col('__operation') == 1, 'DELETE')
                                         .when(F.col('__operation') == 2, 'INSERT')
                                         .when(F.col('__operation') == 4, 'UPDATE')
                                         .otherwise(F.lit(None))
             )
        )
        
    @dlt.table(
        name=f'silver_{table}',
        comment=f'Cleaner formatted version of raw cdc table {table}, maintaining full history'
    )
    def create_silver_table():
        return dlt.read_stream(pretty_name)
    
    dlt.create_target_table(
        name=silver_scd_1_name,
        comment=f'SCD type 1 version of cdc table {table}'
    )
    
    dlt.apply_changes(
        target = silver_scd_1_name,
        source = pretty_name,
        keys = keys,
        sequence_by = F.col("__seqval"), #TODO: confirm with MSFT that seqval is the only column needed for ordering
        apply_as_deletes = F.col('__operation') == 'DELETE',
        except_column_list = cdc_columns_pretty
    )
    
    dlt.create_target_table(
        name=silver_scd_2_name,
        comment=f'SCD type 2 version of cdc table {table}'
    )
    
    dlt.apply_changes(
        target = silver_scd_2_name,
        source = pretty_name,
        keys = keys,
        sequence_by = F.col("__seqval"), #TODO: confirm with MSFT that seqval is the only column needed for ordering
        apply_as_deletes = F.col('__operation') == 'DELETE',
        except_column_list = cdc_columns_pretty,
        stored_as_scd_type = "2"
    )

# COMMAND ----------

for table, keys in get_tables_to_ingest():
    # spin up our ingestions in parallel
    generate_tables(table, keys)
