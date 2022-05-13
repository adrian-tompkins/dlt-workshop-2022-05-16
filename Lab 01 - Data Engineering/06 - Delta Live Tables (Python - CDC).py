# Databricks notebook source
import dlt
import pyspark.sql.functions as F
import json

staged_data_root = spark.conf.get("dltPipeline.stagedDataRoot")

# COMMAND ----------

def get_tables_to_ingest():
    # Here we are using pipeline parameters to set the configuration of tables to ingest;
    # this could be pulled directly from the SQL metastore by
    # connecting to it via JBDC
    #
    # In production, it would be best to scope this pipeline
    # to a collection of tables, rather than all the tables available,
    # eg don't ingest 100 tables at once here; limit it to a logical grouping
    
    # the below expects configurtion to be of the format:
    # dltPipeline.table.[n].(table|identity_cols)
    # eg:
    #
    # dltPipeline.table.0.name my_table_1
    # dltPipeline.table.0.identity_cols id
    # dltPipeline.table.1.name my_table_2
    # dltPipeline.table.1.identity_cols name,revision
    
    # this is pretty fragile code - doesn't handle errors/bad config gracefully
    max_config_items = 1000 # failsafe for max config
    table_config = []
    for i in range(0, max_config_items):
        try:
            name = spark.conf.get(f'dltPipeline.table.{i}.name')
            identity_cols =  spark.conf.get(f'dltPipeline.table.{i}.identityCols').split(',')
        except Exception:
            break
            
        table_config.append({"name": name, "identity_cols": identity_cols})
        
    return table_config

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
    
    cdc_order_columns = ['__$start_lsn', '__$command_id', '__$seqval', '__$operation']

    def add_global_order(df, colname):
        return df.withColumn(colname, F.concat(*cdc_order_columns).cast('binary'))
    
    cdc_columns_pretty = [c.replace('__$', '__') for c in cdc_columns]
    
    # auto loader currently does not support schema inference from parquet
    # this will be coming soon, but for the time being the schema will need to be managed
    # the way this is done below is not a good example of what should be done in production
    # it will get slower as the size of the staging location increases
    # instead, consider infering the schema on a smaller subset of the staging data,
    # or from managing it within the ingestion metadata database
    schema = spark.read.format('parquet').load(f'{staged_data_root}/{table}/*.parquet').schema
    
    @dlt.table(
      name=bronze_name,
      comment=f'raw bronze cdc data for {table}'
    )
    def create_bronze_table():
        return spark.readStream.format('cloudfiles').schema(schema).option('cloudfiles.format', 'parquet').load(f'{staged_data_root}/{table}/*.parquet')
    
    @dlt.view(
        name=pretty_name,
        comment=f'cleaner formatted version of raw cdc table {table}'
    )
    def create_cdc_pretty_view():
        df = add_global_order(dlt.read_stream(bronze_name), '__global_order')
        return (df
             .select(["*"] + [F.col(cdc_col).alias(cdc_col_pretty) for cdc_col, cdc_col_pretty in zip(cdc_columns, cdc_columns_pretty)])
             .drop(*cdc_columns)
             .filter(F.col('__operation') != 3) # ignore values prior to insert - not needed
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
        sequence_by = F.col("__global_order"),
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

for table_config in get_tables_to_ingest():
    # spin up our ingestions in parallel
    generate_tables(table_config['name'], table_config['identity_cols'])
