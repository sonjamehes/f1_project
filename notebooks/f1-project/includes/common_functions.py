# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df = input_df.withColumn('ingestion_date', current_timestamp())
    return output_df

# COMMAND ----------

def re-arrange_partition_column(input_df, part_col):
    '''this function takes a df as an input, a column name.it will rearange the column so that the partition column is on the last position'''
    partition_column = part_col
    col_list = []
    for col in input_df.schema.names:
        if col != partition_column:
            col_list.append(col)
        else:
            continue
    col_list.append(partition_column)

    output_df = input_df.select(col_list)
    return output_df

# COMMAND ----------

def overwrite_partition (input_df, db_name, table_name, part_col):
    '''this function is for incremental loading. if the table doesn't exist, then it will create it and load the historical data. if it exist it will load only the last data'''
   
    output_df = re-arrange_partition_column(input_df, part_col)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        output_df.write.mode('overwrite').insertInto(f'{db_name}.{table_name}')
    else:
        output_df.write.mode('overwrite').partitionBy(part_col).format('parquet').saveAsTable(f'{db_name}.{table_name}')

# COMMAND ----------

final_results_df = final_results_df.select( 'result_id','constructor_id', 'driver_id', 'fastest_lap', 'fastestLapSpeed', 'fastest_lap_time', 'grid', 'laps', 'milliseconds', 'number', 'points', 'position', 'position_order', 'position_Text', 'rank',  'time', 'data_source', 'file_date', 'ingestion_date', 'race_id')