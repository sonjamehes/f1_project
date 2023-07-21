# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df = input_df.withColumn('ingestion_date', current_timestamp())
    return output_df

# COMMAND ----------

def re_arrange_partition_column(input_df, part_col):   
    col_list = []
    for col in input_df.schema.names:
        if col != part_col:
            col_list.append(col)
    col_list.append(part_col)
    output_df = input_df.select(col_list)
    return output_df

# COMMAND ----------

def overwrite_partition (input_df, db_name, table_name, part_col): 
    output_df = re_arrange_partition_column(input_df, part_col)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        output_df.write.mode('overwrite').insertInto(f"{db_name}.{table_name}")
    else:
        output_df.write.mode('overwrite').partitionBy(part_col).format('parquet').saveAsTable(f"{db_name}.{table_name}")