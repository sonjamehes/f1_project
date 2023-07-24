creating a data pipeline that:
- will ingest CSV and JSON files
- will clean the files through Azure Databricks and will output the new processed files in ADLS under the processed container as parquet format
- will aggregate the files from the processed container through Databricks, and it will output them in ADLS under the presentation folder
- will handle both full-load and incremental load
