creating a data pipeline that:
- it will ingest CSV and JSON files
- it will clean the files through Azure Databricks and it will output the new processed files in ADLS under the processed container as parquet format
- will aggregate the filnes from the processed container through Databricks, and it will output them in ADLS under the presentation folder
