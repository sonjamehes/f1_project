creating a data databricks project called f1-project that:
- will ingest CSV and JSON files
- will clean the files through Azure Databricks and will output the new processed files in ADLS under the processed container as parquet format. Later I will update this, and replace it, so it will save the data into delta tables instead of parquet.
- will aggregate the files from the processed container through Databricks, and it will output them in ADLS under the presentation folder. Same as I did on the ingestion step, I will later update these notebooks so that they'll read data and write from delta tables
- this solution will handle both full-load and incremental workflow
- will use ADF to create a pipeline that will run automatically and run the ingestion and transformation notebooks through a trigger

creating a mini project, including only two files that:
- will basically do the same thing as the one above, but using unity catalogs
  
