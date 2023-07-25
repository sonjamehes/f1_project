in this project:
- I will manually add json and CSV files in ADLS under the row-bronze container, in folders representing dates. the first folder (2021-03-21) will represent the cut-off file and will have all the historical data. the other 2 folders (2021-03-28 and 2021-04-18) will be the "new" data. this structure is so we can cover both incremental and full loads
- I will clean the files through Azure Databricks and I'll output the new processed files in ADLS under the processed container as parquet format. Later I will update this, and replace it, so it will save the data into delta tables instead of parquet.
- I will aggregate the files from the processed container through Databricks, and I'll output them in ADLS under the presentation folder. Same as on the ingestion step I will later update it so that it reads the data from delta tables (from the processed-silver container) and writes it into delta tables in the presentation-gold container
- I will use Azure Data Factory to create a pipeline that will execute all the ingestion notebooks from Databricks and then continue with the execution of the transformation notebooks

