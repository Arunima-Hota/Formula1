`Formula one` project is about ETL processing of its old operations - with incremental load and full load.

## Technologies used
- Databricks - sql, pyspark
- Delta lake
- Data lake (ADLSgen2)
- Azure Key Vault

## Project structure
1. Analysis folder
Analysis is carried out here on dominant formula 1 drivers and teams.

2. Demo folder
  - Creating delta lake demo by writing into external table, managed table and by performing partition.
  - Read operation is performed from file and table.
  - Basic sql operations are performed i.e. creating temp view, join, filter, aggregation.

3. includes folder
  In this folder the configuration and common_functions are attached.

4. ingestion folder
  - 6 csv,json files and 2 folders are ingested here.
  - Inside the files the following operations are performed:
      - Read the data from csv or json files. 
      - Select some required column and renaming some. 
      - If required, added some column or drop a column.       
      - Finally, write data into delta lake.

5. raw folder
   - Created a database called f1_raw here.
   - Inside the database added 8 raw table using sql.

6. set up folder
    - Here the data lake container is mounted to azure data bricks.
    - So each Azure storage account comes with an Access Key, that we can use to       access the storage account.
    - Create a Service Principal and give the required access for the Data Lake to       the Service Principle and use those credentials to access the storage account.
    - Add the secrets to azure key vault. Create databricks secret scope and connect this with azure key vault.

7. trans folder
    Perform some transformation operation i.e. join, filter, aggregation,group by, merge on incremental load (data).

8. utils folder
    Performing incremental load operation here.
 

