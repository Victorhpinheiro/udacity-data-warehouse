# Cloud Data Warehouse

The project have the objective of creating a data warehouse for the user activity and song metadata located in the S3 in a json format. 

We will have to build an ETL pipeline that extracts the data, stages them in Redshift and transform it into a star schema dimensions and fact tables so it can be analised and get insights from.

# How to run

- First set up the dwh.cfg file. Shouls have the following variables:
```
[CLUSTER]
HOST=''
DB_NAME=''
DB_USER=''
DB_PASSWORD=''
DB_PORT=5439

[IAM_ROLE]
ARN=

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'

[AWS]
KEY=
SECRET=

[DWH]
DWH_CLUSTER_TYPE       = multi-node
DWH_NUM_NODES          = 4
DWH_NODE_TYPE          = dc2.large
DWH_CLUSTER_IDENTIFIER = 
DWH_DB                 = 
DWH_DB_USER            = 
DWH_DB_PASSWORD        = 
DWH_PORT               = 5439
DWH_IAM_ROLE_NAME      = 
```

- Optional run
```sh
cluser.py
```
This will create the redshift and the role for the S3 access. You then need to update the values of ARN role manually as the endpoint.
Or you can create the role and the Redshift cluster manually and add the information in the configuration

- Second, run:
```sh
create_tables.py
```
This will drop all the existing tables (if they exist) and proceed to create new ones empty.

-- Finally, run:
```sh
etl.py
```
This will copy the files from the S3 bucket in the stagging tables and proceed to the transformation of the columns.

---

## Files

- create_table.py is where you'll create your fact and dimension tables for the star schema in Redshift.
- etl.py is where you'll load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.
- sql_queries.py is where you'll define you SQL statements, which will be imported into the two other files above.
- README.md is where you'll provide discussion on your process and decisions for this ETL pipeline.

## Packages required to run
boto3
pandas
psycopg2
configparser
