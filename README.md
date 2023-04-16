# Project: Build A Cloud Data Warehouse


## Project description

Sparkify is a music streaming startup with a growing user base and song database.

Their user activity and songs metadata data resides in json files in S3. The goal of the current project is to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.


## Project Structure

```
Cloud Data Warehouse
|____create_tables.py       # database/table creation script 
|____etl.py                 # ELT builder
|____sql_queries.py         # SQL query collections
|____dwh.cfg                # AWS configuration file
|____create_cluster.ipynb   # create cluster and IAM rule 
|____test.ipynb             # testing
```

## Create Amazon Redshift cluster
### create_cluster.ipynb


1. `dwh.cfg DWH configuration`
    * The following configuration is the default configuration for the cluster created for this project, you can change it to match your requirements

```
[DWH] 
DWH_CLUSTER_TYPE= multi-node
DWH_NUM_NODES= 4
DWH_NODE_TYPE= dc2.large
DWH_CLUSTER_IDENTIFIER= dwhCluster
DWH_IAM_ROLE_NAME= dwhRole
```
2. `dwh.cfg CLUSTER configuration`
    * The following configuration is the default configuration for the project, and there is no harm of changing them

```
[CLUSTER]
HOST= ------> Fill the host name after creating the cluster from  create_cluster.ipynb STEP 2 section 2.2 <-------
DB_NAME= dwh
DB_USER= awsuser
DB_PASSWORD= Passw0rd
DB_PORT= 5439
```

3. STEP 5 designed to clean up the cluster and the role created in the create_cluster.ipynb



## ELT Pipeline
### etl.py
ELT pipeline builder

1. `load_staging_tables`
	* Load raw data from S3 buckets to Redshift staging tables
2. `insert_tables`
	* Transform staging table data to dimensional tables for data analysis

### create_tables.py
Creating Staging, Fact and Dimension table schema

1. `drop_tables`
2. `create_tables`

### sql_queries.py
SQL query statement collecitons for `create_tables.py` and `etl.py`

1. `*_table_drop`
2. `*_table_create`
3. `staging_*_copy`
3. `*_table_insert`


## Database Schema
### Staging tables
```
staging_events
                artist              VARCHAR,
                auth                VARCHAR,
                firstName           VARCHAR,
                gender              VARCHAR,
                itemInSession       INTEGER,
                lastName            VARCHAR,
                length              FLOAT,
                level               VARCHAR,
                location            VARCHAR,
                method              VARCHAR,
                page                VARCHAR,
                registration        FLOAT,
                sessionId           INTEGER,
                song                VARCHAR,
                status              INTEGER,
                ts                  BIGINT,
                userAgent           VARCHAR,
                userId              INTEGER

staging_songs
                num_songs           INTEGER,
                artist_id           VARCHAR,
                artist_latitude     VARCHAR,
                artist_longitude    VARCHAR,
                artist_location     VARCHAR,
                artist_name         VARCHAR,
                song_id             VARCHAR,
                title               VARCHAR,
                duration            FLOAT,
                year                INTEGER
```

### Fact table
```
songplays
                songplay_id         INT IDENTITY(0,1),
                start_time          TIMESTAMP,
                user_id             INT,
                level               VARCHAR,
                song_id             VARCHAR,
                artist_id           VARCHAR,
                session_id          INT,
                location            TEXT,
                user_agent          TEXT
```

### Dimension tables
```
users
                user_id             INT,
                first_name          VARCHAR,
                last_name           VARCHAR,
                gender              CHAR(1),
                level               VARCHAR

songs
                song_id             VARCHAR,
                title               VARCHAR,
                artist_id           VARCHAR,
                year                INT,
                duration            FLOAT

artists
                artist_id           VARCHAR,
                name                VARCHAR,
                location            TEXT ,
                latitude            FLOAT ,
                longitude           FLOAT

time
                start_time          TIMESTAMP,
                hour                INT,
                day                 INT,
                week                INT,
                month               INT,
                year                INT,
                weekday             VARCHAR
```
