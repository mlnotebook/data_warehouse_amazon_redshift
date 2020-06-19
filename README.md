# Data Warehouse with Amazon Redshift

A project completed with the Udacity Nanodegree Program in Data Engineering. 

## Purpose

Sparkify are a new music streaming app. They've been collecting data on the songs in their catalogue as well as user activity. The data are currently found in a directory of JSON logs on user activity, as well as a directory with JSON metadata on the songs in their app. The analytics team wish to understand what songs users are listening to, but there's currently no straightforward and simply way to query the data in its current form.

## Data
The available date are located in a public S3 bucket:

* Song data: `s3://udacity-dend/song_data`
* Log data: `s3://udacity-dend/log_data`
* Log data json path: `s3://udacity-dend/log_json_path.json`

## Schema
This project creates a PostgreSQL database schema on Amazon Redshift and an ETL pipeline to perform this analysis. The ETL pipeline creates staging tables which hold the data copied in from S3 using the `COPY` query. The data from the staging tables are then read into the fact and dimension tables using the `INSERT` and `SELECT <columns> FROM <staging table>` queries.

The database is a "Star Schema" and is designed to optimize queries on song play analysis. The star schema comprises fact and dimension tables. Dimension tables hold information about different components of the database, _e.g._ users or songs, while the fact table pulls this information together to gather rich information about the data. The Schema is also shown in Figure 1.

**Staging Tables**
* `stagingEvent`
* `stagingSong`

**Fact Table**:
* `factSongplay`

**Dimension Tables**:
* `dimUser`
* `dimSong`
* `dimArtist`
* `dimTime`

![StarSchema](./images/project2_schema_fact_dim.png#center) 
Figure 1: The Star Schema modeled in this project with one fact table and 4 dimension tables.


![StarSchema](./images/project2_schema_staging.png#center)
Figure 2: The Staging Tables used in this project.

## Usage

0. Create a `.cfg` file that contains the variables below including the `KEY` and `SECRET`.

<mark>Note: the `ARN` under `IAM_ROLE` can be copy and pasted after running `create_cluster.py`</mark>

```
[CLUSTER]
CLUSTER_TYPE=multi-node
NODE_TYPE=dc2.large
NUM_NODES=2
CLUSTER_IDENTIFIER=redshift-cluster
DB_NAME=dev
DB_USER=user
DB_PASSWORD=password
DB_PORT=5439
SCHEMA=distSparkify

[IAM_ROLE]
IAM_ROLE_NAME=redshiftIamRole
ARN=arn:aws:iam::XXXX:role/redshiftIamRole

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'

[AWS]
KEY=KEY
SECRET=SECRET
```

1. Create the Redshift Cluster and IAM Role by running `create_cluster.py`.
2. Create the staging, fact and dimension tables by running `create_tables.py`.
3. Populate the fact and dimension tables from the staged data by running `etl.py`.
4. Delete the Redshift Cluster and IAM Role by running `delete_cluster.py`

## Files

### `create_cluster.py`

* This script creates the Redshift cluster, creates the IAM Role and opens the port to the database.
* The IAM Role is given S3 ReadOnly permissions.
* If the cluster or IAM Role already exists, the user is informed.

### `create_tables.py`

* The staging, fact and dimension tables are created on the Redshift Cluster.

### `sql_queries.py`

* Contains each of the 'CREATE', 'COPY', and 'INSERT' queries necessary for staging the data and loading it into the fact and dimension tables.

### `etl.py`

* Each of the `COPY` and `INSERT` queries in `sql_queries.py` are executed on Redshift.
* The data from S3 is copied into the staging tables.
    * ~2s for stagingEvent.
    * ~290s for stagingSong.
* The data from the staging tables is then read into the fact and dimension tables.
    * ~ 6s for factSongplay.
    * <1 s for each dimension table.

### `examples.ipynb`

* While the cluster is up and the data is populated, run the queries in `examples.ipynb`

### `delete_cluster.py`

* The cluster and IAM Role are both removed.