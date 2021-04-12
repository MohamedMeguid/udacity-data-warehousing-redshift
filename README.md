# Data Warehousing with Redshift 

This project is done as part of the udacity, data engineering nanodegree.

This project aims at designing and developing an etl pipeline for a fictional music streaming startup called Sparkify, the pipeline transforms and moves data from Amazon S3 to Amazon Redshift through the following steps:
1. Extract data from S3 stored in json format and stage them in Redshift.
2. Transform the data into a dimensionally modelled set of tables and materialize the transformed data in Redshift.

### Project Structure
1. `sql_queries.py` contains all sql query statements used to drop and create tables, and insert data into tables.
2. `create_tables.py` contains python code to create tables in Redshift, both staging and analytics tables.
3. `etl.py` contains python code that loads data from S3 to the staging tables, extract the data from staging tables, transform them and load them into the analytics tables.

### Installing Dependencies
1. Install conda or anaconda
2. `cd` into the project's directory
3. Run `conda env create -f environment.yml` to install all python packages dependencies 

### Running the Project
1. First, you need to create a `dwh.cfg` file to hold all credentials to the Redshift instance, IAM Roles, S3 files paths. You can use the `dwh-template.cfg` to create that file.
2. Ensure all queries in `sql_queries.py` are properly defined.
3. Run `create_tables.py` through the command: `python create_tables.py`
4. Run `etl.py` using the command: `python etl.py`
