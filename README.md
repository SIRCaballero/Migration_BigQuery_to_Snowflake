Migrating from BigQuery to Snowflake
====================================

![](https://miro.medium.com/v2/resize:fit:1400/1*g263DkEO8J2sD-UIALUSAw.png)


Here we saw some immediate advantages for the migration:

A better web UI - The BigQuery UI doesn't make the best use of screen space, and it felt clunky to use. Snowsight, the Snowflake web UI, has been super easy to use. We rarely need to use the Snowflake CLI or an external IDE.

SQL - Everything can be done using SQL in Snowflake whereas in BigQuery we had needed to use a custom API for a lot of simple things like renaming a table. These workarounds were not as user friendly as Snowflake.

Zero Copy Clone - In Snowflake, you can create copies of your tables, schemas, and databases without replicating the actual data by using cloning. Previously, in BigQuery, we had to move data physically and pay for additional storage and processing.

Caching - Snowflake caches the results at a global level where BigQuery caches these results per-user. BigQuery doesn't allow this global Results Cache, so you'd have to re-run the query from scratch resulting in extra cost for the end user.

RBAC - Snowflake follows the usual RBAC Security model where you create and manage user, roles, and privileges using native SQL. BigQuery uses the IAM model which is designed for the GCP ecosystem. Snowflake RBAC Model is much easier to work with and users can implement much granular level RBAC in Snowflake.

As we worked on this migration, I made note and pulled together some of our key migration observations and learnings to help you along the way. To make it even easier for you, I've included code snippets.

The Plan
========

Our plan was simple and effective. An illustration can be found as follows, but the steps included:

-   Get ANSI SQL DDLs for BigQuery schemas/tables and execute in Snowflake.
-   Export BigQuery tables into GCS bucket as CSV or JSON Files.
-   Execute Copy commands inside Snowflake to copy data from GCS to Snowflake.
-   Migrate Data Pipelines from data sources to Snowflake instead of BigQuery.

![](https://miro.medium.com/v2/resize:fit:1400/1*ofQpmvmeiPcyl39GNmyvsg.png)

Diagram showing our migration plan from BigQuery to Snowflake.

We completed the steps above without any major changes in the BigQuery tables data structure. This approach allowed us to easily compare the data sets between the two systems and run them in parallel to perform UAT before making the official switch.

Migrate DDL
===========

First, create all the schemas in BigQuery as it is in Snowflake. To do that use the BigQuery INFORMATION_SCHEMA to run a simple query like below to find a list of all schemas. Ingest this data into a table inside Snowflake using simple CSV import/export.

select schema_name from INFORMATION_SCHEMA.SCHEMATA

Use the *save results* option in BigQuery to export this data into a simple schemas.csv file and import that CSV into a table inside Snowflake using the Load Table option. Follow simple instructions on this [data load ](https://docs.snowflake.com/en/user-guide/data-load-web-ui.html)page from Snowflake documentation.

After the Schema names are loaded into a Snowflake table (called bq_schema in the sample code), run below proc on top of it to create those schemas in Snowflake and provide some grants to your Snowflake roles if required.

Then call the Proc to create all schemas at once inside Snowflake -

`call create_bq_schemas();`

Next, pull all table's DDL from BigQuery and convert them to Snowflake compatible syntax and run it. Below is the code we used to pull DDL objects from BigQuery and convert them to Snowflake compatible syntax to run.

You may run into a few issues which I outline below. I've provided some tips on how to overcome them.

SQL Compatibility
=================

BigQuery and Snowflake use slightly different variants of SQL syntax. Above code snippet contains some replace commands which should take care of most of these differences. If the above code is not able to convert your BigQuery DDL into Snowflake compatible SQL then you can check [the linked document here from Google](https://cloud.google.com/architecture/dw2bq/snowflake/snowflake-bq-sql-translation-reference.pdf) to understand differences between BigQuery and Snowflake data types, SQL and modify your replace command in the above statement accordingly.

The [Snowflake community site here](https://community.snowflake.com/s/scripts) also has ready-made scripts for preparing your DDL statements from different databases.

Semi Structured Data
====================

STRUCT type in BigQuery only supports explicitly typed values and in Snowflake it's not supported..

ARRAY type in Snowflake can only support VARIANT types, whereas the ARRAY type in BigQuery can support all data types except an array itself which is weird.

To handle this, I think it's easy if we convert STRUCT columns from BigQuery to VARIANT in Snowflake, because in that way it can handle any explicitly typed values and will be able to store any type of data structure into it. Also while running DDL, you have to replace all STRUCT data types to VARIANT in order to make it run directly in Snowflake.

Migrate Data
============

The easiest way to migrate data from Bigquery to snowflake is -

-   Export bigquery tables to GCS.
-   Create Storage integration in Snowflake which will have access to GCS tables.
-   Run Copy commands inside Snowflake to migrate data from GCS.

Before starting data migration from BigQuery to Snowflake, please note that you can not migrate a BigQuery table with STRUCT or ARRAY data types into GCS as a plain CSV file. It has to be a JSON file where order of columns/keys is not guaranteed. Also BigQuery can export up to 1 GB of data to a single file. If you are exporting more than 1 GB of data, you must export your data to [multiple files](https://cloud.google.com/bigquery/docs/exporting-data#exporting_data_into_one_or_more_files) using Single wildcard URI explained [here](https://cloud.google.com/bigquery/docs/exporting-data#exporting_data_into_one_or_more_files).

To keep track of the data migration, I took an export of the below query from BigQuery and created a table BQ_TABLES_COPY_STATUS inside Snowflake with the copy_done flag set as 'N'.

select table_name, table_schema, case when ddl like '%STRUCT%' or ddl like '%ARRAY%' then 'json' else 'csv' end as export_type, 'N' as copy_done\
FROM `bq`.{}.INFORMATION_SCHEMA.TABLES where table_type='BASE TABLE'

Export to GCS
=============

Keeping all above things in mind, I wrote a data migration script to export data into GCS which connects to GCS bucket and exports all tables data into CSV or JSON files based on their export types.

You can follow steps described [here](https://docs.snowflake.com/en/user-guide/data-load-gcs-config.html) to create storage integration inside your snowflake account which will have access to the BigQuery GCS bucket. You may need help from your GCP administrator to create service accounts and IAM Roles to set up storage integration inside Snowflake.

Snowflake COPY command
======================

After we copy data from BigQuery to GCS, the next step is to run copy commands within Snowflake and complete data migration from BigQuery to Snowflake. For this purpose, I wrote a stored procedures inside Snowflake as below. Please note that I'm using ORDINAL_POSITION from Snowflake INFORMATION_SCHEMA.COLUMNS view to arrange the column positions correctly for both CSV and JSON files. For CSV, you don't need to worry about ORDINAL_POSITION, but in case of JSON files the order is not guaranteed hence this is important.

Note that in BigQuery a TIMESTAMP itself does not have a time zone; it represents the same instance in time globally. However, the display of a timestamp for human readability usually includes a date, a time, and a time zone, in an implementation-dependent format.\
Ex. 2020--01--01 00:00:00 UTC and the same will be exported to GCS hence while running copy commands I'm taking a substring for all timestamp columns and making sure that Snowflake default timestamp matches the BigQuery data timestamp.

show parameters like '%TIMEZONE%' in account;#change your snowflake account setup in case you want a different timezonealter account SET TIMEZONE = 'UTC';

Data Comparison
===============

We also performed some simple data comparison checks to make sure that migration completed properly. Some examples are given below:

-   *Select count of each table and compare results with BigQuery.*
-   *Structure comparison of each table.*
-   *Simple check if schema/table exists.*

You can reuse script below to do some of the data comparison checks above:

Data comparison script

Migrate Data Pipelines
======================

This is an ongoing activity and is the most time consuming one. You will basically explore all existing data pipelines that are ingesting data into the existing BigQuery database and will replicate or rewrite all of them to point to the new Snowflake database.
