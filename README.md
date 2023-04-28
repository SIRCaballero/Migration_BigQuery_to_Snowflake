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

```

create or replace procedure create_bq_schemas()
RETURNS varchar
LANGUAGE JAVASCRIPT
execute as caller
AS
$$
var return_rows = [];
var counter = 0
var SQL_STMT = "select SCHEMA_NAME from bq_schema";
var stmt = snowflake.createStatement({
    sqlText: SQL_STMT
});
var result1 = stmt.execute();
try {
    while (result1.next()) {
        var schema_name = result1.getColumnValue(1);
        var ex = "create schema if not exists bq_db.{sc}";
        ex = ex.replace(/{sc}/g, schema_name);
        var stmt = snowflake.createStatement({
            sqlText: ex
        });
        var r1 = stmt.execute();
        var ex1 = "grant <usage/all> on schema bq_db.{sc} to role bq_db_admin_rl";
        ex1 = ex1.replace(/{sc}/g, schema_name);
        var stmt = snowflake.createStatement({
            sqlText: ex1
        });
        var r2 = stmt.execute();

        counter += 1;
        return_rows.push(counter);
    }
} catch (err) {
    throw err
}
return return_rows;
$$
;

```

Then call the Proc to create all schemas at once inside Snowflake -

`call create_bq_schemas();`

Next, pull all table's DDL from BigQuery and convert them to Snowflake compatible syntax and run it. Below is the code we used to pull DDL objects from BigQuery and convert them to Snowflake compatible syntax to run.

```

def get_execute_ddls():
   creds=service_account.Credentials.from_service_account_file("/Users/stekavade/Desktop/bq_creds.json")
   client = bigquery.Client(credentials=creds)
   query = """
   select schema_name from INFORMATION_SCHEMA.SCHEMATA
   """
   query_job = client.query(query)
   rows = query_job.result()
   for row in rows:
       schema = row.schema_name
       print("Gathering ddl for tables in schema {}".format(schema))
       query = """
       SELECT table_name,replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(ddl,'`',''),'INT64','INT'),'FLOAT64','FLOAT'),'BOOL','BOOLEAN'),'STRUCT','VARIANT'),'PARTITION BY','CLUSTER BY ('),';',');'),'CREATE TABLE ','CREATE TABLE if not exists '), "table INT,", '"table" INT,'),'_"table" INT,','_table INT,'),'ARRAY<STRING>','ARRAY'),'from','"from"'),'_"from"','_from'),'"from"_','from_'),'DATE(_PARTITIONTIME)','date(loaded_at)'),' OPTIONS(',', //'),'));',');'),'_at);','_at));'),'start ','"start" '),'_"start"','_start'),'order ','"order" '),'<',', //'),'_"order"','_order') as ddl
       FROM `bq`.{}.INFORMATION_SCHEMA.TABLES where table_type='BASE TABLE'
       """
       ddl_query = query.format(schema)
       query_job = client.query(ddl_query)
       ddl_set = query_job.result()
       for row in ddl_set:
           table_name = row.table_name
           ddl = row.ddl
           print("Running ddl for table {} in Snowflake".format(table_name))
           use_schema = "use schema {}.{}".format("bq_db",schema)
           with snowflake.connector.connect(
           user='<snowflake_username>',
           password='<snowflake_password>',
           account='<snowflake_prod>',
           warehouse='snwoflake_data_ingest',
           database='bq_db',
           role='bq_ingest_rl'
           )as conn:
               conn.cursor().execute(use_schema)
               conn.cursor().execute(ddl)
           print("Table {} created in bq_db.{} schema".format(table_name,schema))
           
```

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

```

def export_to_gcs():
   client = bigquery.Client()
   bucket_name = 'bq-to-snowflake-transition'
   project = 'bq'
   client = bigquery.Client()
   query = """
   select schema_name from INFORMATION_SCHEMA.SCHEMATA 
   """
   query_job = client.query(query)
   rows = query_job.result()
   for row in rows:
       schema = row.schema_name
       query = """
       select table_name,case when ddl like '%STRUCT%' or ddl like '%ARRAY%' then 'json' else 'csv' end as export_type
       FROM `bq`.{}.INFORMATION_SCHEMA.TABLES where table_type='BASE TABLE'
       """
       ddl_query = query.format(schema)
       query_job = client.query(ddl_query)
       ddl_set = query_job.result()
       for row in ddl_set:
           table_name = row.table_name
           export_type = row.export_type
           print("Exporting data for table {} ...export type is {}".format(table_name,export_type))
           destination_uri = "gs://{}/{}/{}/{}-*.{}".format(bucket_name,schema,table_name,table_name,export_type)
           print(destination_uri)
           dataset_ref = bigquery.DatasetReference(project, schema)
           table_ref = dataset_ref.table(table_name)
           configuration = bigquery.job.ExtractJobConfig()
           configuration.destination_format ='NEWLINE_DELIMITED_JSON'
           if export_type == 'json':
               extract_job = client.extract_table(
                   table_ref,
                   destination_uri,
                   job_config=configuration,
                   location="US",
                   )
           else:
               extract_job = client.extract_table(
                   table_ref,
                   destination_uri,
                   location="US"
                   )
               
           extract_job.result()  # Waits for job to complete.
           print("Exported successfully.. {}:{}.{} to {}".format(project, schema, table_name, destination_uri))

```

You can follow steps described [here](https://docs.snowflake.com/en/user-guide/data-load-gcs-config.html) to create storage integration inside your snowflake account which will have access to the BigQuery GCS bucket. You may need help from your GCP administrator to create service accounts and IAM Roles to set up storage integration inside Snowflake.

Snowflake COPY command
======================

After we copy data from BigQuery to GCS, the next step is to run copy commands within Snowflake and complete data migration from BigQuery to Snowflake. For this purpose, I wrote a stored procedures inside Snowflake as below. Please note that I'm using ORDINAL_POSITION from Snowflake INFORMATION_SCHEMA.COLUMNS view to arrange the column positions correctly for both CSV and JSON files. For CSV, you don't need to worry about ORDINAL_POSITION, but in case of JSON files the order is not guaranteed hence this is important.

```

create or replace procedure copy_data_from_gcs()
RETURNS varchar
LANGUAGE JAVASCRIPT
execute as caller
AS
$$
var return_rows = [];
var counter = 0
var SQL_STMT = "select table_name,table_schema,export_type from BQ_TABLES_COPY_STATUS where copy_done ='N'";
var stmt = snowflake.createStatement({ sqlText: SQL_STMT });
var result1 = stmt.execute();
try {
    while (result1.next()) {
        var table_name = result1.getColumnValue(1);
        var table_schema = result1.getColumnValue(2);
        var export_type = result1.getColumnValue(3);
        if (export_type == 'csv') {

            var SQL_STMT = "select listagg(case when DATA_TYPE = 'TIMESTAMP_NTZ' then 'substring('||concat('$',ORDINAL_POSITION)||',1,23)' \
							else concat('$',ORDINAL_POSITION) end,',') within group (order by ORDINAL_POSITION) as col_list \
							from BQ_DB.INFORMATION_SCHEMA.COLUMNS where lower(TABLE_NAME) = '{t1}' and lower(TABLE_SCHEMA) ='{s1}' order by ORDINAL_POSITION";

            var copy_command = "copy into bq_db.{sc}.{tb} from ( select {col_list} from @bq_stage/{sc}/{tb}/{tb}- )";
          
        } else {

            var SQL_STMT = "select listagg(case when DATA_TYPE = 'TIMESTAMP_NTZ' then 'replace ( $1:' ||lower(column_name)|| ', \\' UTC\\',\\'\\')::'||data_type \
							else concat('$1:',lower(column_name),'::',data_type) end,',') within group (order by ORDINAL_POSITION) as col_list \
							from BQ_DB.INFORMATION_SCHEMA.COLUMNS where lower(TABLE_NAME) = lower('{t1}') and lower(TABLE_SCHEMA) = lower('{s1}') order by ORDINAL_POSITION";

            var copy_command = "copy into bq_db.{sc}.{tb} from ( select {col_list} from @bq_stage/{sc}/{tb}/{tb}- (file_format => FILE_JSON_FORMAT))"
        }
        SQL_STMT = SQL_STMT.replace(/{t1}/g, table_name);
        SQL_STMT = SQL_STMT.replace(/{s1}/g, table_schema);
        copy_command = copy_command.replace(/{sc}/g, table_schema);
        copy_command = copy_command.replace(/{tb}/g, table_name);
        var stmt = snowflake.createStatement({
            sqlText: SQL_STMT
        });
        var result11 = stmt.execute();
        result11.next();
        var col_list = result11.getColumnValue(1);
        if (col_list) {
            copy_command = copy_command.replace(/{col_list}/g, col_list);
            var stmt1 = snowflake.createStatement({ sqlText: copy_command });
            var result12 = stmt1.execute();
        }
        var update_cmd = "Update BQ_TABLES_COPY_STATUS set copy_done ='Y' where table_schema='{sc}' and table_name='{tb}' "
        update_cmd = update_cmd.replace(/{sc}/g, table_schema);
        update_cmd = update_cmd.replace(/{tb}/g, table_name);
        var stmt111 = snowflake.createStatement({ sqlText: update_cmd });
        var result122 = stmt111.execute();
        counter += 1;
        var obj = {}
        obj.cnt = counter;
        obj.table = table_name;
        obj.copy_cmd = copy_command
        return_rows.push(obj);
    }
} catch (err) {
    throw err
}
return JSON.stringify(return_rows);
$$
;

```
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

```

from google.cloud import bigquery
from google.oauth2 import service_account
import snowflake.connector
import pandas as pd
import ast

def google_auth():
    creds=service_account.Credentials.from_service_account_file("./bq_creds.json")
    client = bigquery.Client(credentials=creds)
    print('Bigquery connection successful.')
    return client

def snowflake_auth():
    con_it =  snowflake.connector.connect(
           user='<snowflake_username>',
           password='<snowflake_password>',
           account='<snowflake_prod>',
           warehouse='snwoflake_data_ingest',
           database='bq_db',
           role='bq_ingest_rl'

            )
    cur = con_it.cursor() 
    print('Snowflake connection successful.')
    return cur      

def create_report(df_list, sheet_list, file_name):
    writer = pd.ExcelWriter(file_name,engine='xlsxwriter')   
    for dataframe, sheet in zip(df_list, sheet_list):
        dataframe.to_excel(writer, sheet_name=sheet, startrow=0 , startcol=0)   
    writer.save()

    
def check_schema_exists(bq_cur, sf_cur) -> None:
    bq_sql = '''SELECT upper(schema_name) as schema_name FROM INFORMATION_SCHEMA.SCHEMATA order by schema_name; '''
    bq_schema_df = bq_cur.query(bq_sql).to_dataframe()
    
    sf_sql = '''SELECT schema_name FROM INFORMATION_SCHEMA.SCHEMATA where schema_name != 'INFORMATION_SCHEMA' order by schema_name; '''
    sf_schema_rows = sf_cur.execute(sf_sql)
    sf_schema_df = pd.DataFrame.from_records(iter(sf_schema_rows), columns=[x[0] for x in sf_cur.description])
    
    print('Schema present in bigquery but not in snowflake:')
    bq_schema_present = set(bq_schema_df['schema_name']) - set(sf_schema_df['SCHEMA_NAME'])
    print(bq_schema_present)

    print('Schema present in snowflake but not in bigquery:')
    sf_schema_present = set(sf_schema_df['SCHEMA_NAME']) - set(bq_schema_df['schema_name'])
    print(sf_schema_present)


def check_table_exists(bq_cur, sf_cur) -> pd.DataFrame:
    '''
        Compare the table_name 
    '''
    #Will construct sql command to get list of all tables from different schema in bq project_id.
    bq_table_agg_sql = ''' select  string_agg( concat("select upper(concat(project_id,'.',dataset_id,'.',table_id)) as BQ_TABLE_NAME, date(CAST(TIMESTAMP_MILLIS(last_modified_time) AS DATETIME)) as BQ_LAST_MODIFIED_DATE,row_count as BQ_ROW_COUNT,type from `bq.", schema_name, ".__TABLES__`"), "union all \\n" ) as list_all_table from `bq`.INFORMATION_SCHEMA.SCHEMATA;''' 
    table_agg_list = bq_cur.query(bq_table_agg_sql).to_dataframe()
    
    bq_query = f''' select BQ_TABLE_NAME,BQ_LAST_MODIFIED_DATE,BQ_ROW_COUNT from( {table_agg_list.list_all_table[0]} ) a where type=1;'''
    bq_table_df = bq_cur.query(bq_query).to_dataframe()
    bq_table_df.reset_index(drop=True,inplace=True)
    print('\nResult fetched from Biqquery...')

    sf_query = f'''select 
                    upper("TABLE_CATALOG"||'.'||"TABLE_SCHEMA"||'.'||"TABLE_NAME") as sf_table_name,
                    row_count as sf_row_count,
                    date(last_altered) as sf_last_modified_date
                    from information_schema.tables where table_schema not ilike 'information_schema'
                '''

    sf_table_rows = sf_cur.execute(sf_query)
    sf_table_df = pd.DataFrame.from_records(iter(sf_table_rows), columns=[x[0] for x in sf_cur.description])
    sf_table_df.reset_index(drop=True,inplace=True)
    print('Result fetched from Snowflake...')

    print('\nTables present in bigquery but not in snowflake:')
    print(set(bq_table_df['BQ_TABLE_NAME']) - set(sf_table_df['SF_TABLE_NAME']))

    df_master = pd.merge(bq_table_df,sf_table_df,left_on='BQ_TABLE_NAME',right_on='SF_TABLE_NAME')
    return df_master

    
##replace below date '2022-06-05' with the date on which you exported BigQuery data into GCS
def row_check(df):
    x = df[(df.BQ_LAST_MODIFIED_DATE<=pd.to_datetime('2022-06-05').date()) & (df.BQ_ROW_COUNT!=df.SF_ROW_COUNT)]
    x['Score_diff'] = x['BQ_ROW_COUNT'].sub(x['SF_ROW_COUNT'], axis = 0)
    return x


def check_table_structure(bq_cur, sf_cur) -> pd.DataFrame:
    bq_col_agg_sql = ''' select  string_agg( concat("select * from `bq.", schema_name, ".INFORMATION_SCHEMA.COLUMNS` "), "union all \\n" ) as list_all_col from `bq`.INFORMATION_SCHEMA.SCHEMATA; ''' 
    col_agg_list = bq_cur.query(bq_col_agg_sql).to_dataframe()
    
    bq_query = f''' 
    with cte as(
        select
        table_catalog,table_schema,table_name,column_name,ordinal_position,
        case  when data_type like 'STRUCT%' then 'VARIANT'
            when data_type like 'ARRAY%' then 'ARRAY'
            when data_type like 'INT64' or  data_type like 'NUMERIC' then 'NUMBER'
            when data_type like 'BOOL' then 'BOOLEAN'
            when data_type like 'DATE' then 'DATE'
            when data_type like 'FLOAT64' then 'FLOAT'
            when data_type like 'STRING' then 'TEXT'
            when data_type like 'DATETIME' or data_type like 'TIMESTAMP' then 'TIMESTAMP'
            else data_type end as data_type
        from( {col_agg_list.list_all_col[0]} )a 
    )'''+'''
    select upper(concat(table_catalog,'.',table_schema,'.',table_name)) as bq_table_name,
    count(column_name) as bq_total_columns,
    concat('{',string_agg(concat('"',upper(column_name),'"',":",'"',upper(data_type),'"'),"," order by ordinal_position) ,'}') as bq_all_columns_name,
    sum(case when data_type = 'TEXT' then 1 else 0 end) as bq_text,
    sum(case when data_type = 'FLOAT' then 1 else 0 end) as bq_float,
    sum(case when data_type = 'NUMBER' then 1 else 0 end) as bq_number,
    sum(case when data_type = 'BOOLEAN' then 1 else 0 end) as bq_boolean,
    sum(case when data_type = 'ARRAY' then 1 else 0 end) as bq_array,
    sum(case when data_type = 'DATE' then 1 else 0 end) as bq_date,
    sum(case when data_type = 'TIMESTAMP' then 1 else 0 end) as bq_timestamp,
    sum(case when data_type = 'VARIANT' then 1 else 0 end) as bq_variant 
    from cte
    group by 1;
    '''
    bq_table_structure_df = bq_cur.query(bq_query).to_dataframe()
    bq_table_structure_df.reset_index(drop=True,inplace=True)
    

    sf_query = '''
    select * from (
        select upper(concat(table_catalog,'.',table_schema,'.',table_name)) as sf_table_name,
        concat('{',listagg(
        replace(concat('"',column_name,'"',':','"',data_type,'"'),'_NTZ','')
        ,',')
        within group (ORDER BY ordinal_position),'}')  as sf_all_columns_name,
        count(column_name) as total_columns,
        sum(case when data_type = 'TEXT' then 1 else 0 end) as sf_TEXT,
        sum(case when data_type = 'FLOAT' then 1 else 0 end) as sf_float,
        sum(case when data_type = 'NUMBER' then 1 else 0 end) as sf_NUMBER,
        sum(case when data_type = 'BOOLEAN' then 1 else 0 end) as sf_BOOLEAN,
        sum(case when data_type = 'ARRAY' then 1 else 0 end) as sf_ARRAY,
        sum(case when data_type = 'DATE' then 1 else 0 end) as sf_DATE,
        sum(case when data_type = 'TIMESTAMP_LTZ' or data_type = 'TIMESTAMP_TZ' or data_type = 'TIMESTAMP_NTZ' then 1 else 0 end) as sf_TIMESTAMP,
        sum(case when data_type = 'VARIANT' then 1 else 0 end) as sf_VARIANT
        from information_schema.columns 
        where table_schema not ilike 'information_schema'
        group by 1
        ) a 
    '''

    sf_table_str_rows = sf_cur.execute(sf_query)
    sf_table_str_df = pd.DataFrame.from_records(iter(sf_table_str_rows), columns=[x[0] for x in sf_cur.description])
    sf_table_str_df.reset_index(drop=True,inplace=True)

    df = pd.merge(bq_table_structure_df,sf_table_str_df,left_on='bq_table_name',right_on='SF_TABLE_NAME')
    diff_df = df[(df.bq_total_columns!=df.TOTAL_COLUMNS)]

    Table_name= list()
    Bq_total_col= list()
    Sf_total_col= list()
    SF_Missing_col= list()
    BQ_Missing_col= list()

    for index, row in diff_df.iterrows():
        sf_miss = {k:v for k,v in ast.literal_eval(row["bq_all_columns_name"]).items() if k.upper() not in ast.literal_eval(row["SF_ALL_COLUMNS_NAME"]) and k.lower() not in ast.literal_eval(row["SF_ALL_COLUMNS_NAME"]) and k != '_PARTITIONTIME'}
        bq_miss = {k:v for k,v in ast.literal_eval(row["SF_ALL_COLUMNS_NAME"]).items() if k.upper() not in ast.literal_eval(row["bq_all_columns_name"]) and k.lower() not in ast.literal_eval(row["bq_all_columns_name"])}
        Table_name.append(row['SF_TABLE_NAME'])
        Bq_total_col.append(row['bq_total_columns'])
        Sf_total_col.append(row['TOTAL_COLUMNS'])
        SF_Missing_col.append(str(sf_miss))
        BQ_Missing_col.append(str(bq_miss))
        
    y = pd.DataFrame({'Table_name':Table_name,'Bq_total_col':Bq_total_col,
                    'Sf_total_col':Sf_total_col,'SF_Missing_col':SF_Missing_col, 'BQ_Missing_col':BQ_Missing_col})    
    
    return y    


if __name__ == '__main__':
    gcp_cur = google_auth()
    sf_cur = snowflake_auth()
    # check_schema_exists(gcp_cur,sf_cur)
    df_master = check_table_exists(gcp_cur,sf_cur)
    row_diff_df = row_check(df_master)
    table_str_diff_df = check_table_structure(gcp_cur,sf_cur)
    create_report(df_list=[row_diff_df,table_str_diff_df],sheet_list=['Row_difference_between_BQ_SF','Table_structure_between_BQ_SF'],file_name='./report.xlsx')
    print('Done. Please check the report.xlsx')

```
Data comparison script

Migrate Data Pipelines
======================

This is an ongoing activity and is the most time consuming one. You will basically explore all existing data pipelines that are ingesting data into the existing BigQuery database and will replicate or rewrite all of them to point to the new Snowflake database.
