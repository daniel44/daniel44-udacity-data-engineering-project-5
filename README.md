### Sparkify Music Streaming Database & ETL

##### Purpose and goals of the datawarehouse
The datawarehouse will enable Sparkify to analyze the songs and user activity through diverse Data Analysis techniques, with the end goal of understanding user patterns and tailoring the product to meet the market needs.
The datawarehouse will be created in the cloud (AWS) in the Redshift service and will read the data from the data lake in S3

##### Database schema design
The database was designed with a Star schema, which enables high-speed analytic queries at a low resource cost. Since the data falls into S3, it was decided to utilize Amazon redshift as the backend technology. The database design is as follows:

###### Fact table
1. Songplay

###### Dimension tables
1. Songs
2. Artists
3. Users
4. Time

The tables described above will capture the information collected (Please refer to secion Data pipeline for further details) and expose it through Redshift.

##### Data pipeline

###### Source data
The data is sourced is from two different datasets:

1. A set of JSON files that include general information about a song and its corresponding artist. The files are partitioned by the first three letters of each song's track ID. 
2. A set of JSON files that include activity logs from the Sparkify app.

Both datasets are stored in thr cloud in Amazon S3

###### Database creation and constraints

A redshift cluster was created with the tables described above

The following redshift cluster was created:

![redshift_cluster](/cluster.png)

The security group for redshift was utilized and it was open to public IPs:

![redshift_cluster_permissions](/clusterperm.png)

###### ETL pipeline

The data pipeline include the following steps:

1. Begin execution
2. Load the data into staging tables in redshift
3. Load the songplays fact table
4. Load dimention tables
5. Run Data Quality checks

The pipeline is executed using Apache airflow with the following DAG:

![airflow_dag](/dag.png)

The tree view shows all tasks can be completed succesfully:

![airflow_dag_tv](/dagtv.png)

##### Analysis and Data Quality results

Besides the Airflow Data Quality procedures, the following query was conducted in redshift:

#of rows in each table

```sql
select tab.table_schema,
       tab.table_name,
       tinf.tbl_rows as rows
from svv_tables tab
join svv_table_info tinf
          on tab.table_schema = tinf.schema
          and tab.table_name = tinf.table
where tab.table_type = 'BASE TABLE'
      and tab.table_schema not in('pg_catalog','information_schema','pg_internal')
order by tinf.tbl_rows desc;
```

![numberrows](/number_rows.png)
