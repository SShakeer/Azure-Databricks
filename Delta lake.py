# Databricks notebook source
Delta Lake is an open-source storage layer that brings reliability to data lakes by adding a transactional storage layer on top of data stored in cloud storage (on AWS S3, Azure Storage, and GCS). It allows for ACID transactions, data versioning, and rollback capabilities. It allows you to handle both batch and streaming data in a unified way.

Delta tables are built on top of this storage layer and provide a table abstraction, making it easy to work with large-scale structured data using SQL and the DataFrame API.

# COMMAND ----------

# DBTITLE 1,Create a table
All tables created on Azure Databricks use Delta Lake by default.

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists sample100
# MAGIC (
# MAGIC id int,
# MAGIC name string
# MAGIC ) using delta;

# COMMAND ----------

dbutils.fs.ls('user/hive/warehouse')

# COMMAND ----------

# DBTITLE 1,Using Python
# Load the data from its source.
df = spark.read.load("/databricks-datasets/learning-spark-v2/people/people-10m.delta")

# Write the data to a table.
table_name = "people_10m"

df.write.saveAsTable(table_name)

# COMMAND ----------

# DBTITLE 1,Using SQL
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS people_10m;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS people_10m
# MAGIC AS 
# MAGIC SELECT * FROM delta.`/databricks-datasets/learning-spark-v2/people/people-10m.delta`;

# COMMAND ----------

# DBTITLE 1,creating empty table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS people20m (
# MAGIC   id INT,
# MAGIC   firstName STRING,
# MAGIC   middleName STRING,
# MAGIC   lastName STRING,
# MAGIC   gender STRING,
# MAGIC   birthDate TIMESTAMP,
# MAGIC   ssn STRING,
# MAGIC   salary INT
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc history people20m;

# COMMAND ----------

# DBTITLE 1,create empty table using python
# Create table in the metastore
DeltaTable.createIfNotExists(spark) \
  .tableName("default.people10m") \
  .addColumn("id", "INT") \
  .addColumn("firstName", "STRING") \
  .addColumn("middleName", "STRING") \
  .addColumn("lastName", "STRING", comment = "surname") \
  .addColumn("gender", "STRING") \
  .addColumn("birthDate", "TIMESTAMP") \
  .addColumn("ssn", "STRING") \
  .addColumn("salary", "INT") \
  .execute()

# Create or replace table with path and add properties
DeltaTable.createOrReplace(spark) \
  .addColumn("id", "INT") \
  .addColumn("firstName", "STRING") \
  .addColumn("middleName", "STRING") \
  .addColumn("lastName", "STRING", comment = "surname") \
  .addColumn("gender", "STRING") \
  .addColumn("birthDate", "TIMESTAMP") \
  .addColumn("ssn", "STRING") \
  .addColumn("salary", "INT") \
  .property("description", "table with people data") \
  .location("/tmp/delta/people10m") \
  .execute()

# COMMAND ----------

# DBTITLE 1,Inserting
# MAGIC
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW people_updates (
# MAGIC   id, firstName, middleName, lastName, gender, birthDate, ssn, salary
# MAGIC ) AS VALUES
# MAGIC   (9999998, 'Billy', 'Tommie', 'Luppitt', 'M', '1992-09-17T04:00:00.000+0000', '953-38-9452', 55250),
# MAGIC   (9999999, 'Elias', 'Cyril', 'Leadbetter', 'M', '1984-05-22T04:00:00.000+0000', '906-51-2137', 48500),
# MAGIC   (10000000, 'Joshua', 'Chas', 'Broggio', 'M', '1968-07-22T04:00:00.000+0000', '988-61-6247', 90000),
# MAGIC   (20000001, 'John', '', 'Doe', 'M', '1978-01-14T04:00:00.000+000', '345-67-8901', 55500),
# MAGIC   (20000002, 'Mary', '', 'Smith', 'F', '1982-10-29T01:00:00.000+000', '456-78-9012', 98250),
# MAGIC   (20000003, 'Jane', '', 'Doe', 'F', '1981-06-25T04:00:00.000+000', '567-89-0123', 89900);
# MAGIC
# MAGIC MERGE INTO people20m
# MAGIC USING people_updates
# MAGIC ON people20m.id = people_updates.id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into people20m values (20000003, 'Jane', '', 'Doe', 'F', '1981-06-25', '567-89-0123', 89900);

# COMMAND ----------

# DBTITLE 1,Read a table
# MAGIC %python
# MAGIC people_df = spark.read.table(table_name)
# MAGIC
# MAGIC display(people_df)
# MAGIC
# MAGIC ## or
# MAGIC
# MAGIC people_df = spark.read.load(table_path)
# MAGIC
# MAGIC display(people_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM people_10m;
# MAGIC
# MAGIC SELECT * FROM delta.`<path-to-table`;

# COMMAND ----------

# DBTITLE 1,Write to a table
# MAGIC %sql
# MAGIC INSERT INTO people10m SELECT * FROM more_people

# COMMAND ----------

df.write.mode("append").saveAsTable("people10m")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE TABLE people10m SELECT * FROM more_people

# COMMAND ----------

# DBTITLE 1,Update a table SQL Approache
# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC UPDATE people10m SET gender = 'Female' WHERE gender = 'F';
# MAGIC UPDATE people10m SET gender = 'Male' WHERE gender = 'M';
# MAGIC
# MAGIC UPDATE delta.`/tmp/delta/people-10m` SET gender = 'Female' WHERE gender = 'F';
# MAGIC UPDATE delta.`/tmp/delta/people-10m` SET gender = 'Male' WHERE gender = 'M';

# COMMAND ----------

# DBTITLE 1,Update python approache
from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark, '/tmp/delta/people-10m')

# Declare the predicate by using a SQL-formatted string.
deltaTable.update(
  condition = "gender = 'F'",
  set = { "gender": "'Female'" }
)

# Declare the predicate by using Spark SQL functions.
deltaTable.update(
  condition = col('gender') == 'M',
  set = { 'gender': lit('Male') }
)

# COMMAND ----------

# DBTITLE 1,DELETE
DELETE FROM people10m WHERE birthDate < '1955-01-01'

DELETE FROM delta.`/tmp/delta/people-10m` WHERE birthDate < '1955-01-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY people_10m

# COMMAND ----------

# DBTITLE 1,Query an earlier version of the table (time travel)
# MAGIC %sql
# MAGIC SELECT * FROM people20m VERSION AS OF 1;

# COMMAND ----------

df1 = spark.read.format('delta').option('timestampAsOf', '2023-05-29T02:41:14.000+0000').table("people20m")

display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM people20m TIMESTAMP AS OF '2023-05-29T02:41:14.000+0000'

# COMMAND ----------

df2 = spark.read.format('delta').option('versionAsOf', 0).table("people_10m")

display(df2)

# COMMAND ----------

# DBTITLE 1,Clean up snapshots with VACUUM
# MAGIC %sql
# MAGIC VACUUM people_10m

# COMMAND ----------

# DBTITLE 1,Incremental_load
data = [[10, "Najeeb", "company10",1000],
        [20, "Reddy", "company18",2000], 
        [30, "Gani", "company20",2345],
        [40, "Ramdev", "company11",7889], 
        [50, "Chiru", "Oracle",1000],
       [60,"Mani","TCS",2000]]

columns = ['empno', 'ename', 'org','sal']
df = spark.createDataFrame(data, columns)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table emp_target2
# MAGIC (
# MAGIC empno int,
# MAGIC ename string,
# MAGIC org string,
# MAGIC sal int
# MAGIC ) using delta;

# COMMAND ----------

df.createOrReplaceTempView("emp_source")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --select * from emp_source;
# MAGIC
# MAGIC select * from emp_target2;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO emp_target2
# MAGIC USING emp_source
# MAGIC ON emp_source.empno = emp_target2.empno
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET 
# MAGIC emp_target2.empno=emp_source.empno,
# MAGIC emp_target2.ename=emp_source.ename,
# MAGIC emp_target2.org=emp_source.org,
# MAGIC emp_target2.sal=emp_source.sal
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from emp_target2;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO emp_target1
# MAGIC USING emp_source
# MAGIC ON emp_source.empno = emp_target1.empno
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET 
# MAGIC emp_target1.empno=emp_source.empno,
# MAGIC emp_target1.ename=emp_source.ename,
# MAGIC emp_target1.org=emp_source.org,
# MAGIC emp_target1.sal=emp_source.sal
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC Insert into emp_target1 values(emp_source.empno,emp_source.ename,emp_source.org,emp_source.sal);

# COMMAND ----------

select * from emp_target1

# COMMAND ----------

# DBTITLE 1,dataframe approache
# MAGIC %sql
# MAGIC create table emp2_target
# MAGIC (
# MAGIC empno int,
# MAGIC ename string,
# MAGIC org string,
# MAGIC sal int
# MAGIC ) using delta
# MAGIC location "/FileStore/tables/merge_target2"

# COMMAND ----------

data = [[10, "Najeeb", "company10",1000],
        [20, "Reddy", "company18",2000], 
        [30, "Gani", "company20",2345],
        [40, "Ramdev", "company11",7889], 
        [50, "Chiru", "company14",8990],
       [60,"Maheer","Oracle",40200]]

columns = ['empno', 'ename', 'org','sal']
df = spark.createDataFrame(data, columns)

# COMMAND ----------

df.createOrReplaceTempView("emp_source1")

# COMMAND ----------

from delta.tables import *
df_delta=DeltaTable.forPath(spark,"/FileStore/tables/merge_target2")

df_delta.alias("target").merge(
source =df.alias("source"),
  condition="target.empno=source.empno"
).whenMatchedUpdate(set=
                   {
                     "empno":"source.empno",
                     "ename":"source.ename",
                     "org":"source.org",
                     "sal":"source.sal"
                   }).whenNotMatchedInsert(values=
                                          {
                                            "empno":"source.empno",
                                            "ename":"source.ename",
                                            "org":"source.org",
                                            "sal":"source.sal"
                                          }
                                          ).execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from emp2_target
