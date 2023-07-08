-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Create database

-- COMMAND ----------

CREATE DATABASE AZURE;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Create table

-- COMMAND ----------

drop table azure.emp;

-- COMMAND ----------

create table azure.emp
(
empno int,
name string,
job string,
boss int,
hiredate date,
salary int,
comm int,
deptno int
) using CSV
location  '/FileStore/tables/emp12.csv'

-- COMMAND ----------

create table azure.emp200
as
select * from azure.emp where deptno in (10,20,30);

-- COMMAND ----------

select * from azure.emp

-- COMMAND ----------

select * from azure.dept

-- COMMAND ----------

create table azure.dept
(
deptno int,
dname string,
loc string
) using DELTA
location '/FileStore/tables/dept'

-- COMMAND ----------

insert into azure.dept values (10,'ACCOUNTING','DALLAS');
insert into azure.dept values (20,'SW','BLR');
insert into azure.dept values (30,'HARDWARE','CHN');
insert into azure.dept values (10,'AGG','NHT');

-- COMMAND ----------

drop table azure.emp100;

-- COMMAND ----------

create table demo_parquet
(
deptno int,
dname string,
loc string
)
USING PARQUET
Location '/FileStore/tables/demo2';


-- COMMAND ----------

create table Azure.demo1
(
deptno int,
dname string,
loc string
)
USING DELTA
Location '/FileStore/tables/sample';

-- COMMAND ----------

create table sample2
(
id int,
name string
) using parquet;


-- COMMAND ----------

insert into  hadoop.sample2 values(30,'ABC')

-- COMMAND ----------

CONVERT to DELTA hadoop.sample2;

-- COMMAND ----------

DESCRIBE history azure.emp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Versioning

-- COMMAND ----------

RESTORE TABLE db.target_table TO VERSION AS OF <version>;
RESTORE TABLE hadoop.demo1 TO VERSION AS OF 5;

-- COMMAND ----------

CREATE TABLE default.people10m (
  id INT,
  firstName STRING,
  middleName STRING,
  lastName STRING,
  gender STRING,
  birthDate TIMESTAMP,
  ssn STRING,
  salary INT
)
USING DELTA
PARTITIONED BY (gender)
LOCATION '/tmp/delta/people-10m'

-- COMMAND ----------

select * from default.people10m 
SELECT * FROM default.people10m VERSION AS OF 0;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #UPSERT

-- COMMAND ----------

CREATE TABLE default.people10m (
  id INT,
  firstName STRING,
  middleName STRING,
  lastName STRING,
  gender STRING,
  birthDate TIMESTAMP,
  ssn STRING,
  salary INT
)
USING DELTA
PARTITIONED BY (gender)
LOCATION '/tmp/delta/people-10m'

-- COMMAND ----------

select * from default.people10m 
SELECT * FROM default.people10m VERSION AS OF 0;

-- COMMAND ----------

CREATE TABLE default.people10m_upload (
  id INT,
  firstName STRING,
  middleName STRING,
  lastName STRING,
  gender STRING,
  birthDate TIMESTAMP,
  ssn STRING,
  salary INT
) USING DELTA

-- COMMAND ----------

INSERT INTO default.people10m_upload VALUES
  (9999998, 'Billy', 'Tommie', 'Luppitt', 'M', '1992-09-17T04:00:00.000+0000', '953-38-9452', 55250),
  (9999999, 'Elias', 'Cyril', 'Leadbetter', 'M', '1984-05-22T04:00:00.000+0000', '906-51-2137', 48500),
  (10000000, 'Joshua', 'Chas', 'Broggio', 'M', '1968-07-22T04:00:00.000+0000', '988-61-6247', 90000)

-- COMMAND ----------

select * from default.people10m_upload

-- COMMAND ----------

MERGE INTO default.people10m
USING default.people10m_upload
ON default.people10m.id = default.people10m_upload.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *


-- COMMAND ----------

--select * from default.people10m_upload
select * from default.people10m

-- COMMAND ----------

INSERT INTO default.people10m_upload VALUES
  (20000001, 'John', '', 'Doe', 'M', '1994-09-17T04:00:00.000+0000', '345-67-8901', 55500),
  (20000002, 'Mary', '', 'Smith', 'F', '1982-10-29T01:00:00.000+0000', '456-78-9012', 98250),
  (20000003, 'Jane', '', 'Doe', 'F', '1981-06-25T04:00:00.000+0000', '567-89-0123', 89900)

-- COMMAND ----------

MERGE INTO default.people10m
USING default.people10m_upload
ON default.people10m.id = default.people10m_upload.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *


-- COMMAND ----------

select * from default.people10m

-- COMMAND ----------

--select * from default.people10m

-- COMMAND ----------

select * FROM default.people10m VERSION AS OF 2;

--SELECT * FROM default.people10m TIMESTAMP AS OF '2019-01-29 00:37:58';

-- COMMAND ----------

select * from azure.emp200;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #joins

-- COMMAND ----------

select * from azure.emp200

-- COMMAND ----------


--Name Empno Deptno Salary Dname 
---****************************
select E.name,
E.empno,
E.deptno,
E.salary,
D.Dname FROM azure.emp200 E inner join azure.dept D on (E.Deptno=D.Deptno)



-- COMMAND ----------

select E.name,
E.empno,
E.deptno,
E.salary,
D.Dname FROM azure.emp200 E left outer join azure.dept D on (E.Deptno=D.Deptno)

-- COMMAND ----------

select distinct E.name,
E.empno,
E.deptno,
E.salary,
D.Dname FROM azure.emp200 E right outer join azure.dept D on (E.Deptno=D.Deptno)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Functions

-- COMMAND ----------

 SELECT concat('Spark', ' SQL');

-- COMMAND ----------

SELECT date_add('2016-07-30', 1);

-- COMMAND ----------

--SELECT date_sub('2016-07-30', 1);
--SELECT datediff('2009-07-31', '2009-07-30');
--SELECT dayofyear('2022-10-26')
--SELECT dayofweek('2022-10-26')

select max(salary) from azure.emp200;
select min(salary) from azure.emp200;
select avg(salary) from azure.emp200;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Window functions

-- COMMAND ----------

use azure;
--RANK : duplicates if will skip the sequence
--DENSE_RANK
--ROW_NUMBER
select e1.* from
(
select empno,name,salary,deptno,rank() over (order by salary desc) rank from emp200
) e1 where rank=2

-- COMMAND ----------

select e1.* from
(
select empno,name,salary,deptno,dense_rank() over (order by salary desc) rank from emp200
) e1 where rank=3

-- COMMAND ----------

select e1.* from
(
select empno,name,salary,deptno,dense_rank() over (partition by deptno order by salary desc) rank from emp200
) e1 where rank=1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Delta
