# Databricks notebook source
# MAGIC %md
# MAGIC #expr

# COMMAND ----------

from pyspark.sql.functions import *
data = [["James","Sales","NY",9000,34], 
        ["Alicia","Sales","NY",8600,56], 
        ["Robert","Sales","CA",8100,30], 
        ["Lisa","Finance","CA",9000,24], 
        ["Deja","Finance","CA",9900,40], 
        ["Sugie","Finance","NY",8300,36], 
        ["Ram","Finance","NY",7900,53], 
        ["Kyle","Marketing","CA",8000,25], 
        ["Reid","Marketing","NY",9100,50]]

schema=("empname","dept","state","salary","age") 
df = spark.createDataFrame(data,schema)

df2=df.groupBy("dept").agg(max("salary").alias("avg_salary"))
#df2.show()

#select deptno,max(sal) from emp group by deptno

# COMMAND ----------

df.select(expr("salary+100").alias("new_salary")).show()

# COMMAND ----------

#expr(str): • It takes SQL Expression as a string argument, executes the expression and returns a Column Type. 
df.withColumn("age_desc",expr("case when age > 50 then 'Senior' else 'Adult' end")).show() 
df.withColumn('emp-dept',expr(" empname ||'-'|| dept")).show() 
df.select(expr("age + 10 as age_10")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #regexp_extract

# COMMAND ----------

\d =Matches digits 0-9.
\w =Matches alphabets and digits.
\s =Matches space.
. =Matches any character except newline.
? =0 or more character.
+ =1 or more character.
(a-zA-Z)= Anything in range a-z and A-Z.


# COMMAND ----------

# MAGIC %md
# MAGIC #regexp_replace

# COMMAND ----------

df.select(regexp_replace(lit('11ss1 ab'),'(\d+)','xx').alias('op')).show()


# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import *
addr = ((1,"2625 Indian School Rd","Phoenix"), (2,"1234 Thomas  St","Glendale")) 
df =spark.createDataFrame(addr,("id","addr","city")) 


df.withColumn('new_addr', when(df.addr.endswith('Rd'),regexp_replace(df.addr,'Rd','Road')) .when(df.addr.endswith('St'),regexp_replace(df.addr,'St','Street')).otherwise(df.addr)).show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC #Date Functions

# COMMAND ----------

df.select(current_date())

# COMMAND ----------

from pyspark.sql.functions import *
df1=df.withColumn("adddays",current_date()+10).withColumn("addmonths",add_months(current_date(),10))

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #Write Methods

# COMMAND ----------

df.write.save('/FileStore/tables/sample20221101',format='csv',sep='#',mode='overwrite',header='True', compression='gzip')


# COMMAND ----------

df.repartition(10).write.save('practice/dump/retail_db/orderText',format='text')

df.write.save('practice/dump/retail_db/orderParquet',format='parquet',mode='overwrite',partitionBy="deptno")


rd.write.save('practice/dump/retail_db/orderOrc',format='orc', partitionBy="order_status")

ord.coalesce(1).write.save('practice/dump/retail_db/orderJson',format='json', compression='bzip2')

ord.write.save('practice/dump/retail_db/orderAvro',format='avro')


# COMMAND ----------

df.write.saveAsTable(name='default.order_test', format='orc')
#df.write.saveAsTable(name='db2_orders.order_test', format='orc', mode='overwrite')
#df.write.saveAsTable('azure.emp',partitionBy='deptno',mode='overwrite',compression='none')


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from default.order_test;

# COMMAND ----------

#Ex-1 ( Write into a Table. Table should not exist.) 
df.coalsce(2).write.format("jdbc") \ 
.option("url", url) \ 
.option("driver", "oracle.jdbc.driver.OracleDriver") \ 
.option("dbtable","new_orders" ) \ 
.option("user", someUser) \ 
.option("password", somePassword) \ 
.save()


#If Table exist, use mode. 
df.write.format("jdbc") \ 
.option("url", url) \ 
.option("driver", "oracle.jdbc.driver.OracleDriver") \ 
.option("dbtable","new_orders" ) \ 
.option("user", someUser) \ 
.option("password", somePassword) \ 
.mode("append") \ 
.save() 

# COMMAND ----------

#batchsize: The JDBC batch size, which determines how many rows to insert per round trip. This can help performance on JDBC drivers. This option applies only to writing. It defaults to 1000.
ord.select('order_id').write.format("jdbc") \ 
.option("url", url) \ 
.option("driver", "oracle.jdbc.driver.OracleDriver") \ 
.option("createTableColumnTypes","order_id char(10)" ) \ 
.option("dbtable","new_orders" ) \ 
.option("user", someUser) \ 
.option("password", somePassword) \ 
.option("batchsize",50000) \ 
.mode("overwrite") \ 
.save()


# COMMAND ----------

# MAGIC %md
# MAGIC #Coalesce and Repartition

# COMMAND ----------

#spark.sql.files.maxPartitionBytes
sc.defaultParallelism
#nothing but no.of partitions when we create dataframe in the spark
#partitions will be created based on below parameter
spark.sql.files.maxPartitionBytes //128MB


# COMMAND ----------

from pyspark.sql.types import *
df=spark.createDataFrame(range(10),IntegerType())

df.rdd.getNumPartitions()

# COMMAND ----------

df.rdd.glom().collect()

# COMMAND ----------

df4=df.coalesce(2)


# COMMAND ----------

df4.rdd.glom().collect()

# COMMAND ----------

#if i load all small files as dataframe, that many partitions will be created.
df=spark.read.format("")//3 files
 

# COMMAND ----------

spark.conf.set("spark.sql.files.maxPartitionBytes",200000)#changing partition size to 200Bytes

# COMMAND ----------

df1=df.repartition(20)

df1.rdd.getNumPartitions()

# COMMAND ----------

df1.rdd.glom().collect()

# COMMAND ----------

df2=df1.coalesce(2)

# COMMAND ----------

df2.rdd.glom().collect()

# COMMAND ----------

# MAGIC %md
# MAGIC #cache and Persist

# COMMAND ----------

from pyspark import SparkContext
import pyspark
df=spark.createDataFrame(range(10),IntegerType())

df1=df.persist(pyspark.StorageLevel.MEMORY_AND_DISK)


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #broadcast

# COMMAND ----------

Broadcast variables allow the programmer to keep a read-only variable cached on each machine rather than shipping a copy of it with tasks. 
• Immutable and cached on each worker nodes only once. 
• Efficient manner to give a copy of a large dataset to each node. 



# COMMAND ----------

#Broadcast a list 

numbers = (1,2,3,4,5) 
broadcastNumbers=spark.sparkContext.broadcast(numbers) 
display(broadcastNumbers.value)
#display(broadcastNumbers.value(0))


# COMMAND ----------

#accumilator
Accumulator is a shared variable to perform sum and counter operations. • These variables are shared by all executors to update and add information through associative or commutative operations. 

# COMMAND ----------

from pyspark.sql.functions import *
df_emp = spark.read.load('/FileStore/tables/emp-2',format='csv',sep=',',inferSchema=True,Header=True)
df_dept=spark.read.load('/FileStore/tables/dept.csv',format='csv',sep=',',inferSchema=True,Header=True)

df_join=df_emp.join(broadcast(df_dept),on='deptno',how='inner').select("deptno","sal")

# COMMAND ----------

df_join.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC #SparkSQL

# COMMAND ----------

df_join.createOrReplaceTempView("sample_table1")
df_join.createOrReplaceTempView("sample_table2")
df_join.createOrReplaceTempView("sample_table")
df_join.createOrReplaceTempView("sample_table")
df_join.createOrReplaceTempView("sample_table")
df_join.createOrReplaceTempView("sample_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sample_table;

# COMMAND ----------

#spark.sql("")


df100=spark.sql("select deptno,count(*) as total from emp100 group by deptno").show()

# COMMAND ----------

df100=spark.sql("select deptno,count(*) as total from emp100 group by deptno").show()

# COMMAND ----------

df1=spark.sql("select * from emp100")
df2=spark.sql("select * from sample_table")
df1.printSchema()
df2.printSchema()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #Dbutils

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

#dbutils.fs.head("/FileStore/tables/emp.csv",10)
#dbutils.fs.ls("/FileStore/tables")
#dbutils.fs.mkdirs("/FileStore/tables/Reddy")
dbutils.fs.rm("/FileStore/tables/Reddy")

# COMMAND ----------

dbutils.fs.head("/FileStore/tables/wordcount.txt,"/FileStore/tables/wordcount.txt")

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

# MAGIC %md
# MAGIC #Delta tables

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

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

# COMMAND ----------

people_df = spark.read.table("people10m")

# COMMAND ----------

# MAGIC %md
# MAGIC #SCD Type-1 demo

# COMMAND ----------

data = [[10, "Najeeb", "company10",1000],
        [20, "Reddy", "company18",2000], 
        [30, "Gani", "company20",2345],
        [40, "Ramdev", "company11",7889], 
        [50, "Chiru", "company14",8990]]

columns = ['empno', 'ename', 'org','sal']
df = spark.createDataFrame(data, columns)

# COMMAND ----------

data = [[10, "Najeeb", "company10",1000],
        [20, "Reddy", "company18",2000], 
        [30, "Gani", "company20",2345],
        [40, "Ramdev", "company11",7889], 
        [50, "Chiru", "company14",8990],
       [60,"Shakeer","abc",4000]]

columns = ['empno', 'ename', 'org','sal']
df = spark.createDataFrame(data, columns)

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC create table emp_target
# MAGIC (
# MAGIC empno int,
# MAGIC ename string,
# MAGIC org string,
# MAGIC sal int
# MAGIC ) using delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_target

# COMMAND ----------

# MAGIC %python
# MAGIC spark.sql("insert into emp_source values (60,'Shakeer','ABC',5900)")

# COMMAND ----------

df.createOrReplaceTempView("emp_source")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO emp_target
# MAGIC USING emp_source
# MAGIC ON emp_source.empno = emp_target.empno
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET 
# MAGIC emp_target.empno=emp_source.empno,
# MAGIC emp_target.ename=emp_source.ename,
# MAGIC emp_target.org=emp_source.org,
# MAGIC emp_target.sal=emp_source.sal
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_target

# COMMAND ----------

# MAGIC %md
# MAGIC #dataframe approache

# COMMAND ----------

# MAGIC %sql
# MAGIC create table emp1_target
# MAGIC (
# MAGIC empno int,
# MAGIC ename string,
# MAGIC org string,
# MAGIC sal int
# MAGIC ) using delta
# MAGIC location "/FileStore/tables/merge_target"

# COMMAND ----------

data = [[10, "Najeeb", "company10",1000],
        [20, "Reddy", "company18",2000], 
        [30, "Gani", "company20",2345],
        [40, "Ramdev", "company11",7889], 
        [50, "Chiru", "company14",8990],
       [60,"Maheer","abcd",40200]]

columns = ['empno', 'ename', 'org','sal']
df = spark.createDataFrame(data, columns)

# COMMAND ----------

df.createOrReplaceTempView("emp_source1")

# COMMAND ----------

from delta.tables import *
df_delta=DeltaTable.forPath(spark,"/FileStore/tables/merge_target")

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
# MAGIC select * from emp1_target
