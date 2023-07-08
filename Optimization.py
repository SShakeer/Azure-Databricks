# Databricks notebook source
# DBTITLE 1,Coalesce and Repartition

from pyspark.sql.types import *
#spark.sql.files.maxPartitionBytes
print(sc.defaultParallelism)
#nothing but no.of partitions when we create dataframe in the spark
#partitions will be created based on below parameter
#spark.sql.files.maxPartitionBytes 
#128MB


# COMMAND ----------

--step1: create dataframe
--step2: joins
--step3: applying transformations (400 partitions)
--df.coalesce(100)
--step4: create intermediate table
--step5: save as parquet file in ADLS

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

df1=df.repartition(20)

df1.rdd.getNumPartitions()

# COMMAND ----------

df1.rdd.glom().collect()

# COMMAND ----------

# DBTITLE 1,cache and Persist
from pyspark import SparkContext
import pyspark
df=spark.createDataFrame(range(10),IntegerType())

df1=df.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
df1=df.persist(pyspark.StorageLevel.
df1=df.persist(pyspark.StorageLevel.DISK_ONLY_2)
df1=df.persist(pyspark.StorageLevel.DISK_ONLY)
df1=df.persist(pyspark.StorageLevel.MEMORY_DISK_2)

# COMMAND ----------

df=spark.createDataFrame(range(10),IntegerType())

df.cache()
df.persist()

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Broad cast
Broadcast variables allow the programmer to keep a read-only variable cached on each machine rather than shipping a copy of it with tasks. 
• Immutable and cached on each worker nodes only once. 
• Efficient manner to give a copy of a large dataset to each node. 

--2 GB


# COMMAND ----------

#Broadcast a list 

numbers = (1,2,3,4,5) 
broadcastNumbers=spark.sparkContext.broadcast(numbers) 
display(broadcastNumbers.value)
#display(broadcastNumbers.value(0))

# COMMAND ----------

#accumilator
Accumulator is a shared variable to perform sum and counter operations. 
• These variables are shared by all executors to update and add information through associative or commutative operations. 

# COMMAND ----------

from pyspark.sql.functions import *
df_emp = spark.read.load('/FileStore/tables/emp-2',format='csv',sep=',',inferSchema=True,Header=True)
df_dept=spark.read.load('/FileStore/tables/dept.csv',format='csv',sep=',',inferSchema=True,Header=True)

df_join=df_emp.join(broadcast(df_dept),on='deptno',how='inner').select("deptno","sal")

# COMMAND ----------

df_join.explain()
