# Databricks notebook source
# DBTITLE 1,Window Functions
Window Functions operates on a group of rows and return a single value for each input row. 
Main Package: pyspark.sql.window. It has two classes Window and WindowSpec 
• Window class has APIs such as partitionBy, orderBy, rangeBetween, rowsBetween. 
• WindowSpec class defines the partitioning, ordering and frame boundaries

# COMMAND ----------

# MAGIC %run "/Users/basha.ora11@gmail.com/Batch4/data"

# COMMAND ----------

emp_df.show()

# COMMAND ----------

# DBTITLE 1,Dept wise highest salary
from pyspark.sql.functions import *
from pyspark.sql.window import * 

spec = Window.partitionBy("deptno").orderBy(desc("sal"))
df_rank=emp_df.withColumn("rank1",dense_rank().over(spec))

df_filter=df_rank.filter(col("rank1")==2).select("empno","deptno","sal")

# COMMAND ----------



from pyspark.sql.functions import *
from pyspark.sql.window import * 

#df1=df.where(col("dept")=='Sales')
df2=emp_df.withColumn("city",lit("Bangalore")).withColumn("pincode",lit(12343454))
spec = Window.partitionBy("deptno").orderBy(desc("sal"))
df3=df2.withColumn("rank1",dense_rank().over(spec))
df4=df3.where(col("rank1")==1).show()

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
df = spark.createDataFrame(data=data,schema=schema)

df2=df.groupBy(df.dept).max("salary").alias("avvg")
df2.show()

# COMMAND ----------

spec = Window.partitionBy("deptno")
emp_df.select(col("deptno"),col("sal")) \
.withColumn("sum_sal",sum("sal").over(spec)) \
.withColumn("max_sal",max("sal").over(spec)) \
.withColumn("min_sal",min("sal").over(spec)) \
.withColumn("avg_sal",avg("sal").over(spec)) \
.withColumn("count_sal",count("sal").over(spec)).show()


# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

# Build an example DataFrame dataset to work with.
dbutils.fs.rm("/tmp/dataframe_sample.csv", True)
dbutils.fs.put("/tmp/dataframe_sample.csv", """id|end_date|start_date|location
1|2015-10-14 00:00:00|2015-09-14 00:00:00|CA-SF
2|2015-10-15 01:00:20|2015-08-14 00:00:00|CA-SD
3|2015-10-16 02:30:00|2015-01-14 00:00:00|NY-NY
4|2015-10-17 03:00:20|2015-02-14 00:00:00|NY-NY
5|2015-10-18 04:30:00|2014-04-14 00:00:00|CA-SD
""", True)

df = spark.read.format("csv").options(header='true', delimiter = '|').load("/tmp/dataframe_sample.csv")
df.show()

# Instead of registering a UDF, call the builtin functions to perform operations on the columns.
# This will provide a performance improvement as the builtins compile and run in the platform's JVM.

# Convert to a Date type
df2 = df.withColumn('date', F.to_date(df.end_date))

# Parse out the date only
df3 = df.withColumn('date_only', F.regexp_replace(df.end_date,' (\d+)[:](\d+)[:](\d+).*$', ''))

# Split a string and index a field
df = df.withColumn('city', F.split(df.location, '-')[0])

# Perform a date diff function
#df = df.withColumn('date_diff', F.datediff(F.to_date(df.end_date), F.to_date(df.start_date)))

# COMMAND ----------

#Provide the min, count, and avg and groupBy the location column. Diplay the results
agg_df = df.groupBy("location").agg(F.min("id"), F.count("id"))
display(agg_df)

# COMMAND ----------

df = df.withColumn('end_month', F.month('end_date'))
df = df.withColumn('end_year', F.year('end_date')).show()
#df.write.partitionBy("end_year", "end_month").parquet("/tmp/sample_table")
#display(dbutils.fs.ls("/tmp/sample_table"))
