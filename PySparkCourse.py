# Databricks notebook source
# MAGIC %md
# MAGIC #create dataframe with Range

# COMMAND ----------

df = spark.range(1,10,2) 

# COMMAND ----------

df =spark.range(10)   

# COMMAND ----------

# MAGIC %md
# MAGIC #createDataFrame():

# COMMAND ----------

lst1=[
  [10,"REDDY",20000,'BANGALORE'],
  [20,"GANI",30000,'CHENNAI'],
  [30,"SIVA",20000,'HYDERABAD']
]

scema1=['deptno','ename','salary','city']

df=spark.createDataFrame(lst1,scema1)
display(df)

# COMMAND ----------

#This creates a DataFrame from a collection(list, dict), RDD or Python Pandas.
lst = (('Robert',35),('James',25)) 
spark.createDataFrame(data=lst)##With Out Schema 
df = spark.createDataFrame(data=lst,schema=('Name','Age'))  ##With Schema


# COMMAND ----------

#using dictinary
dict = ({"name":"robert","age":25}, {"name" : "james","age" : 31}) 
df = spark.createDataFrame(dict)
#display(df)
df.show()


# COMMAND ----------

#using rdd
lst3 = (('Robert',35),('James',25)) 
rdd = sc.parallelize(lst3) 
df =  spark.createDataFrame(data=rdd,schema=('name string, age long'))

df.show()
#DF=RDD+schema

# COMMAND ----------

# Using Python Pandas DamaFrame
#Pandas dataframe is a two dimensional structure with named rows and columns. So data is aligned in a tabular fashion in rows and columns
import pandas as pd
data = (('tom', 10), ('nick', 15), ('juli', 14)) 
df_pandas = pd.DataFrame(data,columns=('Name','Age')) 


df = spark.createDataFrame(data=df_pandas)


# COMMAND ----------

# MAGIC %md
# MAGIC #SQL Approache

# COMMAND ----------

lst1 = (('Robert',35),('James',25)) 
lst2= (('Robert',101),('James',102))
df_emp = spark.createDataFrame(data=lst1,schema=('EmpName','Age')) 
df_emp.createOrReplaceTempView("dept")

# COMMAND ----------

df_dept = spark.createDataFrame(data=lst2,schema=('EmpName',’DeptNo'))
df_dept.createOrReplaceTempView("dept")
df_joined = spark.sql (""" select e.name,e.age,d.dept from emp e join dept d where e.name = d.name """)
createOrReplaceTempView("table1") #Creates the view in the current database and valid for only one session.
createOrReplaceGlobalTempView("table1")  
                                                  
                                                  
 #Creates the views in global_temp database. // Valid across all sessions of      an application.                                                  
                                                  

# COMMAND ----------

df = spark.sql ("""select * from emp""")

# COMMAND ----------

df_op = spark.table("emp") 
sorted(df_op.collect()) == sorted(df_emp.collect())


# COMMAND ----------

# MAGIC %md
# MAGIC #Reading CSV file

# COMMAND ----------

#df = spark.read.load(path='/FileStore/tables/emp.csv', format='csv', schema=('order_id int,order_date string,order_customer_id int,order_status string'))

#df = spark.read.load(path='/FileStore/tables/emp.csv', format='csv',inferSchema=True,Header=True,sep=',')
#df=spark.read.load('practice/retail_db/testSpace.txt',format='csv',sep=',',ignoreLeadingWhiteSpace=True,ignoreTr ailingWhiteSpace=True)
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #Read text file

# COMMAND ----------

Load a text File: 
  • Use text file where there is fixed length. 
  • Default field name is ‘value’. 
  • Also you may load Into rdd. 
  • Convert to dataframe using toDF() and Row

# COMMAND ----------

df = spark.read.load('/FileStore/tables/emp10.txt',format='text')
#--Read the whole text file into a single line. 
#df =spark.read.load('practice/retail_db/orders',format='text', wholeText=True)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Reading ORC and Parquet files

# COMMAND ----------

df = spark.read.load('practice/retail_db/orders_orc',format='orc')
df = spark.read.load('practice/retail_db/orders_parquet',format='parquet')

# COMMAND ----------

CSV, JSON and AVRO are Row-based File formats. 
Sample data in CSV: 
  Deptno,Dname
  10,'Bangalore'
  
  ORC,PARQUET are Column-based File formats. 
  ID/INT/3:1,2 
  FIRST_NAME/STRING/11:REDDY,GANI 
  AGE/INT/6:19,25


# COMMAND ----------

# MAGIC %md
# MAGIC #Read JSON

# COMMAND ----------

df = spark.read.load(‘practice/retail_db/orders_json',format='json')
                     #JAVA SCRIPT OBJECT NOTATION


# COMMAND ----------

# MAGIC %md
# MAGIC #Load JDBC table

# COMMAND ----------

# MAGIC %scala
# MAGIC val df= spark.read.format("jdbc").option("url", "jdbc:oracle:thin:scott/tiger@192.168.29.210:1521/ORCL").option("driver", "oracle.jdbc.driver.OracleDriver").option("dbtable","scott.emp").option("user", "scott").option("password", "tiger").load()

# COMMAND ----------

df= spark.read.format("jdbc")
.option("url", "jdbc:oracle:thin:@xxxx-xxx-xxxx:1521/xxx")
.option("driver", "oracle.jdbc.driver.OracleDriver")
.option("dbtable","(SELECT * FROM T_EMP WHERE ID=1) query" )
.option("user", "{someUser}")
.option("password", "{xxx}")
.load()


# COMMAND ----------

#: Partition 
Partitioning can be done only on numeric or date fields. If there is no numeric field generate it. For ex- Use ROWNUM to generate dummy numeric fields in Oracle Database. Define partitionColumn, numPartitions, lowerBound, upperBound.


# COMMAND ----------

df= spark.read.format("jdbc")
.option("url", "jdbc:oracle:thin:@xxxx-xxx-xxxx:1521/xxx")
.option("driver", "oracle.jdbc.driver.OracleDriver")
.option("dbtable","ORDERS" )
.option("partitionColumn","ORDER_ID")
#.option("lowerBound", "500")
#.option("upperBound", "1000")
.option("numPartitions","5")
.option("user", "someUser")
.option("password", "somePassword")
.load()


# COMMAND ----------

# MAGIC %md
# MAGIC #SparkSession

# COMMAND ----------

new_spark = spark.newSession()
spark.sql (""" create table student1(name int) """); 

spark.sql (""" insert into student1 values (2) """);
spark.sql (""" select count(*) from student1 """).show() 
new_spark.sql (""" select count(*) from student1 """).show()


# COMMAND ----------

#100 -query 
#dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC #Supported Data types

# COMMAND ----------

Below are some of the important data types supported in both Data Frame and Spark SQL
from pyspark.sql.types import *

IntegerType() -4-byte signed integer numbers 
FloatType() -4-byte single-precision floating point numbers 
DoubleType() -8-byte double-precision floating point numbers
StringType()- Character String Values 
VarcharType(length) -Variant of StringType with Length limitation 
CharType(length)- Variant of VarcharType with Fixed Length
TimestampType() - year, month, day, hour, minute, second, time zone 
DateType ()- year, month, day
ArrayType (elementType,containsNull) 
MapType (keyType, valueType,valueContainsNull) 
StructType (fields)


# COMMAND ----------

#Example
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
schema = StructType(
  (StructField("name",StringType(),True), 
   StructField("id", IntegerType(),True),
  ))

data=(("Reddy",1), ("Siva",2), ("ABC",3) )
df = spark.createDataFrame(data=data,schema=schema) 
df.printSchema() 
df.show(truncate=False) 


# COMMAND ----------

# (Map Types)

schema = StructType(
  (StructField('name', StringType(), True), 
   StructField('properties', MapType(StringType(),StringType()),True) 
  )
)
d = (('REDDY',{'hair':'black','eye':'brown'}), ('MAHEER',{'hair':'brown','eye':None}), ('SIVA',{'hair':'red','eye':'black'}) ) 
df_map= spark.createDataFrame(data=d, schema = schema) 
df_map.printSchema() 
df_map.show(truncate=False) 
df_map.select(df_map.properties).show(truncate=False) 
df_map.select(df_map.properties('eye')).show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC #Dataframes- Column

# COMMAND ----------

A column in a DataFrame. 
• Available in pyspark.sql.Column


# COMMAND ----------

#ord = spark.read.load('/FileStore/tables/emp.csv',format='csv',sep=',',schema=('order_id int,order_date timestamp, order_cust_id int,order_status string') )
from pyspark.sql.functions import col
df = spark.read.load('/FileStore/tables/emp-2',format='csv',sep=',',inferSchema=True,Header=True)
#Select a Column 
#df.order_id or df("order_id") 
#ord.select(col("*")).show()  

#df.printSchema()

# COMMAND ----------

#df.select(col("ENAME"),col("DNAME")).show()
from pyspark.sql.functions import lit
df1=df.withColumn("city",lit("Bangalore")).withColumn("Gender",lit("M")).withColumn("age",lit(20))

# COMMAND ----------

display(df.select(col("*")))

# COMMAND ----------

# Give a alias name to a column 
df.select(col("job").alias("manager"))
#ord.printSchema()

#df.select("job as manager").show()

# COMMAND ----------

df.orderBy(col("empno").asc()).show()

# COMMAND ----------

#order by
ord.orderBy(col("empno").desc()).select(col("empno")).distinct().show() 

# COMMAND ----------

#.cast() : Convert type of a column
df.select(col("deptno").cast("int")).show()

# COMMAND ----------

#between(): 
#PS: Print all the deptno between 10 and 20.
df.where(col("deptno").between(10,20)).show()


# COMMAND ----------

#contains(), startswith,endswith(),like(),rlike() 
# Print all the empno with job CLERK. 
#df.where(col("job").like('CLERK')).show()
df.where(col("job").contain('CLERK')).show()

# COMMAND ----------

df.where(col("ename").like('%TH')).show()


# COMMAND ----------

# Multiple values. 

df.where(col("job").isin('CLERK','MANAGER')).show()
#.select(col("job")).distinct().show()


# COMMAND ----------

ord.printSchema()

# COMMAND ----------

# substr
#Find Number of emp joined in the year 1980 and location dallas. 
#df2=df.where((col("hiredate").substr(8,2).contains('80')) & (col("loc").contains('DALLAS'))) 

#ord.where((col("hiredate").substr(1,4) == '1980') & (col("job") == 'CLERK')).count()
#df.where(col("hiredate").substr(8,2) == '80')


# COMMAND ----------

# when(), otherwise() : 
from pyspark.sql.functions import * 
ord.select(col("job"), 
           when(col("job") == 'CLERK', 'office')
           .when(col("job") == 'MANAGER', 'CL')
           .when(col("job" == 'PRESIDENT', 'CO')
                 .when(col("job" == 'SALESMAN','PR'))
                 .otherwise(col("job").alias("job22"))


# COMMAND ----------

from pyspark.sql.functions import when

df2 = ord.withColumn("ac",when(col("job") == 'M','Male').when(col("job") == 'F','Female').otherwise('ddfd')

# COMMAND ----------

# MAGIC %md
# MAGIC #Selection APIs

# COMMAND ----------

data=(('Reddy',35,40,40),('Gani',35,40,40),('Charan',31,33,29),('Ram',31,33,91)) 
emp = spark.createDataFrame(data=data,schema=('name','score1','score2','score3'))
#emp.select("*").show()
emp.select(emp.score1,"score2", (emp.score1 +100).alias("score3")).show() 

# COMMAND ----------

#selectExpr(*expr) 
#ord.selectExpr('substring(hiredate,1,10) as order_month').show() #
ord.select(substring(col("hiredate"),7,4).alias('year')).show() 

# COMMAND ----------

ord.withColumn('order_month',substring(col("hiredate"),1,10)).show()


# COMMAND ----------

order = ord.drop('order_id','order_date')
emp.dropDuplicates().show() 
emp.dropDuplicates(("name","score1","score2")).show()


# COMMAND ----------

 filter(condition): (Its alias ‘where’) 
  ✓ Filter rows using a given condition. 
  ✓ use '&' for 'and‘. '|' for 'or‘. (boolean expressions) 
  ✓ Use column function isin() for multiple search.  
  ✓ Or use IN Operator for SQL Style syntax.   

# COMMAND ----------

df.where((col("deptno") > 10) & (col("order_id") < 20)).show() 
df.where(col("dname").isin('ACCOUNTING','HR')).show() 
df.where("dname IN ('ACCOUNTING','HR')").show()


# COMMAND ----------

# MAGIC %md
# MAGIC #Sort and Order by

# COMMAND ----------

df.sort(df.deptno.desc(),df.deptno.asc()).show() 

# COMMAND ----------

# MAGIC %md
# MAGIC #Set Operators

# COMMAND ----------

 #union() and unionAll(): 
df1 = spark.createDataFrame(data=(('a',1),('b',2)),schema=('col1 string,col2 int')) 
df2 = spark.createDataFrame(data=((2,'b'),(3,'c')),schema=('col2 int,col1 string')) 
df1.union(df2).show() 
df1.unionByName(df2).show()
  

# COMMAND ----------

intersect(): Containing rows in both DataFrames. Removed duplicates. 
intersectAll(): Same as intersect. But retains the duplicates. 
exceptAll(): Rows present in one DataFrame but not in another. 

# COMMAND ----------

df1 = spark.createDataFrame(data=(('a',1),('a',1),('b',2)),schema=('col1 string,col2 int')) 
df2 = spark.createDataFrame(data=(('a',1),('a',1),('c',2)),schema=('col1 string,col2 int')) df1.intersect(df2).show() 
df1.intersectAll(df2).show()
df1.exceptAll(df2).show()


# COMMAND ----------

# MAGIC %md
# MAGIC #Joins

# COMMAND ----------

from pyspark.sql.functions import col
df1 = spark.createDataFrame(data=((1,'Reddy'),(2,'Venkat'),(3,'James')),schema='empid int,empname string') 
df2 = spark.createDataFrame(data=((2,'USA'),(4,'India')),schema='empid int,country string') 

df_inner = df1.join(df2, on=['empid'], how='inner')
df_inner.show()

# COMMAND ----------

#df_left = df1.join(df2, on=['empid'], how='left')
df_left = df1.join(df2, on=['empid'], how='left_outer')
df_left.show()

# COMMAND ----------

#df_left = df1.join(df2, on=['empid'], how='left')
df_right = df1.join(df2, on=['empid'], how='right_outer')
df_right.show()

# COMMAND ----------

#multi column join

df1 = spark.createDataFrame(data=((1,101,'Robert'),(2,102,'Ria'),(3,103,'James')),schema='empid int,deptid int,empname string') 
df2 = spark.createDataFrame(data=((2,102,'USA'),(4,104,'India')),schema='empid int,deptid int,country string') 

#df1.join(df2, & (df1.deptid == df2.deptid)).show()
df_right = df1.join(df2, on=['empid','deptid'], how='inner').show()

# COMMAND ----------

data = [["10", "Najeeb", "company10"],
        ["20", "Reddy", "company18"], 
        ["30", "Gani", "company20"],
        ["40", "Ramdev", "company11"], 
        ["50", "Chiru", "company14"]]

columns = ['empno', 'ename', 'org']
df = spark.createDataFrame(data, columns)

data1 = [["10", "45000", "IT"], 
         ["20", "145000", "Manager"], 
         ["60", "45000", "HR"],
         ["50", "34000", "Sales"]]
  
# specify column names
columns2 = ['empno', 'salary', 'department']
  
# creating a dataframe from the lists of data
df1 = spark.createDataFrame(data1, columns2)

df.join(df1, df.empno == df1.empno,'inner').show()

# COMMAND ----------

#leftanti
#This join is like df1-df2, as it selects all rows from df1 that are not present in df2.
df.join(df1, df.empno == df1.empno,'leftanti').show()

# COMMAND ----------

#leftsemi

#This is like inner join, with only the left dataframe columns and values are selected
df.join(df1, df.empno == df1.empno,'leftsemi').show()


# COMMAND ----------

df.join(df1, (df.empno == df1.empno) & (df1.department=='IT'),'inner').show()


# COMMAND ----------

# MAGIC %md
# MAGIC # Aggregation

# COMMAND ----------

from pyspark.sql.functions import avg
df1.select(avg(df1.salary).alias("avg_sal")).show()
df1.select(min(df1.salary).alias("avg_sal")).show()
df1.select(max(df1.salary).alias("avg_sal")).show() 
df1.select(sum(df1.salary), sumDistinct(df1.salary)).show() 
df1.select(count(df1.salary),countDistinct(df1.salary).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #GroupBy

# COMMAND ----------

When we apply groupBy on a DataFrame Column, it returns GroupedDataobject. It has below aggregate functions
avg(),
mean() 
count() 
min() 
max() 
sum() 
agg() → For multiple aggregations at once 
pivot() 

# COMMAND ----------

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
df.groupBy(df.dept).avg("salary").alias("avvg").show()


# COMMAND ----------

df.groupBy(df.dept,df.state).min("salary","age").show()


# COMMAND ----------

df.groupBy(df.dept).min("salary").alias('min_salary')


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# import pyspark class Row from module sql
from pyspark.sql import *

# Create Example Data - Departments and Employees

# Create the Departments
department1 = Row(id='123456', name='Computer Science')
department2 = Row(id='789012', name='Mechanical Engineering')
department3 = Row(id='345678', name='Theater and Drama')
department4 = Row(id='901234', name='Indoor Recreation')

# Create the Employees
Employee = Row("firstName", "lastName", "email", "salary")
employee1 = Employee('michael', 'armbrust', 'no-reply@berkeley.edu', 100000)
employee2 = Employee('xiangrui', 'meng', 'no-reply@stanford.edu', 120000)
employee3 = Employee('matei', None, 'no-reply@waterloo.edu', 140000)
employee4 = Employee(None, 'wendell', 'no-reply@berkeley.edu', 160000)
employee5 = Employee('michael', 'jackson', 'no-reply@neverla.nd', 80000)

# Create the DepartmentWithEmployees instances from Departments and Employees
departmentWithEmployees1 = Row(department=department1, employees=[employee1, employee2])
departmentWithEmployees2 = Row(department=department2, employees=[employee3, employee4])
departmentWithEmployees3 = Row(department=department3, employees=[employee5, employee4])
departmentWithEmployees4 = Row(department=department4, employees=[employee2, employee3])

print(department1)
print(employee2)
print(departmentWithEmployees1.employees[0].email)

# COMMAND ----------

unionDF = df1.union(df2)
display(unionDF)

# COMMAND ----------

# Remove the file if it exists
dbutils.fs.rm("/tmp/databricks-df-example.parquet", True)
unionDF.write.parquet("/tmp/databricks-df-example.parquet")

# COMMAND ----------

parquetDF = spark.read.parquet("/tmp/databricks-df-example.parquet")
display(parquetDF)

# COMMAND ----------

from pyspark.sql.functions import explode

explodeDF = unionDF.select(explode("employees").alias("e"))
flattenDF = explodeDF.selectExpr("e.firstName", "e.lastName", "e.email", "e.salary")

flattenDF.show()

# COMMAND ----------

filterDF = flattenDF.filter(flattenDF.firstName == "xiangrui").sort(flattenDF.lastName)
display(filterDF)

# COMMAND ----------

from pyspark.sql.functions import col, asc

# Use `|` instead of `or`
filterDF = flattenDF.filter((col("firstName") == "xiangrui") | (col("firstName") == "michael")).sort(asc("lastName"))
display(filterDF)

# COMMAND ----------

whereDF = flattenDF.where((col("firstName") == "xiangrui") | (col("firstName") == "michael")).sort(asc("lastName"))
display(whereDF)

# COMMAND ----------

#Replace null values with -- using DataFrame Na function
nonNullDF = flattenDF.fillna("--")
display(nonNullDF)

# COMMAND ----------

#Retrieve only rows with missing firstName or lastName
filterNonNullDF = flattenDF.filter(col("firstName").isNull() | col("lastName").isNull()).sort("email")
display(filterNonNullDF)
#Example aggregations using agg() and countDistinct()
from pyspark.sql.functions import countDistinct

countDistinctDF = nonNullDF.select("firstName", "lastName")\
  .groupBy("firstName")\
  .agg(countDistinct("lastName").alias("distinct_last_names"))

display(countDistinctDF)

countDistinctDF.explain()
#Sum up all the salaries
salarySumDF = nonNullDF.agg({"salary" : "sum"})
display(salarySumDF)

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
df.printSchema()

# Instead of registering a UDF, call the builtin functions to perform operations on the columns.
# This will provide a performance improvement as the builtins compile and run in the platform's JVM.

# Convert to a Date type
df = df.withColumn('date', F.to_date(df.end_date))

# Parse out the date only
df = df.withColumn('date_only', F.regexp_replace(df.end_date,' (\d+)[:](\d+)[:](\d+).*$', ''))

# Split a string and index a field
df = df.withColumn('city', F.split(df.location, '-')[1])

# Perform a date diff function
df = df.withColumn('date_diff', F.datediff(F.to_date(df.end_date), F.to_date(df.start_date)))

# COMMAND ----------

# Provide the min, count, and avg and groupBy the location column. Diplay the results
agg_df = df.groupBy("location").agg(F.min("id"), F.count("id"), F.avg("date_diff"))
display(agg_df)

# COMMAND ----------

df = df.withColumn('end_month', F.month('end_date'))
df = df.withColumn('end_year', F.year('end_date'))
df.write.partitionBy("end_year", "end_month").parquet("/tmp/sample_table")
display(dbutils.fs.ls("/tmp/sample_table"))

# COMMAND ----------

null_item_schema = StructType([StructField("col1", StringType(), True),
                               StructField("col2", IntegerType(), True)])
null_df = spark.createDataFrame([("test", 1), (None, 2)], null_item_schema)
display(null_df.filter("col1 IS NOT NULL"))

# COMMAND ----------


