# Databricks notebook source
# MAGIC %run "/Users/basha.ora11@gmail.com/Batch4/data"

# COMMAND ----------

dept_df.write.json(f'/FileStore/tables/courses100', mode='overwrite')
#dbutils.fs.ls(f'/user/{username}/courses')

# COMMAND ----------

dbutils.fs.ls(f'/FileStore/tables/courses100')

# COMMAND ----------

emp_df.write.format('json').save(f'/user/{username}/courses', mode='overwrite')

# COMMAND ----------

# Default behavior
# It will delimit the data using comma as separator
courses_df.write.csv(f'/user/{username}/courses')

# COMMAND ----------

dept_df.write.format('csv').save(f'/FileStore/tables/dept1000.csv')

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/")

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/tables/merge_target2/")

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/dept1000.csv")

# COMMAND ----------

emp_df. \
    coalesce(1). \
    write. \
    csv(f'/FileStore/tables/dept.csv', mode='overwrite', header=True)

# COMMAND ----------

#spark.read.text(f'/user/{username}/courses').show(truncate=False)

# COMMAND ----------

spark.read.csv(f'/FileStore/tables/dept.csv', header=True).show()

# COMMAND ----------

emp_df. \
    coalesce(1). \
    write. \
    csv(
        f'/FileStore/tables/dept.csv', 
        mode='overwrite', 
        compression='gzip',
        header=True
    )

# COMMAND ----------

dbutils.fs.ls(f'/FileStore/tables/')

# COMMAND ----------

spark.read.csv(f'/FileStore/tables/dept.csv', header=True).show()

# COMMAND ----------

df.coalesce(1).write.mode('overwrite').csv(f'{output_dir}/emp', sep='|')

#orders = spark.read.schema(schema).csv(f'/user/{username}/retail_db_pipe/orders', sep='|')

# COMMAND ----------

emp_df. \
    coalesce(1). \
    write. \
    mode('overwrite'). \
    csv(f'/FileStore/tables/dept', sep='|', header=True, compression='gzip')

# COMMAND ----------

dept_df. \
    coalesce(1). \
    write. \
    mode('overwrite'). \
    option('compression', 'gzip'). \
    option('header', True). \
    option('sep', '|'). \
    csv(f'/FileStore/tables/dept/sample')

# COMMAND ----------

dept_df. \
    coalesce(1). \
    write. \
    mode('overwrite'). \
    options(sep='|', header=True, compression='gzip'). \
    csv(f'/FileStore/tables/dept.csv')

# COMMAND ----------

spark.read.csv(f'/FileStore/tables/dept.csv', sep='|', header=True, inferSchema=True).show()

# COMMAND ----------

# DBTITLE 1,COMPRESS-JSON
courses_df. \
    coalesce(1). \
    write. \
    json(
        f'/user/{username}/courses', 
        mode='overwrite',
        compression='gzip'
    )

# COMMAND ----------

courses_df. \
    coalesce(1). \
    write. \
    format('json'). \
    save(
        f'/user/{username}/courses', 
        mode='overwrite',
        compression='snappy'
    )

# COMMAND ----------

courses_df. \
    coalesce(1). \
    write. \
    format('parquet'). \
    save(
        f'/user/{username}/courses', 
        mode='overwrite'
    )

# COMMAND ----------

spark.conf.get('spark.sql.parquet.compression.codec')

# COMMAND ----------

spark.conf.set('spark.sql.parquet.compression.codec', 'none')

# COMMAND ----------

emp_df. \
    coalesce(1). \
    write. \
    parquet(
        f'/path', 
        mode='overwrite'
    )

# COMMAND ----------

dept_df. \
    coalesce(1). \
    write. \
    mode('append'). \
    parquet(f'path')

# COMMAND ----------

from pyspark.sql.functions import *

emp=[
    (7839,"KING", "PRESIDENT",1234,"1981-11-10",5000,100,10),
    (7698,"BLAKE", "MANAGER",7839,"1981-05-01",2850,500,30),
    (7782,"CLARK", "MANAGER",7839, "1981-06-09", 2450,7890 ,10),
    (7566, "JONES", "MANAGER",7839, "1981-04-01", 2975,987,20),
    (7788, "SCOTT",  "ANALYST",7566, "1990-04-07",3000,786,20),
    (7902, "FORD", "ANALYST",7566, "1981-03-12", 3000,765,20),
    (7369,"SMITH",  "CLERK",7902, "1980-12-01", 800,40,20),
    (7499, "ALLEN",  "SALESMAN", 7698, "1981-02-20",1600,300, 30),
    (7521, "WARD" ,  "SALESMAN", 7698, "1981-02-03",    1250,     500,    30),
    (7654, "MARTIN",    "SALESMAN",    7698, "1981-09-28", 1250 ,   1400,    30),
    (7844, "TURNER",    "SALESMAN",    7698, "1981-09-08",    1500,       0,    30),
    (7876, "ADAMS",    "CLERK",       7788, "1987-09-09",    1100,90,            20),
    (7900,"JAMES",     "CLERK",       7698, "1981-12-03",     950,90,            30),
    (7934, "MILLER",    "CLERK",       7782, "1982-01-01" ,   1300 ,80,           10)
]


emp_df = spark.createDataFrame(emp, schema="empno INTEGER, ename STRING,job STRING,mgr INTEGER,hiredate STRING,sal INTEGER,comm INTEGER,deptno INTEGER")


df_filter=emp_df.filter(col("deptno")==10)

df_add=df_filter.withColumn("city",lit("HYD"))


df_add.write.mode("overwrite").saveAsTable("spark.emp")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create schema spark;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show databases;

# COMMAND ----------

# MAGIC %sql
# MAGIC use spark;
# MAGIC show tables;
