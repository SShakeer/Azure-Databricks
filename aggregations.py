# Databricks notebook source
# MAGIC %run "/Users/basha.ora11@gmail.com/Batch4/data"

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC emp_df.createOrReplaceTempView("emp")

# COMMAND ----------

# MAGIC %sql
# MAGIC select deptno,count(*) as total from emp
# MAGIC group by deptno
# MAGIC order by deptno desc;

# COMMAND ----------

from pyspark.sql.functions import count
emp_df.select(count("*")).show()

# COMMAND ----------

emp_df.groupBy('deptno').max("sal").show()

# COMMAND ----------

emp_df.count()

# COMMAND ----------

emp_grouped = emp_df.groupBy('deptno')
type(emp_grouped)


# COMMAND ----------

emp_grouped.count().show()

# COMMAND ----------

emp_grouped. \
    count(). \
    withColumnRenamed('count', 'emp_count'). \
    show()

# COMMAND ----------

# Get sum of all numeric fields
emp_grouped. \
    sum(). \
    show()

# COMMAND ----------

# Consider only order_item_quantity and order_item_subtotal
emp_grouped. \
    sum('sal', 'comm'). \
    show()

# COMMAND ----------


from pyspark.sql.functions import *
emp_grouped. \
    sum('sal', 'comm'). \
    toDF('deptno', 'sal', 'total_sal'). \
    withColumn('revenue', round('total_sal', 2)). \
    show()

# COMMAND ----------

emp_grouped. \
    agg(sum('sal'), count('empno')). \
    printSchema()

# COMMAND ----------

emp_grouped. \
    agg(sum('sal').alias('order_quanity'), round(sum('comm'), 2).alias('total_revenue')). \
    printSchema()
