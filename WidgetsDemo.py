# Databricks notebook source
#dbutils.widgets.text("Dname", 'ACCOUNTING')

# COMMAND ----------

names = spark.sql("select distinct(dname) from dept100").rdd.map(lambda row : row[0]).collect()

# COMMAND ----------

print(names)

# COMMAND ----------

dbutils.widgets.dropdown("Dname", "ACCOUNTING", [str(x) for x in names])

# COMMAND ----------

df=spark.sql("select * from dept100")

# COMMAND ----------

abc=dbutils.widgets.get("Dname")
print(abc)

# COMMAND ----------

from pyspark.sql.functions import *
df.where(col("dname")==abc).show()

# COMMAND ----------

dbutils.widgets.removeAll()
