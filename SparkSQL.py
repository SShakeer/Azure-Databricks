# Databricks notebook source
# MAGIC %run "/Users/basha.ora11@gmail.com/Batch4/data"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE students (
# MAGIC     student_id INT,
# MAGIC     student_first_name STRING,
# MAGIC     student_last_name STRING,
# MAGIC     student_phone_number STRING,
# MAGIC     student_address STRING
# MAGIC ) STORED AS TEXTFILE;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE students (
# MAGIC     student_id INT,
# MAGIC     student_first_name STRING,
# MAGIC     student_last_name STRING,
# MAGIC     student_phone_numbers ARRAY<STRING>,
# MAGIC     student_address STRUCT<street:STRING, city:STRING, state:STRING, zip:STRING>
# MAGIC ) STORED AS TEXTFILE
# MAGIC ROW FORMAT
# MAGIC     DELIMITED FIELDS TERMINATED BY '\t'
# MAGIC     COLLECTION ITEMS TERMINATED BY ',';
# MAGIC     

# COMMAND ----------

INSERT INTO students VALUES 
    (3, 'Mickey', 'Mouse', ARRAY('1234567890', '2345678901'), STRUCT('A Street', 'One City', 'Some State', '12345')),
    (4, 'Bubble', 'Guppy', ARRAY('5678901234', '6789012345'), STRUCT('Bubbly Street', 'Guppy', 'La la land', '45678'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT empno, deptno, sal,hiredate,
# MAGIC     count(1) OVER (PARTITION BY deptno) AS employee_count,
# MAGIC     rank() OVER (ORDER BY sal DESC) AS rnk,
# MAGIC     lead(empno) OVER (PARTITION BY deptno ORDER BY sal DESC) AS lead_emp_id,
# MAGIC     lead(sal) OVER (PARTITION BY deptno ORDER BY sal DESC) AS lead_emp_sal
# MAGIC FROM emp
# MAGIC ORDER BY empno;

# COMMAND ----------

emp_df.createOrReplaceTempView("emp")

dept_df.createOrReplaceTempView("dept")


# COMMAND ----------

# MAGIC %python
# MAGIC df20=spark.sql("SELECT empno, deptno, sal,hiredate,count(1) OVER (PARTITION BY deptno) AS employee_count,rank() OVER (ORDER BY sal DESC) AS rnk,lead(empno) OVER (PARTITION BY deptno ORDER BY sal DESC) AS lead_emp_id,lead(sal) OVER (PARTITION BY deptno ORDER BY sal DESC) AS lead_emp_sal FROM emp ORDER BY empno")

# COMMAND ----------

df20.coalesce(1).write.format("parquet").save("/mnt/data/emp.parquet")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT deptno,
# MAGIC        sum(sal) AS department_sal
# MAGIC FROM emp
# MAGIC GROUP BY deptno
# MAGIC ORDER BY deptno

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC df_sum=emp_df.groupBy("deptno").sum("sal").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT e.empno, e.deptno, e.sal,
# MAGIC        ae.department_salary,
# MAGIC        ae.avg_salary
# MAGIC FROM emp e JOIN (
# MAGIC      SELECT deptno, 
# MAGIC             sum(sal) AS department_salary,
# MAGIC             avg(sal) AS avg_salary
# MAGIC      FROM emp
# MAGIC      GROUP BY deptno
# MAGIC ) ae
# MAGIC ON e.deptno = ae.deptno
# MAGIC ORDER BY deptno, sal;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC e.empno, 
# MAGIC e.deptno, 
# MAGIC e.sal,
# MAGIC sum(e.sal) OVER (PARTITION BY e.deptno) AS department_salary
# MAGIC FROM emp e
# MAGIC ORDER BY e.deptno

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT e.empno, 
# MAGIC e.deptno, 
# MAGIC e.sal,
# MAGIC     sum(e.sal) OVER (PARTITION BY e.deptno) AS sum_sal_expense,
# MAGIC     avg(e.sal) OVER (PARTITION BY e.deptno) AS avg_sal_expense,
# MAGIC     min(e.sal) OVER (PARTITION BY e.deptno) AS min_sal_expense,
# MAGIC     max(e.sal) OVER (PARTITION BY e.deptno) AS max_sal_expense,
# MAGIC     count(e.sal) OVER (PARTITION BY e.deptno) AS cnt_sal_expense
# MAGIC FROM emp e
# MAGIC ORDER BY e.deptno

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT t.*,
# MAGIC   lead(hiredate) OVER (ORDER BY hiredate DESC) AS prior_date,
# MAGIC   lead(sal) OVER (ORDER BY sal DESC) AS prior_revenue,
# MAGIC   lag(hiredate) OVER (ORDER BY hiredate) AS lag_prior_date,
# MAGIC   lag(sal) OVER (ORDER BY sal) AS lag_prior_revenue
# MAGIC FROM emp AS t
# MAGIC ORDER BY hiredate DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT t.*,
# MAGIC   LEAD(sal) OVER (
# MAGIC     PARTITION BY deptno 
# MAGIC     ORDER BY sal DESC
# MAGIC   ) next_product_id,
# MAGIC   LEAD(sal) OVER (
# MAGIC     PARTITION BY deptno 
# MAGIC     ORDER BY sal DESC
# MAGIC   ) next_revenue
# MAGIC FROM emp t
# MAGIC ORDER BY deptno, sal DESC
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   empno,
# MAGIC   deptno,
# MAGIC   sal,
# MAGIC   rank() OVER (
# MAGIC     PARTITION BY deptno
# MAGIC     ORDER BY sal DESC
# MAGIC   ) rnk,
# MAGIC   dense_rank() OVER (
# MAGIC     PARTITION BY deptno
# MAGIC     ORDER BY sal DESC
# MAGIC   ) drnk,
# MAGIC   row_number() OVER (
# MAGIC     PARTITION BY deptno
# MAGIC     ORDER BY sal DESC
# MAGIC   ) rn
# MAGIC FROM emp
# MAGIC ORDER BY deptno, sal DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM (
# MAGIC     SELECT deptno, count(1) AS emp_count
# MAGIC     FROM emp
# MAGIC     GROUP BY deptno
# MAGIC ) q
# MAGIC WHERE q.emp_count > 4

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT e.empno, e.deptno, e.sal,
# MAGIC     sum(e.sal) OVER (
# MAGIC         PARTITION BY e.deptno
# MAGIC         ORDER BY e.sal DESC
# MAGIC         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC     ) AS sum_sal_expense
# MAGIC FROM emp e
# MAGIC ORDER BY e.deptno, e.sal DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT t.*,
# MAGIC     round(sum(t.sal) OVER (
# MAGIC         PARTITION BY date_format(hiredate, 'yyyy-MM')
# MAGIC         ORDER BY hiredate
# MAGIC         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC     ), 2) AS cumulative_daily_revenue
# MAGIC FROM emp t
# MAGIC ORDER BY date_format(hiredate, 'yyyy-MM'),
# MAGIC     hiredate

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_date AS current_date

# COMMAND ----------

SELECT split(current_date, '-')[1] AS month
SELECT cast(split(current_date, '-')[1] AS INT) AS month;

SELECT
    round(10.44) rnd1,
    round(10.44, 1) rnd1,
    round(10.46, 1) rnd2,
    floor(10.44) flr,
    ceil(10.44) cl;
    

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT deptno, 
# MAGIC     round(sum(sal), 2) AS revenue_sum,
# MAGIC     min(sal) AS subtotal_min,
# MAGIC     max(sal) AS subtotal_max 
# MAGIC FROM emp
# MAGIC GROUP BY deptno
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT o.*,
# MAGIC     CASE WHEN job IN ('CLERK', 'MANAGER') THEN 'ABC'
# MAGIC     END AS status
# MAGIC FROM emp o
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT ename,
# MAGIC     CASE 
# MAGIC         WHEN job IN ('PRESIDENT') THEN 'Excelent'
# MAGIC         WHEN job LIKE '%CLERK%' OR deptno IN (10,20)
# MAGIC             THEN 'Average'
# MAGIC         ELSE 'Poor'
# MAGIC     END AS status
# MAGIC FROM emp;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT current_timestamp AS current_timestamp

# COMMAND ----------

date_add- can be used to add or subtract days.
date_sub-- can be used to subtract or add days.
datediff -can be used to get difference between 2 dates
add_months -can be used add months to a date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --SELECT date_add(current_date, 32) AS result;
# MAGIC SELECT date_add('2018-04-15', 730) AS result;
# MAGIC --SELECT date_add('2018-04-15', -730) AS result;
# MAGIC --SELECT date_sub(current_date, 30) AS result;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT datediff('2019-03-30', '2017-12-31') AS result
# MAGIC
# MAGIC SELECT datediff('2017-12-31', '2019-03-30') AS result

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT add_months('2019-01-31', 1) AS result;
# MAGIC SELECT add_months(current_timestamp, 3) AS result

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_timestamp AS current_timestamp, 
# MAGIC     date_format(current_timestamp, 'yyyy') AS year;
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_timestamp AS current_timestamp, 
# MAGIC     date_format(current_timestamp, 'yy') AS year

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC SELECT current_timestamp AS current_timestamp, 
# MAGIC     date_format(current_timestamp, 'MM') AS month

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_timestamp AS current_timestamp, 
# MAGIC     date_format(current_timestamp, 'dd') AS day_of_month

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_timestamp AS current_timestamp, 
# MAGIC     date_format(current_timestamp, 'DD') AS day_of_year

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_timestamp AS current_timestamp, 
# MAGIC     date_format(current_timestamp, 'MMM') AS month_name

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_timestamp AS current_timestamp, 
# MAGIC     date_format(current_timestamp, 'MMMM') AS month_name

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_timestamp AS current_timestamp, 
# MAGIC     date_format(current_timestamp, 'HH') AS hour24

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_timestamp AS current_timestamp, 
# MAGIC     date_format(current_timestamp, 'hh') AS hour12

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_timestamp AS current_timestamp, 
# MAGIC     date_format(current_timestamp, 'mm') AS minutes

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT current_timestamp AS current_timestamp, 
# MAGIC     date_format(current_timestamp, 'ss') AS seconds

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_timestamp AS current_timestamp, 
# MAGIC     date_format(current_timestamp, 'SS') AS millis

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date_format(current_timestamp, 'yyyyMM') AS current_month;
# MAGIC SELECT date_format(current_timestamp, 'yyyyMMdd') AS current_date;
# MAGIC SELECT date_format(current_timestamp, 'yyyy/MM/dd') AS current_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT year(current_date) AS year;
# MAGIC SELECT month(current_date) AS month;
# MAGIC SELECT weekofyear(current_date) AS weekofyear;

# COMMAND ----------

# DBTITLE 1,Dealing with Unix Timestamp
Dealing with Unix Timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT from_unixtime(1556662731) AS timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT to_unix_timestamp('2019-04-30 18:18:51') AS unixtime

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT from_unixtime(1556662731, 'yyyy-MM-dd') AS date

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT from_unixtime(1556662731, 'yyyy-MM-dd HH:mm') AS timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT from_unixtime(1556662731, 'yyyy-MM-dd hh:mm') AS timestamp
