# Databricks notebook source
# MAGIC %md
# MAGIC ####Create table using parquet file

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE basketball_silver
# MAGIC USING parquet
# MAGIC OPTIONS (path "/FileStore/cleansed_data.parquet")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from basketball_silver 

# COMMAND ----------

# MAGIC %md
# MAGIC #####1. View most aged players from each team

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (
# MAGIC   select name, team, age,row_number() over (partition by team order by age desc) as rnum
# MAGIC from basketball_silver)
# MAGIC select name,team,age from cte
# MAGIC where rnum=1

# COMMAND ----------

# MAGIC %md
# MAGIC #####2. Display player from each team with height greater than 6 feet.

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (
# MAGIC   select name, team, height,feet, row_number() over (partition by team order by height desc) as rnum
# MAGIC from basketball_silver where height>72)
# MAGIC select name,team,height,feet from cte
# MAGIC where rnum=1