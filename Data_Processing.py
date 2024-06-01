# Databricks notebook source
# MAGIC %md
# MAGIC ####Read input dataset - csv file.

# COMMAND ----------

df=spark.read.csv('dbfs:/FileStore/tables/projects/Baseball_Team.csv',header='True',inferSchema='True')

# COMMAND ----------

# MAGIC %md
# MAGIC ####Data Cleansing

# COMMAND ----------

# MAGIC %md
# MAGIC #####1.	Rename columns Height(inches) to Height and Weight(lbs) to Weight

# COMMAND ----------

from pyspark.sql.functions import *
import re

df1=df.withColumnRenamed('Height(inches)','Height').withColumnRenamed('Weight(lbs)','Weight')

#print data , uncomment to check.
#df1.show() 
#OR
#display(df1) 


# COMMAND ----------

# MAGIC %md
# MAGIC ######2.	Convert Height into feet and inches, example if height is 74 inches, then translate the same to 6’2” 

# COMMAND ----------

#3.	Convert Height into feet and inches, example if height is 74 inches, then translate the same to 6’2” 
df2 = df1.withColumn("feet", concat(floor(col("Height") / 12),lit("'"),col("Height") % 12,lit("''")))
df2.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ######3. Convert the names into upper case.

# COMMAND ----------

df3 = df2.toDF(*[col.upper() for col in df2.columns])
df3 = df3.withColumn("NAME", upper(col("NAME")))
#df3.select(x for x in col('NAME'))
df3.show()


# COMMAND ----------


##Dynamic way to change multiple column values to upper case

l=['NAME','POSITION']

def change_column(l,df3):
    for  i in range(len(l)):
        df3 = df3.withColumn(l[i], upper(col(l[i])))
    return df3    
df3=change_column(l,df3)
display(df3)
   

# COMMAND ----------

# MAGIC %md
# MAGIC #####4.	Eliminate any characters in name other than a-z/A-Z/ space /. (dot), example Ma@rk -> Mark

# COMMAND ----------


pattern = r"[^a-zA-Z0-9. ]"
df4 = df3.withColumn("NAME", regexp_replace(col("NAME"), pattern, ""))
#x=df5.filter(col('NAME').rlike(pattern)).count()
#
# print(x)

# COMMAND ----------

# MAGIC %md
# MAGIC #####5.	If the name is blank, then populate as “UnKnown’
# MAGIC

# COMMAND ----------


df5=df4.fillna("Unknown", subset=["NAME"])
#df4=df4.na.fill('Unknown',['NAME'])  ---same purpose
df5.filter(col('NAME')=='Unknown').show()

# COMMAND ----------

# MAGIC %md
# MAGIC #####6.	Add a column “PlayerID” and assign a row number/ unique identifier for each row to populate this column.
# MAGIC

# COMMAND ----------


df6 = df5.withColumn("PLAYERID", monotonically_increasing_id())
df6.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #####7.	Convert the age to nearest integer less then or equals to Age. Example 28.13 -> 28 

# COMMAND ----------


df7 = df6.withColumn("AGE",floor(col("AGE")))
df7.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Write back the data as Parquet format.
# MAGIC

# COMMAND ----------


df7.write.parquet("dbfs:/FileStore/cleansed_data.parquet")


# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/cleansed_data.parquet