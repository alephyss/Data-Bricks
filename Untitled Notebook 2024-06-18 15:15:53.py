# Databricks notebook source
# MAGIC %fs ls
# MAGIC

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/images/advanced-data-engineering-with-databricks-3.4.1/
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %fs ls dbfs:/local_disk0/tmp/
# MAGIC

# COMMAND ----------

# MAGIC %fs ls
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC %fs ls dbfs:/databricks-datasets/

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/bikeSharing/

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/bikeSharing/data-001/
# MAGIC

# COMMAND ----------

df=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/databricks-datasets/bikeSharing/data-001/hour.csv")

# COMMAND ----------

df.show()


# COMMAND ----------

display(df)
df.printSchema()

# COMMAND ----------

df.select(col("instant"),col("dteday"),"season").show()


# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df.select(df.instant,col("dteday"),"season").show()

# COMMAND ----------

df.select(col("season").alias("seasons")).show()

# COMMAND ----------

display(df)


# COMMAND ----------

 df.take(3)

# COMMAND ----------

df.tail(4)

# COMMAND ----------

display(df.describe())

# COMMAND ----------

df.describe(['yr']).show()

# COMMAND ----------

display(df)

# COMMAND ----------

print("vamsi")
