# Databricks notebook source
storageAccountKey='C4Dy1o2T2R8M56JWVUD7BddoBSHqtgsd72S5KOymsnI7ByGC+uhPS0mrEk5Pph1n6x3lK7NNowjk+AStnKN8KA=='
spark.conf.set("fs.azure.account.key.getsydemo.dfs.core.windows.net",storageAccountKey)

# COMMAND ----------

import pandas as pd

# COMMAND ----------

sourceFileURL = '/Workspace/Users/azuser3620_mml.local@techademy.com/myFolder/US.json'
df = pd.read_json(sourceFileURL)

HoildayCSVFilePath = 'abfss://working-labs@getsydemo.dfs.core.windows.net/bronze/us_public_holidays_2025/csv/us_public_holidays_2025.csv'



# COMMAND ----------

sourceFilePandasDF = pandas.read_csv(sourceFileURL)

# COMMAND ----------

# Copy the local file to ADLS path
dbutils.fs.cp("file:/tmp/us_public_holidays_2025.csv", "abfss://working-labs@getsydemo.dfs.core.windows.net/bronze/us_public_holidays_2025/csv/us_public_holidays_2025.csv")


# COMMAND ----------

HoildayCSVFilePath = "abfss://working-labs@getsydemo.dfs.core.windows.net/bronze/us_public_holidays_2025/csv/us_public_holidays_2025.csv"
df.to_csv('/tmp/us_public_holidays_2025.csv', index=False)# Then upload the file to ADLS using dbutils or other methods.




# COMMAND ----------

csv_path = "abfss://working-labs@getsydemo.dfs.core.windows.net/bronze/us_public_holidays_2025/csv/us_public_holidays_2025.csv"
df_spark = spark.read.option("header", "true").csv(csv_path)


# COMMAND ----------

df_spark.printSchema()


# COMMAND ----------

df_spark.show(5)

# COMMAND ----------

from pyspark.sql.functions import to_date, col
df_spark = df_spark.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

# COMMAND ----------

from pyspark.sql.functions import month, date_format

df_spark = df_spark.withColumn("hoilday_month", month(col("date"))) \
                   .withColumn("weekday_names", date_format(col("date"), "EEEE"))

display(df_spark)

# COMMAND ----------

df_spark.groupBy("weekday_names").count().orderBy("count", ascending=False).show(1)


# COMMAND ----------

Week_days_count = df_spark.groupBy("weekday_names").count().orderBy("count", ascending=False)
display(Week_days_count)

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import month

month_counts = df_spark.groupBy(month("date").alias("month")) \
                       .count() \
                       .filter("count > 2") \
                       .orderBy("month")

display(month_counts)


# COMMAND ----------

df_spark.write.mode("overwrite") \
        .csv("abfss://working-labs@getsydemo.dfs.core.windows.net/us_public_holidays_2025/csv")


df_spark.write.mode("overwrite") \
        .json("abfss://working-labs@getsydemo.dfs.core.windows.net/us_public_holidays_2025/json")


df_spark.write.mode("overwrite") \
        .parquet("abfss://working-labs@getsydemo.dfs.core.windows.net/us_public_holidays_2025/parquet")
