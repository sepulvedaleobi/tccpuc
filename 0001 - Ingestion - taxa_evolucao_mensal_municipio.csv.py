# Databricks notebook source
# MAGIC %md 
# MAGIC ### Leitura do arquivo taxa_evolucao_mensal_municipio.csv

# COMMAND ----------

csv = spark.read.format("csv").option("header", "true").option("sep", ",").option("encoding", "ISO-8859-1").load(f'/mnt/raw/taxa_evolucao_mensal_municipio.csv').createOrReplaceTempView('taxa_evolucao_mensal_municipio')

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Variaves de controle de carga

# COMMAND ----------

from datetime import datetime, timedelta

current_date = datetime.now()
max_yearmonth_date = current_date
max_yearmonth = max_yearmonth_date.strftime('%Y%m')

min_yearmonth_date = current_date - timedelta(days=current_date.day - 1) - timedelta(weeks=12)
min_yearmonth = min_yearmonth_date.strftime('%Y%m')


# COMMAND ----------

# MAGIC %md 
# MAGIC ### Data preparation

# COMMAND ----------

query = spark.sql(f'''
                    Select 
                    *,
                    current_Date() as dt_carga
                    From taxa_evolucao_mensal_municipio
                    Where 1=1
                      -- and Concat(ano, LPAD(mes, 2, '0')) >= {min_yearmonth} Liberar quando houver mais dados na base
                      -- and Concat(ano, LPAD(mes, 2, '0')) <= {max_yearmonth} Liberar quando houver mais dados na base
                ''')


# COMMAND ----------

# MAGIC %md 
# MAGIC ### Salvando os dados na camada bronze

# COMMAND ----------

path_to_write = '/mnt/bronze/criminal/Tb_taxa_evolucao_mensal_municipio'
query.write.mode("overwrite").option("mergeSchema", "true").option("path", path_to_write).saveAsTable(f"criminal_bronze.Tb_taxa_evolucao_mensal_municipio")  
