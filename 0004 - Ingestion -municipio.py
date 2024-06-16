# Databricks notebook source
# MAGIC %md 
# MAGIC ### Leitura do arquivo taxa_evolucao_mensal_municipio.csv

# COMMAND ----------

csv = spark.read.format("csv").option("header", "true").option("sep", ";").option("encoding", "UTF-8").load(f'/mnt/raw/RELATORIO_DTB_BRASIL_MUNICIPIO.csv').createOrReplaceTempView('Municipios_Brasil')

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Data preparation

# COMMAND ----------

query = spark.sql(f'''
                  Select
                    UF                                      as UF,
                    Nome_UF                                 as Nome_UF,
                    `Região Geográfica Intermediária`       as Regiao_Geografica_Intermediaria,
                    `Nome Região Geográfica Intermediária`  as Nome_Regiao_Geografica_Intermediaria,
                    `Região Geográfica Imediata`            as Regiao_Geografica_Imediata,
                    `Nome Região Geográfica Imediata`       as Nome_Regiao_Geografica_Imediata,
                    `Mesorregião Geográfica`                as MesoRegiao_Geografica,
                    `Nome_Mesorregião`                      as Nome_MesoRegiao,
                    `Microrregião Geográfica`               as MicroRegiao_Geografica,
                    `Nome_Microrregião`                     as Nome_MicroRegiao,
                    `Município`                             as Codigo_Municipio,
                    `Código Município Completo`             as Id_Municipio,
                    `Nome_Município`                        as Nome_Municipio,
                    current_Date()                          as Dt_Carga
                  From Municipios_Brasil
                  Where 1=1
                ''')


# COMMAND ----------

# MAGIC %md 
# MAGIC ### Salvando os dados na camada bronze

# COMMAND ----------

path_to_write = '/mnt/bronze/criminal/Tb_Municipios_Brasil'
query.write.mode("overwrite").option("mergeSchema", "true").option("path", path_to_write).saveAsTable(f"criminal_bronze.Tb_Municipios_Brasil")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select *
# MAGIC From Delta.`/mnt/bronze/criminal/Tb_Municipios_Brasil`

# COMMAND ----------


