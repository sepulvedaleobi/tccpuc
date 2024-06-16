# Databricks notebook source
# MAGIC %md 
# MAGIC ### Leitura do arquivo taxa_evolucao_mensal_municipio.csv

# COMMAND ----------

result = spark.sql(f"""Delete From criminal_gold.Dim_Municipios_Brasil
                        Where 1=1
                    """
                  )

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Data preparation

# COMMAND ----------

query = spark.sql(f'''
                  Select
                   UF,
                   Nome_UF,
                   Regiao_Geografica_Intermediaria,
                   Nome_Regiao_Geografica_Intermediaria,
                   Regiao_Geografica_Imediata,
                   Nome_Regiao_Geografica_Imediata,
                   MesoRegiao_Geografica,
                   Nome_MesoRegiao,
                   MicroRegiao_Geografica,
                   Nome_MicroRegiao,
                   Codigo_Municipio,
                   Id_Municipio,
                   Nome_Municipio,
                   Dt_Carga
                  From criminal_bronze.Tb_Municipios_Brasil
                  Where 1=1
                ''')


# COMMAND ----------

# MAGIC %md 
# MAGIC ### Salvando os dados na camada bronze

# COMMAND ----------

path_to_write = '/mnt/gold/criminal/Dim_Municipios_Brasil'
query.write.mode("overwrite").option("mergeSchema", "true").option("path", path_to_write).saveAsTable(f"criminal_gold.Dim_Municipios_Brasil")

# COMMAND ----------


