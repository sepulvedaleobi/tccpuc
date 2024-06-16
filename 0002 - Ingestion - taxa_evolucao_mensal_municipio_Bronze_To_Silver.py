# Databricks notebook source
# MAGIC %md 
# MAGIC ### Variaves de controle de carga

# COMMAND ----------

from pyspark.sql import SparkSession
from datetime import datetime, timedelta

# COMMAND ----------


max_yearmonth = spark.sql("""
                        Select Distinct Max(Concat(ano, LPAD(mes, 2, '0'))) as Year_Month
                        From criminal_bronze.Tb_taxa_evolucao_mensal_municipio
                     """).collect()[0][0]

min_yearmonth = spark.sql('''Select Min(Concat(ano, LPAD(mes, 2, '0'))) as Year_Month
                            From criminal_bronze.Tb_taxa_evolucao_mensal_municipio
                        ''').collect()[0][0]


# COMMAND ----------

# MAGIC %md 
# MAGIC ### Limpeza do range de dado que será carregado

# COMMAND ----------

result = spark.sql(f"""Delete From criminal_silver.Tb_taxa_evolucao_mensal_municipio
                        Where 1=1
                          and CONCAT(ano, LPAD(mes, 2, '0')) >= {min_yearmonth}
                          and CONCAT(ano, LPAD(mes, 2, '0')) <= {max_yearmonth}
                    """
                  )

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Data preparation

# COMMAND ----------

query = spark.sql(f'''
                    Select 
                      ano,
                      mes,
                      id_municipio,
                      regiao,
                      taxa_homicidio_doloso,
                      taxa_latrocinio,
                      taxa_lesao_corporal_morte,
                      taxa_crimes_violentos_letais_intencionais,
                      taxa_homicidio_intervencao_policial,
                      taxa_letalidade_violenta,
                      taxa_tentativa_homicidio,
                      taxa_lesao_corporal_dolosa,
                      taxa_estupro,
                      taxa_homicidio_culposo,
                      taxa_lesao_corporal_culposa,
                      taxa_roubo_transeunte,
                      taxa_roubo_celular,
                      taxa_roubo_corporal_coletivo,
                      taxa_roubo_rua,
                      taxa_roubo_veiculo,
                      taxa_roubo_carga,
                      taxa_roubo_comercio,
                      taxa_roubo_residencia,
                      taxa_roubo_banco,
                      taxa_roubo_caixa_eletronico,
                      taxa_roubo_conducao_saque,
                      taxa_roubo_apos_saque,
                      taxa_roubo_bicicleta,
                      taxa_outros_roubos,
                      taxa_total_roubos,
                      taxa_furto_veiculos,
                      taxa_furto_transeunte,
                      taxa_furto_coletivo,
                      taxa_furto_celular,
                      taxa_furto_bicicleta,
                      taxa_outros_furtos,
                      taxa_total_furtos,
                      taxa_sequestro,
                      taxa_extorsao,
                      taxa_sequestro_relampago,
                      taxa_estelionato,
                      taxa_apreensao_drogas,
                      taxa_registro_posse_drogas,
                      taxa_registro_trafico_drogas,
                      taxa_registro_apreensao_drogas_sem_autor,
                      taxa_registro_veiculo_recuperado,
                      taxa_apf,
                      taxa_aaapai,
                      taxa_cmp,
                      taxa_cmba,
                      taxa_ameaca,
                      taxa_pessoas_desaparecidas,
                      taxa_encontro_cadaver,
                      taxa_encontro_ossada,
                      taxa_policial_militar_morto_servico,
                      taxa_policial_civil_morto_servico,
                      taxa_registro_ocorrencia,
                      tipo_fase,
                      current_Date() as dt_carga
                    From criminal_bronze.Tb_taxa_evolucao_mensal_municipio
                        Where 1=1
                          and CONCAT(ano, LPAD(mes, 2, '0')) >= '{min_yearmonth}' 
                          and CONCAT(ano, LPAD(mes, 2, '0')) <= '{max_yearmonth}'
                ''')


# COMMAND ----------

# MAGIC %md 
# MAGIC ### Salvando os dados na camada bronze

# COMMAND ----------

path_to_write = '/mnt/silver/criminal/Tb_taxa_evolucao_mensal_municipio'
query.write.mode("append").option("mergeSchema", "true").option("path", path_to_write).saveAsTable(f"criminal_silver.Tb_taxa_evolucao_mensal_municipio")  

# COMMAND ----------


