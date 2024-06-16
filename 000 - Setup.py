# Databricks notebook source
# MAGIC %md 
# MAGIC #### Estruturação dos DataBase HIVE para cada camada

# COMMAND ----------

# MAGIC %sql
# MAGIC Create DataBase if not Exists criminal_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC Create DataBase if not Exists criminal_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC Create DataBase if not Exists criminal_gold

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Criação das tabelas camada Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE criminal_bronze.Tb_taxa_evolucao_mensal_municipio (
# MAGIC   ano STRING,
# MAGIC   mes STRING,
# MAGIC   id_municipio STRING,
# MAGIC   regiao STRING,
# MAGIC   taxa_homicidio_doloso STRING,
# MAGIC   taxa_latrocinio STRING,
# MAGIC   taxa_lesao_corporal_morte STRING,
# MAGIC   taxa_crimes_violentos_letais_intencionais STRING,
# MAGIC   taxa_homicidio_intervencao_policial STRING,
# MAGIC   taxa_letalidade_violenta STRING,
# MAGIC   taxa_tentativa_homicidio STRING,
# MAGIC   taxa_lesao_corporal_dolosa STRING,
# MAGIC   taxa_estupro STRING,
# MAGIC   taxa_homicidio_culposo STRING,
# MAGIC   taxa_lesao_corporal_culposa STRING,
# MAGIC   taxa_roubo_transeunte STRING,
# MAGIC   taxa_roubo_celular STRING,
# MAGIC   taxa_roubo_corporal_coletivo STRING,
# MAGIC   taxa_roubo_rua STRING,
# MAGIC   taxa_roubo_veiculo STRING,
# MAGIC   taxa_roubo_carga STRING,
# MAGIC   taxa_roubo_comercio STRING,
# MAGIC   taxa_roubo_residencia STRING,
# MAGIC   taxa_roubo_banco STRING,
# MAGIC   taxa_roubo_caixa_eletronico STRING,
# MAGIC   taxa_roubo_conducao_saque STRING,
# MAGIC   taxa_roubo_apos_saque STRING,
# MAGIC   taxa_roubo_bicicleta STRING,
# MAGIC   taxa_outros_roubos STRING,
# MAGIC   taxa_total_roubos STRING,
# MAGIC   taxa_furto_veiculos STRING,
# MAGIC   taxa_furto_transeunte STRING,
# MAGIC   taxa_furto_coletivo STRING,
# MAGIC   taxa_furto_celular STRING,
# MAGIC   taxa_furto_bicicleta STRING,
# MAGIC   taxa_outros_furtos STRING,
# MAGIC   taxa_total_furtos STRING,
# MAGIC   taxa_sequestro STRING,
# MAGIC   taxa_extorsao STRING,
# MAGIC   taxa_sequestro_relampago STRING,
# MAGIC   taxa_estelionato STRING,
# MAGIC   taxa_apreensao_drogas STRING,
# MAGIC   taxa_registro_posse_drogas STRING,
# MAGIC   taxa_registro_trafico_drogas STRING,
# MAGIC   taxa_registro_apreensao_drogas_sem_autor STRING,
# MAGIC   taxa_registro_veiculo_recuperado STRING,
# MAGIC   taxa_apf STRING,
# MAGIC   taxa_aaapai STRING,
# MAGIC   taxa_cmp STRING,
# MAGIC   taxa_cmba STRING,
# MAGIC   taxa_ameaca STRING,
# MAGIC   taxa_pessoas_desaparecidas STRING,
# MAGIC   taxa_encontro_cadaver STRING,
# MAGIC   taxa_encontro_ossada STRING,
# MAGIC   taxa_policial_militar_morto_servico STRING,
# MAGIC   taxa_policial_civil_morto_servico STRING,
# MAGIC   taxa_registro_ocorrencia STRING,
# MAGIC   tipo_fase STRING,
# MAGIC   dt_carga DATE)
# MAGIC USING delta
# MAGIC LOCATION 'dbfs:/mnt/bronze/criminal/Tb_taxa_evolucao_mensal_municipio'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '2')
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE criminal_bronze.tb_municipios_brasil (
# MAGIC   UF STRING,
# MAGIC   Nome_UF STRING,
# MAGIC   Regiao_Geografica_Intermediaria STRING,
# MAGIC   Nome_Regiao_Geografica_Intermediaria STRING,
# MAGIC   Regiao_Geografica_Imediata STRING,
# MAGIC   Nome_Regiao_Geografica_Imediata STRING,
# MAGIC   MesoRegiao_Geografica STRING,
# MAGIC   Nome_MesoRegiao STRING,
# MAGIC   MicroRegiao_Geografica STRING,
# MAGIC   Nome_MicroRegiao STRING,
# MAGIC   Codigo_Municipio STRING,
# MAGIC   Id_Municipio STRING,
# MAGIC   Nome_Municipio STRING,
# MAGIC   Dt_Carga DATE)
# MAGIC USING delta
# MAGIC LOCATION 'dbfs:/mnt/bronze/criminal/Tb_Municipios_Brasil'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '2')
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Criação das tabelas camada Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE criminal_silver.Tb_taxa_evolucao_mensal_municipio (
# MAGIC   ano STRING,
# MAGIC   mes STRING,
# MAGIC   id_municipio STRING,
# MAGIC   regiao STRING,
# MAGIC   taxa_homicidio_doloso STRING,
# MAGIC   taxa_latrocinio STRING,
# MAGIC   taxa_lesao_corporal_morte STRING,
# MAGIC   taxa_crimes_violentos_letais_intencionais STRING,
# MAGIC   taxa_homicidio_intervencao_policial STRING,
# MAGIC   taxa_letalidade_violenta STRING,
# MAGIC   taxa_tentativa_homicidio STRING,
# MAGIC   taxa_lesao_corporal_dolosa STRING,
# MAGIC   taxa_estupro STRING,
# MAGIC   taxa_homicidio_culposo STRING,
# MAGIC   taxa_lesao_corporal_culposa STRING,
# MAGIC   taxa_roubo_transeunte STRING,
# MAGIC   taxa_roubo_celular STRING,
# MAGIC   taxa_roubo_corporal_coletivo STRING,
# MAGIC   taxa_roubo_rua STRING,
# MAGIC   taxa_roubo_veiculo STRING,
# MAGIC   taxa_roubo_carga STRING,
# MAGIC   taxa_roubo_comercio STRING,
# MAGIC   taxa_roubo_residencia STRING,
# MAGIC   taxa_roubo_banco STRING,
# MAGIC   taxa_roubo_caixa_eletronico STRING,
# MAGIC   taxa_roubo_conducao_saque STRING,
# MAGIC   taxa_roubo_apos_saque STRING,
# MAGIC   taxa_roubo_bicicleta STRING,
# MAGIC   taxa_outros_roubos STRING,
# MAGIC   taxa_total_roubos STRING,
# MAGIC   taxa_furto_veiculos STRING,
# MAGIC   taxa_furto_transeunte STRING,
# MAGIC   taxa_furto_coletivo STRING,
# MAGIC   taxa_furto_celular STRING,
# MAGIC   taxa_furto_bicicleta STRING,
# MAGIC   taxa_outros_furtos STRING,
# MAGIC   taxa_total_furtos STRING,
# MAGIC   taxa_sequestro STRING,
# MAGIC   taxa_extorsao STRING,
# MAGIC   taxa_sequestro_relampago STRING,
# MAGIC   taxa_estelionato STRING,
# MAGIC   taxa_apreensao_drogas STRING,
# MAGIC   taxa_registro_posse_drogas STRING,
# MAGIC   taxa_registro_trafico_drogas STRING,
# MAGIC   taxa_registro_apreensao_drogas_sem_autor STRING,
# MAGIC   taxa_registro_veiculo_recuperado STRING,
# MAGIC   taxa_apf STRING,
# MAGIC   taxa_aaapai STRING,
# MAGIC   taxa_cmp STRING,
# MAGIC   taxa_cmba STRING,
# MAGIC   taxa_ameaca STRING,
# MAGIC   taxa_pessoas_desaparecidas STRING,
# MAGIC   taxa_encontro_cadaver STRING,
# MAGIC   taxa_encontro_ossada STRING,
# MAGIC   taxa_policial_militar_morto_servico STRING,
# MAGIC   taxa_policial_civil_morto_servico STRING,
# MAGIC   taxa_registro_ocorrencia STRING,
# MAGIC   tipo_fase STRING,
# MAGIC   dt_carga DATE)
# MAGIC USING delta
# MAGIC LOCATION 'dbfs:/mnt/silver/criminal/Tb_taxa_evolucao_mensal_municipio'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '2')
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Criação das tabelas camada Gold

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE criminal_gold.Fact_taxa_evolucao_mensal_municipio (
# MAGIC   ano STRING,
# MAGIC   mes STRING,
# MAGIC   id_municipio STRING,
# MAGIC   taxa_homicidio_doloso STRING,
# MAGIC   taxa_latrocinio STRING,
# MAGIC   taxa_lesao_corporal_morte STRING,
# MAGIC   taxa_crimes_violentos_letais_intencionais STRING,
# MAGIC   taxa_homicidio_intervencao_policial STRING,
# MAGIC   taxa_letalidade_violenta STRING,
# MAGIC   taxa_tentativa_homicidio STRING,
# MAGIC   taxa_lesao_corporal_dolosa STRING,
# MAGIC   taxa_estupro STRING,
# MAGIC   taxa_homicidio_culposo STRING,
# MAGIC   taxa_lesao_corporal_culposa STRING,
# MAGIC   taxa_roubo_transeunte STRING,
# MAGIC   taxa_roubo_celular STRING,
# MAGIC   taxa_roubo_corporal_coletivo STRING,
# MAGIC   taxa_roubo_rua STRING,
# MAGIC   taxa_roubo_veiculo STRING,
# MAGIC   taxa_roubo_carga STRING,
# MAGIC   taxa_roubo_comercio STRING,
# MAGIC   taxa_roubo_residencia STRING,
# MAGIC   taxa_roubo_banco STRING,
# MAGIC   taxa_roubo_caixa_eletronico STRING,
# MAGIC   taxa_roubo_conducao_saque STRING,
# MAGIC   taxa_roubo_apos_saque STRING,
# MAGIC   taxa_roubo_bicicleta STRING,
# MAGIC   taxa_outros_roubos STRING,
# MAGIC   taxa_total_roubos STRING,
# MAGIC   taxa_furto_veiculos STRING,
# MAGIC   taxa_furto_transeunte STRING,
# MAGIC   taxa_furto_coletivo STRING,
# MAGIC   taxa_furto_celular STRING,
# MAGIC   taxa_furto_bicicleta STRING,
# MAGIC   taxa_outros_furtos STRING,
# MAGIC   taxa_total_furtos STRING,
# MAGIC   taxa_sequestro STRING,
# MAGIC   taxa_extorsao STRING,
# MAGIC   taxa_sequestro_relampago STRING,
# MAGIC   taxa_estelionato STRING,
# MAGIC   taxa_apreensao_drogas STRING,
# MAGIC   taxa_registro_posse_drogas STRING,
# MAGIC   taxa_registro_trafico_drogas STRING,
# MAGIC   taxa_registro_apreensao_drogas_sem_autor STRING,
# MAGIC   taxa_registro_veiculo_recuperado STRING,
# MAGIC   taxa_apf STRING,
# MAGIC   taxa_aaapai STRING,
# MAGIC   taxa_cmp STRING,
# MAGIC   taxa_cmba STRING,
# MAGIC   taxa_ameaca STRING,
# MAGIC   taxa_pessoas_desaparecidas STRING,
# MAGIC   taxa_encontro_cadaver STRING,
# MAGIC   taxa_encontro_ossada STRING,
# MAGIC   taxa_policial_militar_morto_servico STRING,
# MAGIC   taxa_policial_civil_morto_servico STRING,
# MAGIC   taxa_registro_ocorrencia STRING,
# MAGIC   tipo_fase STRING,
# MAGIC   dt_carga DATE)
# MAGIC USING delta
# MAGIC LOCATION 'dbfs:/mnt/gold/criminal/Fact_taxa_evolucao_mensal_municipio'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '2')
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE criminal_gold.dim_municipios_brasil (
# MAGIC   UF STRING,
# MAGIC   Nome_UF STRING,
# MAGIC   Regiao_Geografica_Intermediaria STRING,
# MAGIC   Nome_Regiao_Geografica_Intermediaria STRING,
# MAGIC   Regiao_Geografica_Imediata STRING,
# MAGIC   Nome_Regiao_Geografica_Imediata STRING,
# MAGIC   MesoRegiao_Geografica STRING,
# MAGIC   Nome_MesoRegiao STRING,
# MAGIC   MicroRegiao_Geografica STRING,
# MAGIC   Nome_MicroRegiao STRING,
# MAGIC   Codigo_Municipio STRING,
# MAGIC   Id_Municipio STRING,
# MAGIC   Nome_Municipio STRING,
# MAGIC   Dt_Carga DATE)
# MAGIC USING delta
# MAGIC LOCATION 'dbfs:/mnt/gold/criminal/Dim_Municipios_Brasil'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '2')
# MAGIC
