# Databricks notebook source
# MAGIC %md
# MAGIC # Componente Carga Silver & Gold

# COMMAND ----------

# DBTITLE 1,Libraries
import os
from delta.tables import *

# COMMAND ----------

dbutils.widgets.text("idprocesso", "", "idprocesso")
idprocesso = dbutils.widgets.get("idprocesso")
unitCatalogName = os.getenv(f"unitCatalogNamePrd")

print(idprocesso)

# COMMAND ----------

idprocesso_ajustado = idprocesso.strip()
dados_processo = spark.sql(f"""
    SELECT * 
    FROM {unitCatalogName}.0_par.processos
    WHERE idprocesso = cast({idprocesso_ajustado} as int)
    """).collect()[0].asDict()
print(dados_processo)

# COMMAND ----------

nomeAmbiente = dados_processo.get("nomeAmbiente")
tipoCarga =    str(dados_processo.get("tipoCarga"))
nomeProcesso = dados_processo.get("nomeProcesso")
nomeCamada =   dados_processo.get("nomeCamada")
nomeDatabaseDatabricks = dados_processo.get("nomeDatabaseDatabricks")
nomeTabelaDatabricks =   dados_processo.get("nomeTabelaDatabricks")
header =                 str(dados_processo.get("header"))
regraIncremental =       dados_processo.get("regraIncremental")
nomeNotebookDatabricks = dados_processo.get("nomeNotebookDatabricks")
PreCopyScript =  dados_processo.get("PreCopyScript")
PostCopyScript = dados_processo.get("PostCopyScript")
nomeOrigem =     dados_processo.get("nomeOrigem")
queryOrigem =    dados_processo.get("queryOrigem")

#Propriedades customizadas
conteiner = os.getenv("conteinerName")
storage = os.getenv("storageNamePrd")
unitCatalogName = os.getenv(f"unitCatalogName{nomeAmbiente.capitalize()}")
databricksCaminhoLanding = f"abfss://{conteiner}@{storage}.dfs.core.windows.net/landing/"
databricksCaminhoDelta = f"abfss://{conteiner}@{storage}.dfs.core.windows.net/externaldelta"

# COMMAND ----------

print(databricksCaminhoLanding)
print(databricksCaminhoDelta)

# COMMAND ----------

spark.sql(f'USE CATALOG {unitCatalogName}')
print(unitCatalogName)

# COMMAND ----------

# MAGIC %md
# MAGIC # Definição de Funções
# MAGIC

# COMMAND ----------

def delete_files(path, database_name, table_name):
    basepath = f"{path}/{database_name}/{table_name}"
    try:
        for filepath in dbutils.fs.ls(basepath):
            file = str(filepath).split('\'')[1]
            dbutils.fs.rm(file,True)
    except:
        print('caminho nao encontrado: ' + basepath)

def get_delta_parquet(read_path):
    df = spark \
      .read \
      .format('delta') \
      .parquet(read_path)
    return df
  
def carga_full(read_path,save_path,database_name,table_name,query):
    df = executeQuery(query)
    spark.sql("DROP TABLE IF EXISTS "+ database_name +'.' +table_name)
    df.write\
        .mode("overwrite")\
        .option("overwriteSchema",True)\
        .format("delta")\
        .option("path",f"{save_path}/{database_name}/{table_name}")\
        .saveAsTable(f"{database_name}.{table_name}")

def merge_data(existing_table_name, new_data, regraIncremental):
    existing_data = DeltaTable.forName(spark, existing_table_name)
    existing_data.alias('old') \
      .merge(
        new_data.alias('new'),
        str(regraIncremental)
      ) \
      .whenMatchedUpdateAll() \
      .whenNotMatchedInsertAll() \
      .execute()

def deleteInsert_data(existing_table_name, new_data, regraIncremental):
    spark.sql(regraIncremental)
    new_data.write.format("delta").mode("append").saveAsTable(existing_table_name)

def deleteWhereExists_Insert(existing_table_name, new_data, regraIncremental):
    existing_data = DeltaTable.forName(spark, existing_table_name)
    new_data.createOrReplaceTempView('tmp_table')  
    spark.sql("""
            delete
            from """ + existing_table_name + """ old 
            where exists (select 1 from tmp_table new where """ + regraIncremental + """)
    """)
    spark.sql("""
            insert into  """ + existing_table_name + """ 
            select * from tmp_table new
    """)
        
def executeSaveQuery(query, save_path, database_name, table_name):   
    df = spark.sql(query)
    df.write\
        .mode("overwrite")\
        .option("overwriteSchema",True)\
        .format("delta")\
        .option("path",f"{save_path}/{database_name}/{table_name}")\
        .saveAsTable(f"{database_name}.{table_name}")

def executeQuery(query):
    df = spark.sql(query)
    return df                    

# COMMAND ----------

# MAGIC %md
# MAGIC # Execução

# COMMAND ----------

#Pre Copy Script
if PreCopyScript is not None and PreCopyScript.strip() != '':
    spark.sql(PreCopyScript)

# COMMAND ----------

#carga full
if tipoCarga=='1':
    delete_files(databricksCaminhoDelta, nomeDatabaseDatabricks, nomeTabelaDatabricks)
    carga_full(databricksCaminhoLanding,databricksCaminhoDelta,nomeDatabaseDatabricks,nomeTabelaDatabricks,queryOrigem)

# COMMAND ----------

#carga delete-insert
if tipoCarga=='2':  
    df_etl = executeQuery(queryOrigem)
    deleteInsert_data(nomeDatabaseDatabricks+'.'+nomeTabelaDatabricks, df_etl, regraIncremental)

# COMMAND ----------

#carga merge
if tipoCarga=='3':   
    df_etl = executeQuery(queryOrigem)
    merge_data(nomeDatabaseDatabricks+'.'+nomeTabelaDatabricks, df_etl, regraIncremental)

# COMMAND ----------

#carga de execucao de notebook
if tipoCarga=='4':
    dbutils.notebook.run(nomeNotebookDatabricks, 3000)

# COMMAND ----------

#carga de execucao de query
if tipoCarga=='5':
    executeSaveQuery(queryOrigem,databricksCaminhoDelta,nomeDatabaseDatabricks,nomeTabelaDatabricks)

# COMMAND ----------

#carga delete where exists
if tipoCarga=='6':    
    df_etl = executeQuery(queryOrigem)
    deleteWhereExists_Insert(nomeDatabaseDatabricks+'.'+nomeTabelaDatabricks, df_etl, regraIncremental)

# COMMAND ----------

#post copy script 
if PostCopyScript is not None and PostCopyScript.strip() != '':
    spark.sql(PostCopyScript)

