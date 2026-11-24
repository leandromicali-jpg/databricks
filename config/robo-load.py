# Databricks notebook source
# MAGIC %md
# MAGIC # Componente de Carga
# MAGIC Componente de carga da camada Landing para a camada Bronze

# COMMAND ----------

# DBTITLE 1,Libraries
import os
from delta.tables import *

# COMMAND ----------

dbutils.widgets.text("idprocesso", "", "idprocesso")
idprocesso = dbutils.widgets.get("idprocesso")

print(idprocesso)

# COMMAND ----------

# DBTITLE 1,Paramns
#Propriedades customizadas
container = os.getenv("conteinerName")
storage = os.getenv(f"storageNamePrd")
unitCatalogName = os.getenv(f"unitCatalogNamePrd")
databricksCaminhoLanding = f"abfss://{container}@{storage}.dfs.core.windows.net/landing"
databricksCaminhoDelta = f"abfss://{container}@{storage}.dfs.core.windows.net/externaldelta"

spark.sql(f"USE CATALOG {unitCatalogName}")
print((container, storage, unitCatalogName))


# COMMAND ----------

# DBTITLE 1,Cell 5
idprocesso_ajustado = idprocesso.strip()
dados_processo = spark.sql(f"""
    SELECT * 
    FROM 0_par.processos
    WHERE idprocesso = cast({idprocesso_ajustado} as int)
    """).collect()[0].asDict()
print(dados_processo)

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

# COMMAND ----------

# DBTITLE 1,Catalog Selection
spark.sql(f'USE CATALOG {unitCatalogName}')
print(unitCatalogName)

# COMMAND ----------

# MAGIC %md
# MAGIC # Definição de Funções

# COMMAND ----------

# DBTITLE 1,Funções de Carga
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

def carga_full(read_path,save_path,database_name,table_name):
    df = get_delta_parquet(str(read_path)+"/"+str(table_name))
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
    new_data.write.format("delta").mode("append").option("schema","false").saveAsTable(existing_table_name)

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

def executeQuery(query, save_path, database_name, table_name):   
    df = spark.sql(query)
    df.write\
        .mode("overwrite")\
        .option("overwriteSchema",True)\
        .format("delta")\
        .option("path",f"{save_path}/{database_name}/{table_name}")\
        .saveAsTable(f"{database_name}.{table_name}")

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
    carga_full(databricksCaminhoLanding,databricksCaminhoDelta,nomeDatabaseDatabricks,nomeTabelaDatabricks)

# COMMAND ----------

#carga delete-insert com header
if tipoCarga=='2':    
    df_etl = get_delta_parquet(str(databricksCaminhoLanding)+"/"+str(nomeTabelaDatabricks))
    deleteInsert_data(nomeDatabaseDatabricks+'.'+nomeTabelaDatabricks, df_etl, regraIncremental)      

# COMMAND ----------

#carga merge com header
if tipoCarga=='3':      
    df_etl = get_delta_parquet(str(databricksCaminhoLanding)+"/"+str(nomeTabelaDatabricks))
    merge_data(nomeDatabaseDatabricks+'.'+nomeTabelaDatabricks, df_etl, regraIncremental)

# COMMAND ----------

#carga de execucao de notebook
if tipoCarga=='4':
    dbutils.notebook.run(nomeNotebookDatabricks, 3000)

# COMMAND ----------

#carga de execucao de query
if tipoCarga=='5':
    executeQuery(queryOrigem,databricksCaminhoDelta,nomeDatabaseDatabricks,nomeTabelaDatabricks)        

# COMMAND ----------

#carga delete where exists
if tipoCarga=='6':   
    df_etl = get_delta_parquet(str(databricksCaminhoLanding)+"/"+str(nomeTabelaDatabricks))
    deleteWhereExists_Insert(nomeDatabaseDatabricks+'.'+nomeTabelaDatabricks, df_etl, regraIncremental)

# COMMAND ----------

#post copy script 
if PostCopyScript is not None and PostCopyScript.strip() != '':
    spark.sql(PostCopyScript)

