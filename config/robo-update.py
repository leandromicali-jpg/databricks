# Databricks notebook source
import os
import time
from IPython.display import clear_output

# COMMAND ----------

dbutils.widgets.text("idprocesso", "", "idprocesso")
dbutils.widgets.text("idSetupStatus", "")
dbutils.widgets.text("tipo", "")
dbutils.widgets.text("conteinerName", "")
unitCatalogName = os.getenv("unitCatalogNamePrd")
print(unitCatalogName)

idprocesso = dbutils.widgets.get("idprocesso")
idSetupStatus = dbutils.widgets.get("idSetupStatus")
tipo = dbutils.widgets.get("tipo")
containerName = dbutils.widgets.get("conteinerName")

nomeAmbiente = os.getenv('env')
idprocesso_ajustado = idprocesso.strip()
storageName = os.getenv('storageNamePrd')

# COMMAND ----------

dados_processo = spark.sql(f"""
    SELECT * 
    FROM {unitCatalogName}.0_par.processos
    WHERE idprocesso = cast({idprocesso_ajustado} as int)
    """).collect()[0].asDict()
print(dados_processo)

# COMMAND ----------

containerName = os.getenv('conteinerName')
path = f"abfss://{containerName}@{storageName}.dfs.core.windows.net/landing/{dados_processo['nomeTabelaDatabricks']}/"
print(f"{path}")
parquet_gerado = 0

# COMMAND ----------

try:
    arquivos = dbutils.fs.ls(path)

    if not arquivos:
        print("Diretório existe, mas está vazio.")
        parquet_gerado = 0
    else:
        for arquivo in arquivos:
            if arquivo.name.endswith(".parquet"):
                print(f"Nome: {arquivo.name}, Caminho: {arquivo.path}, Tamanho: {arquivo.size}")
                parquet_gerado = 1
            else:
                parquet_gerado = 0

except Exception as e:
    if "java.io.FileNotFoundException" in str(e):
        print(f"O diretório '{path}' não existe. Nada a fazer.")
    else:
        print(f"Erro inesperado ao acessar o diretório: {path}")
        raise e  # re-levanta o erro se for algo não esperado

# COMMAND ----------

def exec_query(query):
    spark.sql(query)

def tentativas(query):
    tentativas = 5
    i = 1
    while i <= tentativas:
        try:
            exec_query(query)
            break
        except Exception as erro:
            clear_output(wait=True)
            print(erro)
            time.sleep(5)
        i = i+1

# COMMAND ----------

if tipo == 'landing':
    if parquet_gerado == 1:
        query = f"""
        update {unitCatalogName}.0_par.processos
        set arquivoLanding = 1
        where 
        idprocesso = cast({idprocesso_ajustado} as int)
        """
        tentativas(query)
    else:
        query = f"""
        update {unitCatalogName}.0_par.processos
        set arquivoLanding = 0
        , idSetupStatus = 4
        where 
        idprocesso = cast({idprocesso} as int)
        """
        tentativas(query)
else:
    query = f"""
    update {unitCatalogName}.0_par.processos
    set idSetupStatus = {idSetupStatus}
    ,dataFinal = current_timestamp()
    where 
    idprocesso = cast({idprocesso} as int)
    """
    tentativas(query)

