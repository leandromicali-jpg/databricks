# Databricks notebook source
import os

# COMMAND ----------

dbutils.widgets.text("idprocesso", "", "idprocesso")
idprocesso = dbutils.widgets.get("idprocesso")

containerName = os.getenv('containerName')
unitCatalogName = os.getenv('unitCatalogName')
print((idprocesso, containerName, unitCatalogName))

spark.sql(f"USE CATALOG {unitCatalogName}")

# COMMAND ----------

idprocesso_ajustado = idprocesso.strip()
dados_processo = spark.sql(f"""
    SELECT * 
    FROM 0_par.processos
    WHERE idprocesso = cast({idprocesso_ajustado} as int)
    """).collect()[0].asDict()
print(dados_processo)

# COMMAND ----------

containerName = os.getenv('containerName')
path = f"/mnt/{containerName}/landing/{dados_processo['nomeTabelaDatabricks']}/"
print(f"{path}")

# COMMAND ----------

try:
    arquivos = dbutils.fs.ls(path)

    if not arquivos:
        print("Diretório existe, mas está vazio.")
    else:
        for arquivo in arquivos:
            print(f"Nome: {arquivo.name}, Caminho: {arquivo.path}, Tamanho: {arquivo.size}")
            if arquivo.name.endswith(".parquet"):
                print(f"Deletando: {arquivo.path}")
                dbutils.fs.rm(arquivo.path)

except Exception as e:
    if "java.io.FileNotFoundException" in str(e):
        print(f"O diretório '{path}' não existe. Nada a fazer.")
    else:
        print(f"Erro inesperado ao acessar o diretório: {path}")
        raise e  # re-levanta o erro se for algo não esperado