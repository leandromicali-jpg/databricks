# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Inicia os processos de carga
# MAGIC

# COMMAND ----------

import os
import time
from IPython.display import clear_output

# COMMAND ----------

# DBTITLE 1,Obtém Parâmetros
dbutils.widgets.text("nomeAmbiente", "dev", "nomeAmbiente")
dbutils.widgets.text("codigoExecucao", "0", "codigoExecucao")

nomeAmbiente = dbutils.widgets.get("nomeAmbiente")
codigoExecucao = dbutils.widgets.get("codigoExecucao")

# COMMAND ----------

unitCatalogName = os.getenv(f"unitCatalogName{nomeAmbiente.capitalize()}")
spark.sql(f'USE CATALOG {unitCatalogName}')

# COMMAND ----------

# DBTITLE 1,Define Função de Execução
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

# DBTITLE 1,Update Status
query = """
    update 0_par.processos
    set idSetupStatus = 1
    ,dataInicio = null
    ,dataFinal = null
    ,arquivoLanding = 0
    where
    nomeAmbiente = lower('{}') 
    and codigoExecucao = '{}'
"""
query = query.format(nomeAmbiente,codigoExecucao)
tentativas(query)

# COMMAND ----------

query = """
    update 0_par.processos
    set arquivoLanding = 1
    where tipoCarga in (4,5)
    and nomeAmbiente = lower('{}')
    and codigoExecucao = '{}'
    """
query = query.format(nomeAmbiente,codigoExecucao)
tentativas(query)