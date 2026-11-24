# Databricks notebook source
import os
import json

# COMMAND ----------

dbutils.widgets.text("codigoExecucao", "0", "codigoExecucao")
dbutils.widgets.text("ordemExecucao", "0", "ordemExecucao")
dbutils.widgets.text("camada", "bronze", "camada")
#dbutils.widgets.text("tipoOrigem", "sqlserver", "tipoOrigem")

codigoExecucao = dbutils.widgets.get("codigoExecucao")
ordemExecucao = dbutils.widgets.get("ordemExecucao")
camada = dbutils.widgets.get("camada")
unitCatalogName = os.getenv('unitCatalogNamePrd')
#tipoOrigem = dbutils.widgets.get("tipoOrigem")
env = os.getenv('env')

print((codigoExecucao, ordemExecucao, camada,env))

# COMMAND ----------

query = f"""
    select * except (dataFinal, dataInicio) 
    from {unitCatalogName}.0_par.processos
    where 1=1 
        and habilitado=1 
        and idSetupStatus <> 4 
        and nomeAmbiente = '{env}' 
        and codigoExecucao = '{codigoExecucao}' 
        and ordemExecucao = '{ordemExecucao}' 
"""

# COMMAND ----------

# DBTITLE 1,Cell 4
de_para_camada_filtro = {
    "landing": f"""
        and arquivoLanding = 0 
        and nomeCamada = 'bronze' 
        """,
    "bronze": """
        and arquivoLanding = 1
        and nomeCamada = 'bronze' 
        """,
    "silver": """
        and nomeCamada = 'silver'
        """,
    "gold": """
        and nomeCamada = 'gold'
        """,
}

camada_tratada = camada.strip().lower()
filtro = de_para_camada_filtro.get(camada_tratada, None)
if filtro is None:
    raise Exception(f"Camada {camada} não mapeada")
else: 
    query += filtro + "order by idprocesso asc"
print(query)

# COMMAND ----------

df = spark.sql(query)
display(df)

# COMMAND ----------

pesquisar_processos = [row.asDict()['idprocesso'] for row in df.collect()]

# COMMAND ----------

pesquisar_processos = [row.asDict()['idprocesso'] for row in df.collect()]
dbutils.jobs.taskValues.set(key="ids_processos", value=json.dumps(pesquisar_processos))