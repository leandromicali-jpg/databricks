# Databricks notebook source
import os
import json

# COMMAND ----------

dbutils.widgets.text("codigoExecucao", "1")
codigoExecucao = dbutils.widgets.get("codigoExecucao")
print(codigoExecucao)

# COMMAND ----------

unitCatalogName = os.getenv('unitCatalogNamePrd')
env = os.getenv("env")

spark.sql(f'USE CATALOG {unitCatalogName}')
print(unitCatalogName)


# COMMAND ----------

query_ordem = f"""
select distinct cast(ordemExecucao as int) 
from 0_par.processos 
where 1=1 
    and habilitado=1 and idSetupStatus <> 4
    and lower(nomeCamada) = "$CAMADA" 
    and lower(nomeAmbiente) = lower("$ENV") and codigoExecucao = $CODIGOEXECUCAO
    $FILTROLANDING
    order by 1 asc
"""

# COMMAND ----------

query_ordem = query_ordem.replace("$ENV", env).replace("$CODIGOEXECUCAO", codigoExecucao)
print(query_ordem)

# COMMAND ----------

ordemExecucaoPorCamada = {
    "landing": [row["ordemExecucao"] 
                    for row in spark.sql(
                        query_ordem.replace("$CAMADA", "bronze")
                        .replace('$FILTROLANDING', 'and arquivoLanding = 0')
                        ).collect()
        ],
    "bronze": [row["ordemExecucao"] 
                for row in spark.sql(
                    query_ordem.replace("$CAMADA", "bronze")
                    .replace('$FILTROLANDING', '')
                ).collect()
        ],
    "silver": [row["ordemExecucao"] 
                for row in spark.sql(
                    query_ordem.replace("$CAMADA", "silver")
                    .replace('$FILTROLANDING', '')
                ).collect()
        ],
    "gold": [row["ordemExecucao"] 
                for row in spark.sql(
                    query_ordem.replace("$CAMADA", "gold")
                    .replace('$FILTROLANDING', '')
                ).collect()
        ]
}

# COMMAND ----------

print(ordemExecucaoPorCamada)

# COMMAND ----------

dbutils.jobs.taskValues.set(key="ordemExecucao_landing", value=json.dumps(ordemExecucaoPorCamada["landing"]))
dbutils.jobs.taskValues.set(key="ordemExecucao_bronze", value=json.dumps(ordemExecucaoPorCamada["bronze"]))
dbutils.jobs.taskValues.set(key="ordemExecucao_silver", value=json.dumps(ordemExecucaoPorCamada["silver"]))
dbutils.jobs.taskValues.set(key="ordemExecucao_gold", value=json.dumps(ordemExecucaoPorCamada["gold"]))