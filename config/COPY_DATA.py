# Databricks notebook source
import os
import re
import unicodedata
from pyspark.sql import functions as F

# 1. Recupera parâmetros
dbutils.widgets.text("idprocesso", "", "idprocesso")
idprocesso = dbutils.widgets.get("idprocesso").strip()

if not idprocesso:
    raise ValueError("O parâmetro 'idprocesso' é obrigatório.")

containerName = os.getenv('conteinerName')
unitCatalogName = os.getenv('unitCatalogNamePrd')
spark.sql(f"USE CATALOG {unitCatalogName}")
storageName = os.getenv('storageNamePrd')



# COMMAND ----------

# 2. Busca metadados do processo
# Usamos crases (backticks) em `0_par` por causa do número inicial
query_metadata = f"SELECT * FROM {unitCatalogName}.`0_par`.processos WHERE idprocesso = cast({idprocesso} as int)"
dados_processo = spark.sql(query_metadata).collect()[0].asDict()
nome_tabela = dados_processo['nomeTabelaDatabricks']
Arquivo = dados_processo['nomeArquivoOrigem']

# COMMAND ----------

# Path 1: Onde o CSV original reside
path_origem_csv = f"abfss://{containerName}@{storageName}.dfs.core.windows.net/filestore/csv/{Arquivo}"

# Path 2: Onde os dados serão salvos em formato Delta (Landing Zone)
path_destino_landing = f"abfss://{containerName}@{storageName}.dfs.core.windows.net/landing/{nome_tabela}"

print(f"Path de Origem: {path_origem_csv}")
print(f"Path de Destino: {path_destino_landing}")

dbutils.fs.rm(path_destino_landing, recurse=True)

# COMMAND ----------

def sanitize_column_names(df):
    new_cols = []
    for col_name in df.columns:
        # Remove acentos (ex: 'ã' -> 'a') usando o módulo nativo unicodedata
        normalized = unicodedata.normalize('NFD', col_name)
        clean_name = "".join([c for c in normalized if not unicodedata.combining(c)])
        
        # Substitui qualquer caracter que não seja letra, número ou underscore por '_'
        clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', clean_name)
        
        # Remove underscores duplicados (ex: 'Nome__Tabela' -> 'Nome_Tabela')
        clean_name = re.sub(r'_+', '_', clean_name).strip('_')
        
        new_cols.append(clean_name)
    
    return df.toDF(*new_cols)

# COMMAND ----------

# DBTITLE 1,Tipo de Dados
# 1. Leitura dos dados
tipo_origem = dados_processo['tipoOrigem'].strip().lower()


if tipo_origem == "csv":
    print("Lendo CSV via Unity Catalog...")
    df = spark.sql(f"""
        SELECT * FROM read_files(
            '{path_origem_csv}',
            format => 'csv',
            header => 'true',
            inferSchema => 'true',
            delimiter => ','
        )
    """)

elif tipo_origem == "jdbc":
    print("Carga jdbc - Oracle")
    host = " "
    porta = " "
    servico = dados_processo['obs']
    url = f"jdbc:oracle:thin:@{host}:{porta}:{servico}"
    df = (
        spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", f"({dados_processo['queryOrigem']}) temp_jdbc")
        .option("user", "PBI_USER")
        .option("password", "PBI_Use#2_2")
        .option("driver", "oracle.jdbc.driver.OracleDriver")
        .load()
    )

else:
    print("Lendo via Query SQL...")
    df = spark.sql(dados_processo['queryOrigem'])

# COMMAND ----------

# 2. Sanitização das colunas (Resolve o erro DELTA_INVALID_CHARACTERS)
df_clean = sanitize_column_names(df)


# COMMAND ----------

# 3. Escrita na Landing (Formato Delta)
print(f"Gravando dados em {path_destino_landing}...")
df_clean.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(path_destino_landing)
 
print("Processo concluído com sucesso!")
#display(df_clean.limit(5))