# Databricks notebook source
# DBTITLE 1,Landing
# MAGIC %sql
# MAGIC Insert Into uc_ultrapar_prdbi.`0_par`.processos (codigoExecucao,ordemExecucao,nomeProcesso,nomeAmbiente,tipoOrigem,habilitado,idSetupStatus,arquivoLanding,nomeCamada,nomeOrigem,nomeSchemaOrigem,nomeTabelaOrigem,nomeNotebookDatabricks,caminhoArquivoOrigem,nomeArquivoOrigem,nomeDatabaseDatabricks,nomeTabelaDatabricks,header,queryOrigem,PreCopyScript,PostCopyScript,dataInicio,dataFinal,tipoCarga,regraIncremental,args,obs)
# MAGIC Select
# MAGIC   1 as codigoExecucao,
# MAGIC   1 as ordemExecucao,
# MAGIC   'arq_test' as nomeProcesso,
# MAGIC   'prd' as nomeAmbiente,
# MAGIC   'csv' as tipoOrigem,
# MAGIC   '1' as habilitado,
# MAGIC   '1' as idSetupStatus,
# MAGIC   '0' as arquivoLanding,
# MAGIC   'raw' as nomeCamada,
# MAGIC   'arquivo' as nomeOrigem,
# MAGIC   '' as nomeSchemaOrigem,
# MAGIC   '' as nomeTabelaOrigem,
# MAGIC   '' as nomeNotebookDatabricks,
# MAGIC   'filestore/arquivosteste/csv' as caminhoArquivoOrigem,
# MAGIC   'arquivotest.csv' as nomeArquivoOrigem,
# MAGIC   '1_raw' as nomeDatabaseDatabricks,
# MAGIC   'tbl_raw_arquivotest' as nomeTabelaDatabricks,
# MAGIC   '1' as header,
# MAGIC   "select * from uc_ultrapar_prdbi.`1_raw`.tbl_raw_arquivotest " as queryOrigem,
# MAGIC   '' as PreCopyScript,
# MAGIC   '' as PostCopyScript,
# MAGIC   current_timestamp() as dataInicio,
# MAGIC   current_timestamp() as dataFinal,
# MAGIC   '1' as tipoCarga,
# MAGIC   '' as regraIncremental,
# MAGIC   '' as args,
# MAGIC   '' as obs

# COMMAND ----------

# DBTITLE 1,RAW
# MAGIC %sql
# MAGIC Insert Into uc_ultrapar_prdbi.`0_par`.processos (codigoExecucao,ordemExecucao,nomeProcesso,nomeAmbiente,tipoOrigem,habilitado,idSetupStatus,arquivoLanding,nomeCamada,nomeOrigem,nomeSchemaOrigem,nomeTabelaOrigem,nomeNotebookDatabricks,caminhoArquivoOrigem,nomeArquivoOrigem,nomeDatabaseDatabricks,nomeTabelaDatabricks,header,queryOrigem,PreCopyScript,PostCopyScript,dataInicio,dataFinal,tipoCarga,regraIncremental,args,obs)
# MAGIC Select
# MAGIC   1 as codigoExecucao,
# MAGIC   1 as ordemExecucao,
# MAGIC   'arq_test' as nomeProcesso,
# MAGIC   'prd' as nomeAmbiente,
# MAGIC   'csv' as tipoOrigem,
# MAGIC   '1' as habilitado,
# MAGIC   '1' as idSetupStatus,
# MAGIC   '1' as arquivoLanding,
# MAGIC   'raw' as nomeCamada,
# MAGIC   'arquivo' as nomeOrigem,
# MAGIC   '' as nomeSchemaOrigem,
# MAGIC   '' as nomeTabelaOrigem,
# MAGIC   '' as nomeNotebookDatabricks,
# MAGIC   'filestore/arquivosteste/csv' as caminhoArquivoOrigem,
# MAGIC   'arquivotest.csv' as nomeArquivoOrigem,
# MAGIC   '1_raw' as nomeDatabaseDatabricks,
# MAGIC   'tbl_raw_arquivotest' as nomeTabelaDatabricks,
# MAGIC   '1' as header,
# MAGIC   "select * from uc_ultrapar_prdbi.`1_raw`.tbl_raw_arquivotest " as queryOrigem,
# MAGIC   '' as PreCopyScript,
# MAGIC   '' as PostCopyScript,
# MAGIC   current_timestamp() as dataInicio,
# MAGIC   current_timestamp() as dataFinal,
# MAGIC   '1' as tipoCarga,
# MAGIC   '' as regraIncremental,
# MAGIC   '' as args,
# MAGIC   '' as obs

# COMMAND ----------

# DBTITLE 1,TRUSTED
# MAGIC %sql
# MAGIC Insert Into uc_ultrapar_prdbi.`0_par`.processos (codigoExecucao,ordemExecucao,nomeProcesso,nomeAmbiente,tipoOrigem,habilitado,idSetupStatus,arquivoLanding,nomeCamada,nomeOrigem,nomeSchemaOrigem,nomeTabelaOrigem,nomeNotebookDatabricks,caminhoArquivoOrigem,nomeArquivoOrigem,nomeDatabaseDatabricks,nomeTabelaDatabricks,header,queryOrigem,PreCopyScript,PostCopyScript,dataInicio,dataFinal,tipoCarga,regraIncremental,args,obs)
# MAGIC Select
# MAGIC   1 as codigoExecucao,
# MAGIC   1 as ordemExecucao,
# MAGIC   'arq_test' as nomeProcesso,
# MAGIC   'prd' as nomeAmbiente,
# MAGIC   'csv' as tipoOrigem,
# MAGIC   '1' as habilitado,
# MAGIC   '1' as idSetupStatus,
# MAGIC   '1' as arquivoLanding,
# MAGIC   'trusted' as nomeCamada,
# MAGIC   'arquivo' as nomeOrigem,
# MAGIC   '' as nomeSchemaOrigem,
# MAGIC   '' as nomeTabelaOrigem,
# MAGIC   '' as nomeNotebookDatabricks,
# MAGIC   'filestore/arquivosteste/csv' as caminhoArquivoOrigem,
# MAGIC   'arquivotest.csv' as nomeArquivoOrigem,
# MAGIC   '2_trusted' as nomeDatabaseDatabricks,
# MAGIC   'tbl_trusted_arquivotest' as nomeTabelaDatabricks,
# MAGIC   '1' as header,
# MAGIC   "select * from uc_ultrapar_prdbi.`1_raw`.tbl_raw_arquivotest " as queryOrigem,
# MAGIC   '' as PreCopyScript,
# MAGIC   '' as PostCopyScript,
# MAGIC   current_timestamp() as dataInicio,
# MAGIC   current_timestamp() as dataFinal,
# MAGIC   '1' as tipoCarga,
# MAGIC   '' as regraIncremental,
# MAGIC   '' as args,
# MAGIC   '' as obs

# COMMAND ----------

# DBTITLE 1,REFINED
# MAGIC %sql
# MAGIC Insert Into uc_ultrapar_prdbi.`0_par`.processos (codigoExecucao,ordemExecucao,nomeProcesso,nomeAmbiente,tipoOrigem,habilitado,idSetupStatus,arquivoLanding,nomeCamada,nomeOrigem,nomeSchemaOrigem,nomeTabelaOrigem,nomeNotebookDatabricks,caminhoArquivoOrigem,nomeArquivoOrigem,nomeDatabaseDatabricks,nomeTabelaDatabricks,header,queryOrigem,PreCopyScript,PostCopyScript,dataInicio,dataFinal,tipoCarga,regraIncremental,args,obs)
# MAGIC Select
# MAGIC   1 as codigoExecucao,
# MAGIC   2 as ordemExecucao,
# MAGIC   'arq_test' as nomeProcesso,
# MAGIC   'prd' as nomeAmbiente,
# MAGIC   'csv' as tipoOrigem,
# MAGIC   '1' as habilitado,
# MAGIC   '1' as idSetupStatus,
# MAGIC   '1' as arquivoLanding,
# MAGIC   'refined' as nomeCamada,
# MAGIC   'arquivo' as nomeOrigem,
# MAGIC   '' as nomeSchemaOrigem,
# MAGIC   '' as nomeTabelaOrigem,
# MAGIC   '' as nomeNotebookDatabricks,
# MAGIC   'filestore/arquivosteste/csv' as caminhoArquivoOrigem,
# MAGIC   'arquivotest.csv' as nomeArquivoOrigem,
# MAGIC   '3_refined' as nomeDatabaseDatabricks,
# MAGIC   'tbl_refined_arquivotest' as nomeTabelaDatabricks,
# MAGIC   '1' as header,
# MAGIC   "select * from uc_ultrapar_prdbi.`1_raw`.tbl_raw_arquivotest " as queryOrigem,
# MAGIC   '' as PreCopyScript,
# MAGIC   '' as PostCopyScript,
# MAGIC   current_timestamp() as dataInicio,
# MAGIC   current_timestamp() as dataFinal,
# MAGIC   '1' as tipoCarga,
# MAGIC   '' as regraIncremental,
# MAGIC   '' as args,
# MAGIC   '' as obs