# Databricks notebook source
# MAGIC %md 
# MAGIC <h2> SYN Connection </h2>

# COMMAND ----------

#%run ../reconciliation/utils/sql-connection

# COMMAND ----------

#import pandas as pd
# open connection - synapse
#dw_conn = synapseConnection.sqlDWConnect()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT measure_group_name, measure_name FROM red_dp_recon.dp_epidata WHERE measure_source = 'EPI'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT measure_source, measure_group_name, measure_name FROM red_dp_recon.red_policy_measures_data WHERE measure_source = 'EPI'

# COMMAND ----------

# MAGIC %md 
# MAGIC <h3> EPI Recon - Variance 

# COMMAND ----------

dpepi_df = sql(
'''
WITH DP AS 
(
SELECT 
UPPER(dpepi.policy_reference) AS policy_reference,  
dpepi.measure_name,
SUM(dpepi.measure_amount) AS EPI_AMOUNT
FROM red_dp_recon.dp_epidata dpepi
WHERE dpepi.measure_name in ('Line_Gross_Net_Estimated_Premium_SCC', 'Line_Gross_Gross_Estimated_Premium_SCC')
GROUP BY dpepi.policy_reference,
dpepi.measure_name
)
SELECT policy_reference AS POLICY_REFERENCE,
ROUND(COALESCE(Line_Gross_Net_Estimated_Premium_SCC,0),2) AS DP_LGN_Prem_SCC,
ROUND(COALESCE(Line_Gross_Gross_Estimated_Premium_SCC,0),2) AS DP_LGG_Prem_SCC
FROM 
(SELECT Policy_reference,
measure_name,
EPI_AMOUNT
FROM DP
) t
PIVOT 
(
SUM(EPI_AMOUNT) FOR measure_name IN ('Line_Gross_Net_Estimated_Premium_SCC', 'Line_Gross_Gross_Estimated_Premium_SCC')
) 
ORDER BY policy_reference
''')

# COMMAND ----------

dpepi_df.createOrReplaceTempView("vw_dpepi")

# COMMAND ----------

redepi_df = sql(
'''WITH RED AS
(
SELECT 
UPPER(redepi.policy_reference) AS policy_reference, 
redepi.measure_name,
SUM(redepi.measure_amount) AS EPI_AMOUNT
FROM red_dp_recon.red_policy_measures_data redepi
WHERE redepi.measure_name in ('GNWPI_Syndicate_SCC_PMD', 'GGWPI_Syndicate_SCC_PMD')
GROUP BY redepi.policy_reference,
redepi.measure_name
)
SELECT policy_reference AS POLICY_REFERENCE,
(ROUND(COALESCE(GNWPI_Syndicate_SCC_PMD,0),2) ) AS RED_GN_EPI,
(ROUND(COALESCE(GGWPI_Syndicate_SCC_PMD),2) ) AS RED_GG_EPI
FROM 
(SELECT Policy_reference,
measure_name,
EPI_AMOUNT
FROM RED
) t
PIVOT 
(
SUM(EPI_AMOUNT) FOR measure_name IN ('GNWPI_Syndicate_SCC_PMD', 'GGWPI_Syndicate_SCC_PMD')
) 
ORDER BY policy_reference
''')

# COMMAND ----------

redepi_df.createOrReplaceTempView("vw_redepi")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC vdpe.policy_reference, 
# MAGIC round(vdpe.DP_LGN_Prem_SCC,2) DP_Line_GN_SCC, round(vre.RED_GN_EPI,2) RED_Line_GN_EPI, (round(vdpe.DP_LGN_Prem_SCC,2) - round(vre.RED_GN_EPI,2)) EPI_Line_GN_VAR,
# MAGIC round(vdpe.DP_LGG_Prem_SCC,2) DP_Line_GG_SCC, round(vre.RED_GG_EPI,2) RED_Line_GG_EPI, (round(vdpe.DP_LGG_Prem_SCC,2) - round(vre.RED_GG_EPI,2)) EPI_Line_GG_VAR,
# MAGIC excl.reports_epi, 'Common_Policy' as Policy_Availability
# MAGIC FROM vw_dpepi vdpe
# MAGIC JOIN vw_redepi vre ON vdpe.policy_reference = vre.policy_reference
# MAGIC left join red_dp_recon.red_epi_variance excl
# MAGIC on Upper(vdpe.policy_reference) = Upper(excl.policy_reference)
# MAGIC 
# MAGIC Union
# MAGIC 
# MAGIC select 
# MAGIC vdpe.policy_reference,
# MAGIC round(vdpe.DP_LGN_Prem_SCC,2) DP_Line_GN_SCC, '0' RED_Line_GN_EPI, coalesce((round(vdpe.DP_LGN_Prem_SCC,2) - round(vre.RED_GN_EPI,2)),0) EPI_Line_GN_VAR,
# MAGIC round(vdpe.DP_LGG_Prem_SCC,2) DP_Line_GG_SCC, '0' RED_Line_GG_EPI, coalesce((round(vdpe.DP_LGG_Prem_SCC,2) - round(vre.RED_GG_EPI,2)),0) EPI_Line_GG_VAR,
# MAGIC excl.reports_epi, 'Missing_Policy_in_Red' as Policy_Availability
# MAGIC FROM vw_dpepi vdpe
# MAGIC LEFT JOIN vw_redepi vre ON vdpe.policy_reference = vre.policy_reference
# MAGIC left join red_dp_recon.red_epi_variance excl
# MAGIC on Upper(vdpe.policy_reference) = Upper(excl.policy_reference)
# MAGIC where vre.policy_reference is NULL
# MAGIC 
# MAGIC Union
# MAGIC 
# MAGIC select 
# MAGIC vre.policy_reference,
# MAGIC '0' DP_Line_GN_SCC, round(vre.RED_GN_EPI,2) RED_Line_GN_EPI, coalesce((round(vdpe.DP_LGN_Prem_SCC,2) - round(vre.RED_GN_EPI,2)),0) EPI_Line_GN_VAR,
# MAGIC '0' DP_Line_GG_SCC, round(vre.RED_GG_EPI,2) RED_Line_GG_EPI, coalesce((round(vdpe.DP_LGG_Prem_SCC,2) - round(vre.RED_GG_EPI,2)),0) EPI_Line_GG_VAR,
# MAGIC excl.reports_epi, 'Missing_Policy_in_DP' as Policy_Availability
# MAGIC FROM vw_dpepi vdpe
# MAGIC RIGHT JOIN vw_redepi vre ON vdpe.policy_reference = vre.policy_reference
# MAGIC left join red_dp_recon.red_epi_variance excl
# MAGIC on Upper(vre.policy_reference) = Upper(excl.policy_reference)
# MAGIC where vdpe.policy_reference is NULL

# COMMAND ----------

# close connection - synapse 
# synapseConnection.closeDWConnect(dw_conn)