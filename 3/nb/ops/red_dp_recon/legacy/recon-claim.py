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
# MAGIC SELECT DISTINCT measure_group_name, measure_name FROM red_dp_recon.dp_claimsdata WHERE measure_source = 'CLAIMS'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT measure_group_name, measure_name FROM red_dp_recon.red_policy_measures_data WHERE measure_source = 'CLAIMS'

# COMMAND ----------

# MAGIC %md 
# MAGIC <h3> Claims Recon - Variance 

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH DP_RED AS 
# MAGIC (
# MAGIC SELECT 
# MAGIC UPPER(dpclaim.policy_reference) AS policy_reference,  
# MAGIC dpclaim.measure_name,
# MAGIC SUM(dpclaim.measure_amount) AS CLAIMS_AMOUNT
# MAGIC FROM red_dp_recon.dp_claimsdata dpclaim
# MAGIC WHERE dpclaim.measure_name = 'Line_Gross_Outstanding_Claim_SCC' 
# MAGIC GROUP BY dpclaim.policy_reference,
# MAGIC dpclaim.measure_name
# MAGIC UNION
# MAGIC SELECT 
# MAGIC UPPER(dpclaim.policy_reference) AS policy_reference,  
# MAGIC dpclaim.measure_name,
# MAGIC SUM(dpclaim.measure_amount) AS CLAIMS_AMOUNT
# MAGIC FROM red_dp_recon.dp_claimsdata dpclaim
# MAGIC WHERE dpclaim.measure_name = 'Line_Gross_Paid_Claim_SCC'
# MAGIC GROUP BY dpclaim.policy_reference,
# MAGIC dpclaim.measure_name
# MAGIC UNION
# MAGIC SELECT 
# MAGIC UPPER(redclaim.policy_reference) AS policy_reference, 
# MAGIC redclaim.measure_name,
# MAGIC SUM(redclaim.measure_amount) AS CLAIMS_AMOUNT
# MAGIC FROM red_dp_recon.red_policy_measures_data redclaim
# MAGIC WHERE redclaim.measure_name ='Gross_Claim_SCM_OS'
# MAGIC GROUP BY redclaim.policy_reference,
# MAGIC redclaim.measure_name
# MAGIC UNION
# MAGIC SELECT 
# MAGIC UPPER(redclaim.policy_reference) AS policy_reference, 
# MAGIC redclaim.measure_name,
# MAGIC SUM(redclaim.measure_amount) AS CLAIMS_AMOUNT
# MAGIC FROM red_dp_recon.red_policy_measures_data redclaim
# MAGIC WHERE redclaim.measure_name ='Gross_Claim_SCM_Paid'
# MAGIC GROUP BY redclaim.policy_reference,
# MAGIC redclaim.measure_name
# MAGIC )
# MAGIC SELECT policy_reference AS POLICY_REFERENCE,
# MAGIC ROUND(COALESCE(Line_Gross_Paid_Claim_SCC,0),2) AS DP_LINE_PAID_CLAIM,
# MAGIC ROUND(COALESCE(Gross_Claim_SCM_Paid,0),2) AS RED_LINE_PAID_CLAIM,
# MAGIC ROUND((ROUND(COALESCE(Line_Gross_Paid_Claim_SCC,0),2)-(0-ROUND(COALESCE(Gross_Claim_SCM_Paid,0),2))),2) AS LINE_PAID_CLAIM_VARIANCE,
# MAGIC ROUND(COALESCE(Line_Gross_Outstanding_Claim_SCC,0),2) AS DP_LINE_OUTSTANDING_CLAIM,
# MAGIC ROUND(COALESCE(Gross_Claim_SCM_OS,0),2) AS RED_LINE_OUTSTANDING_CLAIM,
# MAGIC ROUND((ROUND(COALESCE(Line_Gross_Outstanding_Claim_SCC,0),2)-(0-ROUND(COALESCE(Gross_Claim_SCM_OS,0),2))),2) AS LINE_OS_CLAIM_VARIANCE
# MAGIC FROM 
# MAGIC (SELECT Policy_reference,
# MAGIC measure_name,
# MAGIC CLAIMS_AMOUNT
# MAGIC FROM DP_RED
# MAGIC ) t
# MAGIC PIVOT 
# MAGIC (
# MAGIC SUM(CLAIMS_AMOUNT) FOR measure_name IN ('Line_Gross_Paid_Claim_SCC','Gross_Claim_SCM_Paid','Line_Gross_Outstanding_Claim_SCC','Gross_Claim_SCM_OS')
# MAGIC ) 
# MAGIC ORDER BY policy_reference

# COMMAND ----------

dpclaim_df = sql(
'''
WITH DP AS 
(
SELECT 
UPPER(dpclaim.policy_reference) AS policy_reference,  
dpclaim.measure_name,
SUM(dpclaim.measure_amount) AS CLAIMS_AMOUNT
FROM red_dp_recon.dp_claimsdata dpclaim
WHERE dpclaim.measure_name = 'Line_Gross_Outstanding_Claim_SCC' 
GROUP BY dpclaim.policy_reference,
dpclaim.measure_name
UNION
SELECT 
UPPER(dpclaim.policy_reference) AS policy_reference,  
dpclaim.measure_name,
SUM(dpclaim.measure_amount) AS CLAIMS_AMOUNT
FROM red_dp_recon.dp_claimsdata dpclaim
WHERE dpclaim.measure_name = 'Line_Gross_Paid_Claim_SCC'
GROUP BY dpclaim.policy_reference,
dpclaim.measure_name
)
SELECT policy_reference AS POLICY_REFERENCE,
ROUND(COALESCE(Line_Gross_Paid_Claim_SCC,0),2) AS DP_PAID_CLAIM,
ROUND(COALESCE(Line_Gross_Outstanding_Claim_SCC,0),2) AS DP_OUTSTANDING_CLAIM
FROM 
(SELECT Policy_reference,
measure_name,
CLAIMS_AMOUNT
FROM DP
) t
PIVOT 
(
SUM(CLAIMS_AMOUNT) FOR measure_name IN ('Line_Gross_Paid_Claim_SCC','Line_Gross_Outstanding_Claim_SCC')
) 
ORDER BY policy_reference
''')

# COMMAND ----------

dpclaim_df.createOrReplaceTempView("dpclaims")

# COMMAND ----------

redclaim_df = sql(
'''WITH RED AS
(
SELECT 
UPPER(redclaim.policy_reference) AS policy_reference, 
redclaim.measure_name,
SUM(redclaim.measure_amount) AS CLAIMS_AMOUNT
FROM red_dp_recon.red_policy_measures_data redclaim
WHERE redclaim.measure_name ='Gross_Claim_SCM_OS'
GROUP BY redclaim.policy_reference,
redclaim.measure_name
UNION
SELECT 
UPPER(redclaim.policy_reference) AS policy_reference, 
redclaim.measure_name,
SUM(redclaim.measure_amount) AS CLAIMS_AMOUNT
FROM red_dp_recon.red_policy_measures_data redclaim
WHERE redclaim.measure_name ='Gross_Claim_SCM_Paid'
GROUP BY redclaim.policy_reference,
redclaim.measure_name
)
SELECT policy_reference AS POLICY_REFERENCE,
ROUND(COALESCE(Gross_Claim_SCM_Paid,0),2) AS RED_PAID_CLAIM,
ROUND(COALESCE(Gross_Claim_SCM_OS,0),2) AS RED_OUTSTANDING_CLAIM
FROM 
(SELECT Policy_reference,
measure_name,
CLAIMS_AMOUNT
FROM RED
) t
PIVOT 
(
SUM(CLAIMS_AMOUNT) FOR measure_name IN ('Gross_Claim_SCM_Paid','Gross_Claim_SCM_OS')
) 
ORDER BY policy_reference
''')

# COMMAND ----------

redclaim_df.createOrReplaceTempView("redclaims")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT rdc.policy_reference AS POLICY_REFERENCE,
# MAGIC ROUND(COALESCE(dpc.DP_PAID_CLAIM,0),2) AS DP_LINE_PAID_CLAIM,
# MAGIC ROUND(COALESCE(rdc.RED_PAID_CLAIM,0),2) AS RED_LINE_PAID_CLAIM,
# MAGIC ROUND((ROUND(COALESCE(dpc.DP_PAID_CLAIM,0),2)-(0-ROUND(COALESCE(rdc.RED_PAID_CLAIM,0),2))),2) AS LINE_PAID_CLAIM_VARIANCE,
# MAGIC ROUND(COALESCE(dpc.DP_OUTSTANDING_CLAIM,0),2) AS DP_LINE_OUTSTANDING_CLAIM,
# MAGIC ROUND(COALESCE(rdc.RED_OUTSTANDING_CLAIM,0),2) AS RED_LINE_OUTSTANDING_CLAIM,
# MAGIC ROUND((ROUND(COALESCE(dpc.DP_OUTSTANDING_CLAIM,0),2)-(0-ROUND(COALESCE(rdc.RED_OUTSTANDING_CLAIM,0),2))),2) AS LINE_OS_CLAIM_VARIANCE,
# MAGIC sp.Polcy_Classification,
# MAGIC 'Common Policy' as Policy_Available
# MAGIC FROM dpclaims dpc
# MAGIC INNER JOIN redclaims rdc ON UPPER(dpc.policy_reference) = UPPER(rdc.policy_reference)
# MAGIC LEFT JOIN red_dp_recon.serco_policies sp ON UPPER(dpc.policy_reference) = UPPER(sp.policy_reference)
# MAGIC 
# MAGIC UNION
# MAGIC 
# MAGIC SELECT rdc.policy_reference AS POLICY_REFERENCE,
# MAGIC ROUND(COALESCE(dpc.DP_PAID_CLAIM,0),2) AS DP_LINE_PAID_CLAIM,
# MAGIC ROUND(COALESCE(rdc.RED_PAID_CLAIM,0),2) AS RED_LINE_PAID_CLAIM,
# MAGIC ROUND((ROUND(COALESCE(dpc.DP_PAID_CLAIM,0),2)-(0-ROUND(COALESCE(rdc.RED_PAID_CLAIM,0),2))),2) AS LINE_PAID_CLAIM_VARIANCE,
# MAGIC ROUND(COALESCE(dpc.DP_OUTSTANDING_CLAIM,0),2) AS DP_LINE_OUTSTANDING_CLAIM,
# MAGIC ROUND(COALESCE(rdc.RED_OUTSTANDING_CLAIM,0),2) AS RED_LINE_OUTSTANDING_CLAIM,
# MAGIC ROUND((ROUND(COALESCE(dpc.DP_OUTSTANDING_CLAIM,0),2)-(0-ROUND(COALESCE(rdc.RED_OUTSTANDING_CLAIM,0),2))),2) AS LINE_OS_CLAIM_VARIANCE,
# MAGIC sp.Polcy_Classification,
# MAGIC 'Missing Policy in RED' as Policy_Available
# MAGIC FROM dpclaims dpc
# MAGIC LEFT JOIN redclaims rdc ON UPPER(dpc.policy_reference) = UPPER(rdc.policy_reference)
# MAGIC LEFT JOIN red_dp_recon.serco_policies sp ON UPPER(dpc.policy_reference) = UPPER(sp.policy_reference)
# MAGIC WHERE rdc.policy_reference IS NULL
# MAGIC 
# MAGIC UNION
# MAGIC 
# MAGIC SELECT rdc.policy_reference AS POLICY_REFERENCE,
# MAGIC ROUND(COALESCE(dpc.DP_PAID_CLAIM,0),2) AS DP_LINE_PAID_CLAIM,
# MAGIC ROUND(COALESCE(rdc.RED_PAID_CLAIM,0),2) AS RED_LINE_PAID_CLAIM,
# MAGIC ROUND((ROUND(COALESCE(dpc.DP_PAID_CLAIM,0),2)-(0-ROUND(COALESCE(rdc.RED_PAID_CLAIM,0),2))),2) AS LINE_PAID_CLAIM_VARIANCE,
# MAGIC ROUND(COALESCE(dpc.DP_OUTSTANDING_CLAIM,0),2) AS LINE_DP_OUTSTANDING_CLAIM,
# MAGIC ROUND(COALESCE(rdc.RED_OUTSTANDING_CLAIM,0),2) AS LINE_RED_OUTSTANDING_CLAIM,
# MAGIC ROUND((ROUND(COALESCE(dpc.DP_OUTSTANDING_CLAIM,0),2)-(0-ROUND(COALESCE(rdc.RED_OUTSTANDING_CLAIM,0),2))),2) AS LINE_OS_CLAIM_VARIANCE,
# MAGIC sp.Polcy_Classification,
# MAGIC 'Missing Policy in DP' as Policy_Available
# MAGIC FROM dpclaims dpc
# MAGIC RIGHT JOIN redclaims rdc ON UPPER(dpc.policy_reference) = UPPER(rdc.policy_reference)
# MAGIC LEFT JOIN red_dp_recon.serco_policies sp ON UPPER(rdc.policy_reference) = UPPER(sp.policy_reference)
# MAGIC WHERE dpc.policy_reference IS NULL

# COMMAND ----------

# close connection - synapse 
# synapseConnection.closeDWConnect(dw_conn)