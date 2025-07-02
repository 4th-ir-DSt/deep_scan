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

# MAGIC %md
# MAGIC <h3> Displaying DP signed amount records 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM red_dp_recon.dp_signeddata LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC <h4> Displaying measures of Signed Amount

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT measure_source, measure_group_name, measure_name FROM red_dp_recon.red_policy_measures_data WHERE measure_source = 'SIGNED'
# MAGIC ORDER BY measure_group_name

# COMMAND ----------

# MAGIC %md 
# MAGIC <h2> SIGNED AMOUNT PREMIUM

# COMMAND ----------

# MAGIC %md
# MAGIC <p> prepping the data for the premium view 

# COMMAND ----------

dpsigned_df = sql(
'''
WITH DP AS 
(
SELECT 
dpsigned.year_id YOA, dpsigned.Risk_Code Rk_Cd, dpsigned.BUSINESS_ENTITY Bu_Ent,
UPPER(dpsigned.policy_reference) AS policy_reference,  
dpsigned.measure_name,
SUM(dpsigned.measure_amount) AS SIGNED_AMOUNT
FROM red_dp_recon.dp_signeddata dpsigned
WHERE dpsigned.measure_name in ('Line_Gross_Net_Signed_Premium_SCC' , 'Line_Gross_Paid_Claim_SCC','Line_Gross_Gross_Signed_Premium_SCC','Line_Gross_Signed_Profit_Commission_SCC','Line_Gross_Signed_Reinstatement_Premium_SCC')
GROUP BY dpsigned.policy_reference, dpsigned.year_id, dpsigned.Risk_Code, dpsigned.BUSINESS_ENTITY,
dpsigned.measure_name
)
SELECT 
YOA, rk_cd, Bu_Ent,
policy_reference AS POLICY_REFERENCE,
ROUND(COALESCE(Line_Gross_Net_Signed_Premium_SCC,0),2) AS DP_LGN_PRM_SCC,
ROUND(COALESCE(Line_Gross_Paid_Claim_SCC,0),2) AS DP_LGP_CLAIM_SCC,
ROUND(COALESCE(Line_Gross_Gross_Signed_Premium_SCC,0),2) AS DP_LGG_PRM_SCC, 
ROUND(COALESCE(Line_Gross_Signed_Profit_Commission_SCC,0),2) AS DP_LPC_PRM_SCC,
ROUND(COALESCE(Line_Gross_Signed_Reinstatement_Premium_SCC,0),2) AS DP_LRIP_SCC
FROM 
(SELECT 
YOA, rk_cd, Bu_Ent,
Policy_reference,
measure_name,
SIGNED_AMOUNT
FROM DP
) t
PIVOT 
(
SUM(SIGNED_AMOUNT) FOR measure_name IN ('Line_Gross_Net_Signed_Premium_SCC' , 'Line_Gross_Paid_Claim_SCC','Line_Gross_Gross_Signed_Premium_SCC','Line_Gross_Signed_Profit_Commission_SCC','Line_Gross_Signed_Reinstatement_Premium_SCC')
) 
ORDER BY policy_reference
''')

# COMMAND ----------

dpsigned_df.createOrReplaceTempView("vw_dpsigned")

# COMMAND ----------

redsigned_df = sql(
'''WITH RED AS
(
SELECT 
redsigned.Year_of_Account YOA, redsigned.Risk_Code rk_cd, rtrim(ltrim(redsigned.BUSINESS_ENTITY)) Bu_Ent,
UPPER(redsigned.policy_reference) AS policy_reference, 
redsigned.measure_name,
SUM(redsigned.measure_amount) AS SIGNED_AMOUNT
FROM red_dp_recon.red_policy_measures_data redsigned
WHERE redsigned.measure_name in (
'USM_Premium','USM_Additional_Premium','USM_Return_Premium','USM_Claim','USM_Refund_Claim','USM_ReInstatement_Additional_Premium' , 'USM_ReInstatement_Premium' , 'USM_ReInstatement_Return_Premium','USM_Additional_Premium_Gross' , 'USM_Premium_Gross' , 'USM_Profit_Commission_Additional_Premium_Gross' , 'USM_Profit_Commission_Premium_Gross' , 'USM_Profit_Commission_Return_Premium_Gross' , 'USM_Return_Premium_Gross','USM_Profit_Commission_Additional_Premium' , 'USM_Profit_Commission_Premium' , 'USM_Profit_Commission_Return_Premium'
)
GROUP BY redsigned.policy_reference, redsigned.Year_of_Account,  redsigned.Risk_Code, redsigned.BUSINESS_ENTITY,
redsigned.measure_name
)
SELECT 
YOA, Rk_cd, Bu_Ent,
policy_reference AS POLICY_REFERENCE,
(ROUND(COALESCE(USM_Premium,0),2) + ROUND(COALESCE(USM_Additional_Premium,0),2) + ROUND(COALESCE(USM_Return_Premium,0),2) ) AS RED_LGN_PRM_SCC,
(ROUND(COALESCE(USM_Claim,0),2) + ROUND(COALESCE(USM_Refund_Claim,0),2) ) AS RED_LGP_CLAIM_SCC,
((ROUND(COALESCE(USM_ReInstatement_Return_Premium,0),2))+(ROUND(COALESCE(USM_ReInstatement_Return_Premium,0),2))+(ROUND(COALESCE(USM_ReInstatement_Return_Premium,0),2))) AS RED_LRIP_SCC,

((ROUND(COALESCE(USM_Additional_Premium_Gross,0),2))+
(ROUND(COALESCE(USM_Premium_Gross,0),2))+
(ROUND(COALESCE(USM_Profit_Commission_Additional_Premium_Gross,0),2))+
(ROUND(COALESCE(USM_Profit_Commission_Premium_Gross,0),2))+
(ROUND(COALESCE(USM_Profit_Commission_Return_Premium_Gross,0),2))+
(ROUND(COALESCE(USM_Return_Premium_Gross,0),2))) AS RED_LGG_PRM_SCC,

((ROUND(COALESCE(USM_Profit_Commission_Additional_Premium,0),2))+(ROUND(COALESCE(USM_Profit_Commission_Premium,0),2))+(ROUND(COALESCE(USM_Profit_Commission_Return_Premium,0),2))) AS RED_LPC_PRM_SCC
FROM 
(SELECT 
yoa, Rk_cd, Bu_Ent,
Policy_reference,
measure_name,
SIGNED_AMOUNT
FROM RED
) t
PIVOT 
(
SUM(SIGNED_AMOUNT) FOR measure_name IN (
'USM_Premium','USM_Additional_Premium','USM_Return_Premium','USM_Claim','USM_Refund_Claim','USM_ReInstatement_Additional_Premium' , 'USM_ReInstatement_Premium' , 'USM_ReInstatement_Return_Premium','USM_Additional_Premium_Gross' , 'USM_Premium_Gross' , 'USM_Profit_Commission_Additional_Premium_Gross' , 'USM_Profit_Commission_Premium_Gross' , 'USM_Profit_Commission_Return_Premium_Gross' , 'USM_Return_Premium_Gross','USM_Profit_Commission_Additional_Premium' , 'USM_Profit_Commission_Premium' , 'USM_Profit_Commission_Return_Premium'
) 
) 
ORDER BY policy_reference
''')

# COMMAND ----------

redsigned_df.createOrReplaceTempView("vw_redsigned")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_dpsigned limit 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT policy_reference,COUNT(1) 
# MAGIC FROM vw_dpsigned 
# MAGIC GROUP BY policy_reference
# MAGIC HAVING COUNT(1)>1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT policy_reference,COUNT(1) 
# MAGIC FROM vw_redsigned 
# MAGIC GROUP BY policy_reference
# MAGIC HAVING COUNT(1)>1

# COMMAND ----------

# MAGIC %md
# MAGIC <h5> VARIANCE OUTPUT SIGNED AMOUNT - YEAR OF ACCOUNT LEVEL FOR COMMON POLICIES

# COMMAND ----------

# MAGIC %sql
# MAGIC with dps_yoa as
# MAGIC (
# MAGIC select vdps.yoa YOA, 
# MAGIC sum(round(COALESCE(vdps.DP_LGN_PRM_SCC,0),2) ) DP_LGN_PRM_SCC,
# MAGIC sum(round(COALESCE(vdps.DP_LGP_CLAIM_SCC,0),2) ) DP_LGP_CLAIM_SCC,
# MAGIC sum(round(COALESCE(vdps.DP_LGG_PRM_SCC,0),2) ) DP_LGG_PRM_SCC, 
# MAGIC sum(round(COALESCE(vdps.DP_LPC_PRM_SCC,0),2) ) DP_LPC_PRM_SCC,
# MAGIC sum(round(COALESCE(vdps.DP_LRIP_SCC,0),2) ) DP_LRIP_SCC
# MAGIC from vw_dpsigned vdps
# MAGIC WHERE policy_reference in (SELECT vdpsd.policy_reference FROM vw_dpsigned vdpsd JOIN vw_redsigned vrsd ON vdpsd.policy_reference = vrsd.policy_reference)
# MAGIC group by vdps.yoa
# MAGIC )
# MAGIC , red_yoa as
# MAGIC (
# MAGIC select vrs.yoa YOA, 
# MAGIC sum(round(COALESCE(vrs.RED_LGN_PRM_SCC,0),2)) RED_LGN_PRM_SCC,
# MAGIC sum(round(COALESCE(vrs.RED_LGP_CLAIM_SCC,0),2)) RED_LGP_CLAIM_SCC,
# MAGIC sum(round(COALESCE(vrs.RED_LGG_PRM_SCC,0),2)) RED_LGG_PRM_SCC,
# MAGIC sum(round(COALESCE(vrs.RED_LPC_PRM_SCC,0),2)) RED_LPC_PRM_SCC,
# MAGIC sum(round(COALESCE(vrs.RED_LRIP_SCC,0),2)) RED_LRIP_SCC
# MAGIC from vw_redsigned vrs
# MAGIC WHERE policy_reference in (SELECT vdpsr.policy_reference FROM vw_dpsigned vdpsr JOIN vw_redsigned vrsr ON vdpsr.policy_reference = vrsr.policy_reference)
# MAGIC group by vrs.yoa
# MAGIC )
# MAGIC select dps_yoa.YOA, 
# MAGIC round(dps_yoa.DP_LGN_PRM_SCC, 2) 	DP_LGN_PRM_SCC, 
# MAGIC round(red_yoa.RED_LGN_PRM_SCC,2) 	RED_LGN_PRM_SCC,
# MAGIC round(dps_yoa.DP_LGP_CLAIM_SCC, 2) DP_LGP_CLAIM_SCC, 
# MAGIC round(red_yoa.RED_LGP_CLAIM_SCC,2) RED_LGP_CLAIM_SCC,
# MAGIC round(dps_yoa.DP_LGG_PRM_SCC, 2) 	DP_LGG_PRM_SCC, 
# MAGIC round(red_yoa.RED_LGG_PRM_SCC,2) 	RED_LGG_PRM_SCC,
# MAGIC round(dps_yoa.DP_LPC_PRM_SCC, 2) 	DP_LPC_PRM_SCC, 
# MAGIC round(red_yoa.RED_LPC_PRM_SCC,2) 	RED_LPC_PRM_SCC,
# MAGIC round(dps_yoa.DP_LRIP_SCC, 2) 		DP_LRIP_SCC, 
# MAGIC round(red_yoa.RED_LRIP_SCC,2) 		RED_LRIP_SCC
# MAGIC from dps_yoa 
# MAGIC join red_yoa on dps_YOA.YOA = red_yoa.YOA 
# MAGIC order by 1 desc

# COMMAND ----------

# MAGIC %md
# MAGIC <h5> VARIANCE OUTPUT SIGNED AMOUNT - POLICY LEVEL

# COMMAND ----------

# MAGIC %sql
# MAGIC with dps_pol as
# MAGIC (
# MAGIC select vdps.policy_reference POLICY_REFERENCE, 
# MAGIC sum(round(COALESCE(vdps.DP_LGN_PRM_SCC,0),2) ) DP_LGN_PRM_SCC,
# MAGIC sum(round(COALESCE(vdps.DP_LGP_CLAIM_SCC,0),2) ) DP_LGP_CLAIM_SCC,
# MAGIC sum(round(COALESCE(vdps.DP_LGG_PRM_SCC,0),2) ) DP_LGG_PRM_SCC, 
# MAGIC sum(round(COALESCE(vdps.DP_LPC_PRM_SCC,0),2) ) DP_LPC_PRM_SCC,
# MAGIC sum(round(COALESCE(vdps.DP_LRIP_SCC,0),2) ) DP_LRIP_SCC
# MAGIC from vw_dpsigned vdps
# MAGIC group by vdps.policy_reference
# MAGIC )
# MAGIC , red_pol as
# MAGIC (
# MAGIC select vrs.POLICY_REFERENCE POLICY_REFERENCE,
# MAGIC sum(round(COALESCE(vrs.RED_LGN_PRM_SCC,0),2)) RED_LGN_PRM_SCC,
# MAGIC sum(round(COALESCE(vrs.RED_LGP_CLAIM_SCC,0),2)) RED_LGP_CLAIM_SCC,
# MAGIC sum(round(COALESCE(vrs.RED_LGG_PRM_SCC,0),2)) RED_LGG_PRM_SCC,
# MAGIC sum(round(COALESCE(vrs.RED_LPC_PRM_SCC,0),2)) RED_LPC_PRM_SCC,
# MAGIC sum(round(COALESCE(vrs.RED_LRIP_SCC,0),2)) RED_LRIP_SCC
# MAGIC from vw_redsigned vrs
# MAGIC group by vrs.policy_reference
# MAGIC )
# MAGIC 
# MAGIC SELECT 
# MAGIC 'Common_Policy' as Policy_Availability,
# MAGIC dps_pol.policy_reference, 
# MAGIC round(dps_pol.DP_LGN_PRM_SCC, 2) 	DP_LGN_PRM_SCC, 
# MAGIC round(red_pol.RED_LGN_PRM_SCC,2) 	RED_LGN_PRM_SCC,
# MAGIC ROUND(abs( abs( round(COALESCE(dps_pol.DP_LGN_PRM_SCC,0),2) ) - abs( round(COALESCE(red_pol.RED_LGN_PRM_SCC,0),2) ) ),2) VARIANCE_LGN_PRM_SCC,
# MAGIC round(dps_pol.DP_LGP_CLAIM_SCC, 2) DP_LGP_CLAIM_SCC, 
# MAGIC round(red_pol.RED_LGP_CLAIM_SCC,2) RED_LGP_CLAIM_SCC,
# MAGIC ROUND(abs( abs( round(COALESCE(dps_pol.DP_LGP_CLAIM_SCC,0),2) ) - abs( round(COALESCE(red_pol.RED_LGP_CLAIM_SCC,0),2) ) ),2) VARIANCE_LGP_CLAIM_SCC,
# MAGIC round(dps_pol.DP_LGG_PRM_SCC, 2) 	DP_LGG_PRM_SCC, 
# MAGIC round(red_pol.RED_LGG_PRM_SCC,2) 	RED_LGG_PRM_SCC,
# MAGIC ROUND(abs( abs( round(COALESCE(dps_pol.DP_LGG_PRM_SCC,0),2) ) - abs( round(COALESCE(red_pol.RED_LGG_PRM_SCC,0),2) ) ),2) VARIANCE_LGG_PRM_SCC,
# MAGIC round(dps_pol.DP_LPC_PRM_SCC, 2) 	DP_LPC_PRM_SCC, 
# MAGIC round(red_pol.RED_LPC_PRM_SCC,2) 	RED_LPC_PRM_SCC,
# MAGIC ROUND(abs( abs( round(COALESCE(dps_pol.DP_LPC_PRM_SCC,0),2) ) - abs( round(COALESCE(red_pol.RED_LPC_PRM_SCC,0),2) ) ),2) VARIANCE_LPC_PRM_SCC,
# MAGIC round(dps_pol.DP_LRIP_SCC, 2) 		DP_LRIP_SCC, 
# MAGIC round(red_pol.RED_LRIP_SCC,2) 		RED_LRIP_SCC,
# MAGIC ROUND(abs( abs( round(COALESCE(dps_pol.DP_LRIP_SCC,0),2) ) - abs( round(COALESCE(red_pol.RED_LRIP_SCC,0),2) ) ),2) VARIANCE_LRIP_SCC
# MAGIC FROM dps_pol
# MAGIC JOIN red_pol ON dps_pol.policy_reference = red_pol.policy_reference
# MAGIC 
# MAGIC UNION
# MAGIC 
# MAGIC SELECT 
# MAGIC 'Missing_Policy_in_Red' as Policy_Availability,
# MAGIC dps_pol.policy_reference, 
# MAGIC round(dps_pol.DP_LGN_PRM_SCC, 2) 	DP_LGN_PRM_SCC, 
# MAGIC '0' 	RED_LGN_PRM_SCC,
# MAGIC ROUND(abs( abs( round(COALESCE(dps_pol.DP_LGN_PRM_SCC,0),2) ) - abs( round(COALESCE(red_pol.RED_LGN_PRM_SCC,0),2) ) ),2) VARIANCE_LGN_PRM_SCC,
# MAGIC round(dps_pol.DP_LGP_CLAIM_SCC, 2) DP_LGP_CLAIM_SCC, 
# MAGIC '0' RED_LGP_CLAIM_SCC,
# MAGIC ROUND(abs( abs( round(COALESCE(dps_pol.DP_LGP_CLAIM_SCC,0),2) ) - abs( round(COALESCE(red_pol.RED_LGP_CLAIM_SCC,0),2) ) ),2) VARIANCE_LGP_CLAIM_SCC,
# MAGIC round(dps_pol.DP_LGG_PRM_SCC, 2) 	DP_LGG_PRM_SCC, 
# MAGIC '0' 	RED_LGG_PRM_SCC,
# MAGIC ROUND(abs( abs( round(COALESCE(dps_pol.DP_LGG_PRM_SCC,0),2) ) - abs( round(COALESCE(red_pol.RED_LGG_PRM_SCC,0),2) ) ),2) VARIANCE_LGG_PRM_SCC,
# MAGIC round(dps_pol.DP_LPC_PRM_SCC, 2) 	DP_LPC_PRM_SCC, 
# MAGIC '0' 	RED_LPC_PRM_SCC,
# MAGIC ROUND(abs( abs( round(COALESCE(dps_pol.DP_LPC_PRM_SCC,0),2) ) - abs( round(COALESCE(red_pol.RED_LPC_PRM_SCC,0),2) ) ),2) VARIANCE_LPC_PRM_SCC,
# MAGIC round(dps_pol.DP_LRIP_SCC, 2) 		DP_LRIP_SCC, 
# MAGIC '0' 		RED_LRIP_SCC,
# MAGIC ROUND(abs( abs( round(COALESCE(dps_pol.DP_LRIP_SCC,0),2) ) - abs( round(COALESCE(red_pol.RED_LRIP_SCC,0),2) ) ),2) VARIANCE_LRIP_SCC
# MAGIC FROM dps_pol
# MAGIC LEFT JOIN red_pol ON dps_pol.policy_reference = red_pol.policy_reference
# MAGIC WHERE red_pol.policy_reference is NULL
# MAGIC 
# MAGIC UNION
# MAGIC 
# MAGIC SELECT 
# MAGIC 'Missing_Policy_in_DP' as Policy_Availability,
# MAGIC dps_pol.policy_reference, 
# MAGIC '0'	DP_LGN_PRM_SCC, 
# MAGIC round(red_pol.RED_LGN_PRM_SCC,2) 	RED_LGN_PRM_SCC,
# MAGIC ROUND(abs( abs( round(COALESCE(dps_pol.DP_LGN_PRM_SCC,0),2) ) - abs( round(COALESCE(red_pol.RED_LGN_PRM_SCC,0),2) ) ),2) VARIANCE_LGN_PRM_SCC,
# MAGIC '0' DP_LGP_CLAIM_SCC, 
# MAGIC round(red_pol.RED_LGP_CLAIM_SCC,2) RED_LGP_CLAIM_SCC,
# MAGIC ROUND(abs( abs( round(COALESCE(dps_pol.DP_LGP_CLAIM_SCC,0),2) ) - abs( round(COALESCE(red_pol.RED_LGP_CLAIM_SCC,0),2) ) ),2) VARIANCE_LGP_CLAIM_SCC,
# MAGIC '0' 	DP_LGG_PRM_SCC, 
# MAGIC round(red_pol.RED_LGG_PRM_SCC,2) 	RED_LGG_PRM_SCC,
# MAGIC ROUND(abs( abs( round(COALESCE(dps_pol.DP_LGG_PRM_SCC,0),2) ) - abs( round(COALESCE(red_pol.RED_LGG_PRM_SCC,0),2) ) ),2) VARIANCE_LGG_PRM_SCC,
# MAGIC '0' 	DP_LPC_PRM_SCC, 
# MAGIC round(red_pol.RED_LPC_PRM_SCC,2) 	RED_LPC_PRM_SCC,
# MAGIC ROUND(abs( abs( round(COALESCE(dps_pol.DP_LPC_PRM_SCC,0),2) ) - abs( round(COALESCE(red_pol.RED_LPC_PRM_SCC,0),2) ) ),2) VARIANCE_LPC_PRM_SCC,
# MAGIC '0' 		DP_LRIP_SCC, 
# MAGIC round(red_pol.RED_LRIP_SCC,2) 		RED_LRIP_SCC,
# MAGIC ROUND(abs( abs( round(COALESCE(dps_pol.DP_LRIP_SCC,0),2) ) - abs( round(COALESCE(red_pol.RED_LRIP_SCC,0),2) ) ),2) VARIANCE_LRIP_SCC
# MAGIC FROM dps_pol
# MAGIC RIGHT JOIN red_pol ON dps_pol.policy_reference = red_pol.policy_reference
# MAGIC WHERE dps_pol.policy_reference is NULL

# COMMAND ----------

# MAGIC %md
# MAGIC <h5> VARIANCE OUTPUT SIGNED AMOUNT - BUSINESS ENTITY AND RISK  CODE LEVEL

# COMMAND ----------

# MAGIC %sql
# MAGIC with dps as
# MAGIC (
# MAGIC select 
# MAGIC vdps.policy_reference POLICY_REFERENCE, 
# MAGIC vdps.Rk_cd RISK_CODE, 
# MAGIC vdps.Bu_Ent BUSINESS_ENTITY,
# MAGIC sum(round(COALESCE(vdps.DP_LGN_PRM_SCC,0),2) ) DP_LGN_PRM_SCC,
# MAGIC sum(round(COALESCE(vdps.DP_LGP_CLAIM_SCC,0),2) ) DP_LGP_CLAIM_SCC,
# MAGIC sum(round(COALESCE(vdps.DP_LGG_PRM_SCC,0),2) ) DP_LGG_PRM_SCC, 
# MAGIC sum(round(COALESCE(vdps.DP_LPC_PRM_SCC,0),2) ) DP_LPC_PRM_SCC,
# MAGIC sum(round(COALESCE(vdps.DP_LRIP_SCC,0),2) ) DP_LRIP_SCC
# MAGIC from vw_dpsigned vdps
# MAGIC WHERE vdps.policy_reference in (SELECT vdpsr.policy_reference FROM vw_dpsigned vdpsr JOIN vw_redsigned vrsr ON vdpsr.policy_reference = vrsr.policy_reference)
# MAGIC group by (vdps.policy_reference + vdps.Rk_cd + vdps.Bu_Ent ),
# MAGIC vdps.policy_reference, 
# MAGIC vdps.Rk_cd, 
# MAGIC vdps.Bu_Ent
# MAGIC )
# MAGIC , red as
# MAGIC (
# MAGIC select 
# MAGIC vrs.POLICY_REFERENCE POLICY_REFERENCE,
# MAGIC vrs.Rk_cd RISK_CODE, 
# MAGIC vrs.Bu_Ent BUSINESS_ENTITY,
# MAGIC sum(round(COALESCE(vrs.RED_LGN_PRM_SCC,0),2)) RED_LGN_PRM_SCC,
# MAGIC sum(round(COALESCE(vrs.RED_LGP_CLAIM_SCC,0),2)) RED_LGP_CLAIM_SCC,
# MAGIC sum(round(COALESCE(vrs.RED_LGG_PRM_SCC,0),2)) RED_LGG_PRM_SCC,
# MAGIC sum(round(COALESCE(vrs.RED_LPC_PRM_SCC,0),2)) RED_LPC_PRM_SCC,
# MAGIC sum(round(COALESCE(vrs.RED_LRIP_SCC,0),2)) RED_LRIP_SCC
# MAGIC from vw_redsigned vrs
# MAGIC WHERE policy_reference in (SELECT vdpsr.policy_reference FROM vw_dpsigned vdpsr JOIN vw_redsigned vrsr ON vdpsr.policy_reference = vrsr.policy_reference)
# MAGIC group by (vrs.policy_reference + vrs.Rk_cd + vrs.Bu_Ent ),
# MAGIC vrs.policy_reference, 
# MAGIC vrs.Rk_cd, 
# MAGIC vrs.Bu_Ent
# MAGIC )
# MAGIC 
# MAGIC SELECT 
# MAGIC dps_pol.POLICY_REFERENCE, 
# MAGIC dps_pol.RISK_CODE, 
# MAGIC dps_pol.BUSINESS_ENTITY,
# MAGIC round(dps_pol.DP_LGN_PRM_SCC, 2) 	DP_LGN_PRM_SCC, 
# MAGIC round(red_pol.RED_LGN_PRM_SCC,2) 	RED_LGN_PRM_SCC,
# MAGIC ROUND(abs( abs( round(COALESCE(dps_pol.DP_LGN_PRM_SCC,0),2) ) - abs( round(COALESCE(red_pol.RED_LGN_PRM_SCC,0),2) ) ),2) VARIANCE_LGN_PRM_SCC,
# MAGIC round(dps_pol.DP_LGP_CLAIM_SCC, 2) DP_LGP_CLAIM_SCC, 
# MAGIC round(red_pol.RED_LGP_CLAIM_SCC,2) RED_LGP_CLAIM_SCC,
# MAGIC ROUND(abs( abs( round(COALESCE(dps_pol.DP_LGP_CLAIM_SCC,0),2) ) - abs( round(COALESCE(red_pol.RED_LGP_CLAIM_SCC,0),2) ) ),2) VARIANCE_LGP_CLAIM_SCC,
# MAGIC round(dps_pol.DP_LGG_PRM_SCC, 2) 	DP_LGG_PRM_SCC, 
# MAGIC round(red_pol.RED_LGG_PRM_SCC,2) 	RED_LGG_PRM_SCC,
# MAGIC ROUND(abs( abs( round(COALESCE(dps_pol.DP_LGG_PRM_SCC,0),2) ) - abs( round(COALESCE(red_pol.RED_LGG_PRM_SCC,0),2) ) ),2) VARIANCE_LGG_PRM_SCC,
# MAGIC round(dps_pol.DP_LPC_PRM_SCC, 2) 	DP_LPC_PRM_SCC, 
# MAGIC round(red_pol.RED_LPC_PRM_SCC,2) 	RED_LPC_PRM_SCC,
# MAGIC ROUND(abs( abs( round(COALESCE(dps_pol.DP_LPC_PRM_SCC,0),2) ) - abs( round(COALESCE(red_pol.RED_LPC_PRM_SCC,0),2) ) ),2) VARIANCE_LPC_PRM_SCC,
# MAGIC round(dps_pol.DP_LRIP_SCC, 2) 		DP_LRIP_SCC, 
# MAGIC round(red_pol.RED_LRIP_SCC,2) 		RED_LRIP_SCC,
# MAGIC ROUND(abs( abs( round(COALESCE(dps_pol.DP_LRIP_SCC,0),2) ) - abs( round(COALESCE(red_pol.RED_LRIP_SCC,0),2) ) ),2) VARIANCE_LRIP_SCC
# MAGIC FROM dps dps_pol
# MAGIC JOIN red red_pol ON (dps_pol.POLICY_REFERENCE = red_pol.POLICY_REFERENCE) and (red_pol.RISK_CODE = dps_pol.RISK_CODE) and (red_pol.BUSINESS_ENTITY = dps_pol.BUSINESS_ENTITY)

# COMMAND ----------

# close connection - synapse 
# synapseConnection.closeDWConnect(dw_conn)