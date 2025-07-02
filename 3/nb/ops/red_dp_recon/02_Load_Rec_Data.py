# Databricks notebook source
# MAGIC %md
# MAGIC <h1> Load EPI Data </h1>

# COMMAND ----------

# DBTITLE 1,DP EPI Data
dpepi_df = sql(
'''
WITH DP AS 
(
    SELECT
        concat_ws('_',UPPER(dpepi.policy_reference),  dpepi.Year_of_Account,  dpepi.business_entity, dpepi.Sub_Class_Code, dpepi.SCC, dpepi.risk_code) AS DP_KEY
        ,UPPER(dpepi.policy_reference) AS policy_reference 
        ,dpepi.Year_of_Account
        ,dpepi.business_entity
        ,dpepi.Sub_Class_Code
        ,dpepi.SCC
        ,dpepi.risk_code
        ,dpepi.measure_name
        ,SUM(dpepi.measure_amount) AS EPI_AMOUNT
    FROM red_dp_recon.dp_epidata dpepi
    WHERE dpepi.measure_name in ('Line_Gross_Net_Estimated_Premium_SCC', 'Line_Gross_Gross_Estimated_Premium_SCC')
    GROUP BY dpepi.policy_reference,dpepi.Year_of_Account, dpepi.business_entity, dpepi.risk_code, dpepi.measure_name
                , dpepi.risk_code, dpepi.Sub_Class_Code, dpepi.SCC 
)
SELECT
    DP_KEY
    , policy_reference AS POLICY_REFERENCE
    , Year_of_Account
    , business_entity
    , Sub_Class_Code
    , SCC
    , risk_code
    , ROUND(COALESCE(Line_Gross_Net_Estimated_Premium_SCC,0),2) AS Line_Gross_Net_Estimated_Premium_SCC_DP
    , ROUND(COALESCE(Line_Gross_Gross_Estimated_Premium_SCC,0),2) AS Line_Gross_Gross_Estimated_Premium_SCC_DP
FROM 
(
    SELECT
    DP_KEY
    , Policy_reference
    , Year_of_Account    
    , business_entity
    , Sub_Class_Code
    , SCC    
    , risk_code
    , measure_name
    , EPI_AMOUNT
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

# DBTITLE 1,RED EPI Data

redepi_df = sql(
'''WITH RED AS
(
    SELECT
        concat_ws('_',UPPER(redepi.policy_reference),  cast(redepi.Year_of_Account as bigint),  redepi.business_entity, redepi.SubClass, redepi.SCC,  redepi.risk_code) AS RED_KEY
      , UPPER(redepi.policy_reference) AS policy_reference 
      , cast(redepi.Year_of_Account as bigint) Year_of_Account
      , redepi.business_entity
      , redepi.SubClass
      , redepi.SCC
      , redepi.risk_code
      , redepi.measure_name
      , SUM(redepi.measure_amount) AS EPI_AMOUNT
    FROM red_dp_recon.red_policy_measures_data redepi
    WHERE redepi.measure_name in ('GNWPI_Syndicate_SCC_PMD', 'GGWPI_Syndicate_SCC_PMD')
    GROUP BY redepi.policy_reference, redepi.Year_of_Account, redepi.business_entity, redepi.risk_code, redepi.measure_name , redepi.SubClass, redepi.SCC
)
SELECT
    RED_KEY
    , policy_reference AS POLICY_REFERENCE
    , Year_of_Account
    , business_entity
    , SubClass
    , SCC    
    , risk_code    
    , (ROUND(COALESCE(GNWPI_Syndicate_SCC_PMD,0),2) ) AS Line_Gross_Net_Estimated_Premium_SCC_RED
    , (ROUND(COALESCE(GGWPI_Syndicate_SCC_PMD),2) ) AS Line_Gross_Gross_Estimated_Premium_SCC_RED

FROM 
(
    SELECT
        RED_KEY        
        , Policy_reference
        , Year_of_Account
        , business_entity        
        , SubClass
        , SCC            
        , risk_code 
        , measure_name
        , EPI_AMOUNT
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

# DBTITLE 1,Join RED and DP
var_pivot_df = sql(
'''   
SELECT
    dpepi.dp_key
    , redepi.red_key
    , coalesce(dpepi.policy_reference, redepi.policy_reference) as policy_reference
    , coalesce(dpepi.year_of_account, redepi.year_of_account) as year_of_account
    , coalesce(dpepi.business_entity, redepi.business_entity) as business_entity
    , coalesce(dpepi.Sub_Class_Code, redepi.SubClass) as Sub_Class_Code
    , coalesce(dpepi.SCC, redepi.SCC) as SCC
    , coalesce(dpepi.risk_code, redepi.risk_code) as risk_code
    , coalesce(round(dpepi.Line_Gross_Net_Estimated_Premium_SCC_DP,2),0) Line_Gross_Net_Estimated_Premium_SCC_DP 
    , coalesce(round(redepi.Line_Gross_Net_Estimated_Premium_SCC_RED,2),0) Line_Gross_Net_Estimated_Premium_SCC_RED
    , (coalesce(round(dpepi.Line_Gross_Net_Estimated_Premium_SCC_DP,2),0) - coalesce(round(redepi.Line_Gross_Net_Estimated_Premium_SCC_RED,2),0)) Line_Gross_Net_Estimated_Premium_SCC_VAR

    , coalesce(round(dpepi.Line_Gross_Gross_Estimated_Premium_SCC_DP,2),0) Line_Gross_Gross_Estimated_Premium_SCC_DP
    , coalesce(round(redepi.Line_Gross_Gross_Estimated_Premium_SCC_RED,2),0) Line_Gross_Gross_Estimated_Premium_SCC_RED        
    , (coalesce(round(dpepi.Line_Gross_Gross_Estimated_Premium_SCC_DP,2),0) - coalesce(round(redepi.Line_Gross_Gross_Estimated_Premium_SCC_RED,2),0)) Line_Gross_Gross_Estimated_Premium_SCC_VAR

    , excl.reports_epi
FROM vw_dpepi dpepi
  full outer JOIN vw_redepi redepi 
      ON dpepi.dp_key = redepi.red_key
  left outer join red_dp_recon.red_epi_variance excl
        on Upper(redepi.policy_reference) = Upper(excl.policy_reference)
'''        
)#.display()

# COMMAND ----------

var_pivot_df.createOrReplaceTempView("vw_pivot")

# COMMAND ----------

# DBTITLE 1,UNPIVOT the data
pvt_epi_var_df = spark.sql(
'''
select 
    policy_reference
    , dp_key
    , red_key
    , year_of_account
    , business_entity
    , Sub_Class_Code
    , SCC
    , risk_code
    , reports_epi
    , stack(6,
                'Line_Gross_Net_Estimated_Premium_SCC_DP',Line_Gross_Net_Estimated_Premium_SCC_DP
                , 'Line_Gross_Net_Estimated_Premium_SCC_RED',Line_Gross_Net_Estimated_Premium_SCC_RED
                , 'Line_Gross_Net_Estimated_Premium_SCC_VAR',Line_Gross_Net_Estimated_Premium_SCC_VAR
                , 'Line_Gross_Gross_Estimated_Premium_SCC_DP',Line_Gross_Gross_Estimated_Premium_SCC_DP         
                , 'Line_Gross_Gross_Estimated_Premium_SCC_RED',Line_Gross_Gross_Estimated_Premium_SCC_RED
                , 'Line_Gross_Gross_Estimated_Premium_SCC_VAR',Line_Gross_Gross_Estimated_Premium_SCC_VAR 
             ) as (Measure, Amount) from vw_pivot'''
)#.display()

# COMMAND ----------

pvt_epi_var_df.createOrReplaceTempView("vw_epi_var")

# COMMAND ----------

# DBTITLE 1,Into Final dataframe
epi_fnl_var_df = sql(
''' 
select
  policy_reference
  , 'NA' AS Unique_Claim_Reference
  , dp_key
  , red_key
  , Year_of_Account
  , Business_Entity
  , Sub_Class_Code
  , SCC  
  , Risk_Code
  , Reports_EPI
  , case 
      when measure in ('Line_Gross_Net_Estimated_Premium_SCC_DP', 'Line_Gross_Net_Estimated_Premium_SCC_RED', 'Line_Gross_Net_Estimated_Premium_SCC_VAR', 'Line_Gross_Gross_Estimated_Premium_SCC_DP', 'Line_Gross_Gross_Estimated_Premium_SCC_RED', 'Line_Gross_Gross_Estimated_Premium_SCC_VAR')
        then 'EPI'
    end as Measure_Source
  , case
      when measure in ('Line_Gross_Net_Estimated_Premium_SCC_DP', 'Line_Gross_Net_Estimated_Premium_SCC_RED', 'Line_Gross_Net_Estimated_Premium_SCC_VAR')
        then 'EPI_GN_SCC'
      when measure in ('Line_Gross_Gross_Estimated_Premium_SCC_DP', 'Line_Gross_Gross_Estimated_Premium_SCC_RED', 'Line_Gross_Gross_Estimated_Premium_SCC_VAR')
        then 'EPI_GG_SCC'
    end as Measure_Group_Name
  , case
      when measure in ('Line_Gross_Net_Estimated_Premium_SCC_DP', 'Line_Gross_Gross_Estimated_Premium_SCC_DP')
          then 'DP'
      when measure in ('Line_Gross_Net_Estimated_Premium_SCC_RED', 'Line_Gross_Gross_Estimated_Premium_SCC_RED')
          then 'RED'
      when measure in ('Line_Gross_Net_Estimated_Premium_SCC_VAR', 'Line_Gross_Gross_Estimated_Premium_SCC_VAR')
          then 'VAR'
    end as Data_Type
  , Measure
  , Amount
from vw_epi_var 
''' 
)

# COMMAND ----------

epi_fnl_var_df.createOrReplaceTempView("vw_epi_variance")

# COMMAND ----------

# DBTITLE 1,Load data to storage account
(epi_fnl_var_df
 .coalesce(1)
 .write
 .mode("overwrite")
 .option("header", "true")
 .format("delta")
 .save("dbfs:/mnt/raw/delta/red_dp_recon/RED_DP_Variance"))

# COMMAND ----------

# MAGIC %md
# MAGIC <h1> Load Signed Data </h1>

# COMMAND ----------

# DBTITLE 1,DP Signed Data
dpsigned_df = sql(
'''
WITH DP AS
(
  SELECT
    concat_ws('_',UPPER(dpsigned.policy_reference),  dpsigned.year_id,  dpsigned.business_entity,  dpsigned.Sub_Class_Code, dpsigned.SCC, dpsigned.risk_code) AS DP_KEY
    , UPPER(dpsigned.policy_reference) AS policy_reference
    , dpsigned.year_id
    , dpsigned.BUSINESS_ENTITY
    , dpsigned.Sub_Class_Code
    , dpsigned.SCC
    , dpsigned.Risk_Code 
    , dpsigned.measure_name
    , SUM(dpsigned.measure_amount) AS SIGNED_AMOUNT
  FROM red_dp_recon.dp_signeddata dpsigned
  WHERE dpsigned.measure_name in ('Line_Gross_Net_Signed_Premium_SCC' , 'Line_Gross_Paid_Claim_SCC','Line_Gross_Gross_Signed_Premium_SCC','Line_Gross_Signed_Profit_Commission_SCC','Line_Gross_Signed_Reinstatement_Premium_SCC')
  GROUP BY dpsigned.policy_reference, dpsigned.year_id, dpsigned.Risk_Code, dpsigned.BUSINESS_ENTITY,
  dpsigned.measure_name,  dpsigned.Sub_Class_Code, dpsigned.SCC
)
SELECT
      DP_KEY
    , policy_reference
    , year_id
    , BUSINESS_ENTITY
    , Sub_Class_Code
    , SCC
    , Risk_Code
    , ROUND(COALESCE(Line_Gross_Gross_Signed_Premium_SCC,0),2) AS Line_Gross_Gross_Signed_Premium_SCC_DP
    , ROUND(COALESCE(Line_Gross_Net_Signed_Premium_SCC,0),2) AS Line_Gross_Net_Signed_Premium_SCC_DP
    , ROUND(COALESCE(Line_Gross_Paid_Claim_SCC,0),2) AS Line_Gross_Paid_Claim_SCC_DP
    , ROUND(COALESCE(Line_Gross_Signed_Profit_Commission_SCC,0),2) AS Line_Gross_Signed_Profit_Commission_SCC_DP
    , ROUND(COALESCE(Line_Gross_Signed_Reinstatement_Premium_SCC,0),2) AS Line_Gross_Signed_ReInstatement_Premium_SCC_DP
FROM
(
  SELECT
    DP_KEY
    , policy_reference
    , year_id
    , BUSINESS_ENTITY
    , Sub_Class_Code
    , SCC    
    , Risk_Code
    , measure_name
    , SIGNED_AMOUNT
  FROM DP
) s
PIVOT 
(
SUM(SIGNED_AMOUNT) FOR measure_name IN ('Line_Gross_Net_Signed_Premium_SCC' , 'Line_Gross_Paid_Claim_SCC','Line_Gross_Gross_Signed_Premium_SCC','Line_Gross_Signed_Profit_Commission_SCC','Line_Gross_Signed_Reinstatement_Premium_SCC')
) 
ORDER BY policy_reference
''')

# COMMAND ----------

dpsigned_df.createOrReplaceTempView("vw_dpsigned")

# COMMAND ----------

# DBTITLE 1,RED Signed Data
redsigned_df = sql(
'''
WITH RED AS
(
  SELECT
    concat_ws('_',UPPER(redsigned.policy_reference),  cast(redsigned.Year_of_Account as bigint),  redsigned.business_entity, redsigned.SubClass, redsigned.SCC, redsigned.risk_code) AS RED_KEY
    , UPPER(redsigned.policy_reference) AS policy_reference 
    , cast(redsigned.Year_of_Account as bigint) AS Year_of_Account
    , rtrim(ltrim(redsigned.BUSINESS_ENTITY)) AS BUSINESS_ENTITY
    , redsigned.SubClass
    , redsigned.SCC
    , redsigned.Risk_Code  
    , redsigned.measure_name
    , SUM(redsigned.measure_amount) AS SIGNED_AMOUNT
    FROM red_dp_recon.red_policy_measures_data redsigned
  WHERE redsigned.measure_name in (
  'USM_Premium','USM_Additional_Premium','USM_Return_Premium','USM_Claim','USM_Refund_Claim','USM_ReInstatement_Additional_Premium' , 'USM_ReInstatement_Premium' , 'USM_ReInstatement_Return_Premium','USM_Additional_Premium_Gross' , 'USM_Premium_Gross' , 'USM_Profit_Commission_Additional_Premium_Gross' , 'USM_Profit_Commission_Premium_Gross' , 'USM_Profit_Commission_Return_Premium_Gross' , 'USM_Return_Premium_Gross','USM_Profit_Commission_Additional_Premium' , 'USM_Profit_Commission_Premium' , 'USM_Profit_Commission_Return_Premium'
  )
  GROUP BY redsigned.policy_reference, redsigned.Year_of_Account,  redsigned.Risk_Code, redsigned.BUSINESS_ENTITY,
  redsigned.measure_name, redsigned.SubClass, redsigned.SCC
 )
 SELECT
      RED_KEY
      , policy_reference
      , Year_of_Account
      , BUSINESS_ENTITY
      , SubClass
      , SCC
      , Risk_Code      
      , ((ROUND(COALESCE(USM_Additional_Premium_Gross,0),2)) 
            + (ROUND(COALESCE(USM_Premium_Gross,0),2)) 
            --+ (ROUND(COALESCE(USM_Profit_Commission_Additional_Premium_Gross,0),2))
            --+ (ROUND(COALESCE(USM_Profit_Commission_Premium_Gross,0),2)) 
            --+ (ROUND(COALESCE(USM_Profit_Commission_Return_Premium_Gross,0),2))
            + (ROUND(COALESCE(USM_Return_Premium_Gross,0),2))) AS Line_Gross_Gross_Signed_Premium_SCC_RED
      
      , (ROUND(COALESCE(USM_Premium,0),2) + ROUND(COALESCE(USM_Additional_Premium,0),2) + ROUND(COALESCE(USM_Return_Premium,0),2) ) AS Line_Gross_Net_Signed_Premium_SCC_RED
      
      , (ROUND(COALESCE(USM_Claim,0),2) + ROUND(COALESCE(USM_Refund_Claim,0),2) ) AS Line_Gross_Paid_Claim_SCC_RED
      
      , ((ROUND(COALESCE(USM_Profit_Commission_Additional_Premium,0),2)) + (ROUND(COALESCE(USM_Profit_Commission_Premium,0),2))
          +(ROUND(COALESCE(USM_Profit_Commission_Return_Premium,0),2))) AS Line_Gross_Signed_Profit_Commission_SCC_RED
      
      , ((ROUND(COALESCE(USM_ReInstatement_Premium,0),2)) + (ROUND(COALESCE(USM_ReInstatement_Return_Premium,0),2)) + 
          (ROUND(COALESCE(USM_ReInstatement_Additional_Premium,0),2))) AS Line_Gross_Signed_ReInstatement_Premium_SCC_RED
 FROM
 (
    SELECT
      RED_KEY
      , policy_reference
      , Year_of_Account
      , BUSINESS_ENTITY
      , SubClass
      , SCC      
      , Risk_Code
      , measure_name
      , SIGNED_AMOUNT
    FROM RED
 ) s
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

# DBTITLE 1,Join RED and DP Data
var_signed_pivot_df = sql(
'''   
SELECT
  dp.dp_key
  , red.red_key
  , coalesce(dp.policy_reference, red.policy_reference) as Policy_Reference
  , coalesce(dp.year_id, red.Year_of_Account) AS Year_of_Account
  , coalesce(dp.Business_Entity, red.Business_Entity) AS Business_Entity
  , coalesce(dp.Sub_Class_Code, red.SubClass) AS Sub_Class_Code
  , coalesce(dp.SCC, red.SCC) AS SCC
  , coalesce(dp.Risk_Code, red.Risk_Code) AS Risk_Code
  , coalesce(round(dp.Line_Gross_Gross_Signed_Premium_SCC_DP,2),0) Line_Gross_Gross_Signed_Premium_SCC_DP
  , coalesce(round(red.Line_Gross_Gross_Signed_Premium_SCC_RED,2),0) Line_Gross_Gross_Signed_Premium_SCC_RED
  , (coalesce(round(dp.Line_Gross_Gross_Signed_Premium_SCC_DP,2),0) - coalesce(round(red.Line_Gross_Gross_Signed_Premium_SCC_RED,2),0)) Line_Gross_Gross_Signed_Premium_SCC_VAR
  
  , coalesce(round(dp.Line_Gross_Net_Signed_Premium_SCC_DP,2),0) Line_Gross_Net_Signed_Premium_SCC_DP
  , coalesce(round(red.Line_Gross_Net_Signed_Premium_SCC_RED,2),0) Line_Gross_Net_Signed_Premium_SCC_RED
  , (coalesce(round(dp.Line_Gross_Net_Signed_Premium_SCC_DP,2),0) - coalesce(round(red.Line_Gross_Net_Signed_Premium_SCC_RED,2),0)) Line_Gross_Net_Signed_Premium_SCC_VAR
  
  , coalesce(round(dp.Line_Gross_Paid_Claim_SCC_DP,2),0) Line_Gross_Paid_Claim_SCC_DP
  , coalesce(round(red.Line_Gross_Paid_Claim_SCC_RED,2),0) Line_Gross_Paid_Claim_SCC_RED
  , (coalesce(round(dp.Line_Gross_Paid_Claim_SCC_DP,2),0) - coalesce(round(red.Line_Gross_Paid_Claim_SCC_RED,2),0)) Line_Gross_Paid_Claim_SCC_VAR
  
  , coalesce(round(dp.Line_Gross_Signed_Profit_Commission_SCC_DP,2),0) Line_Gross_Signed_Profit_Commission_SCC_DP
  , coalesce(round(red.Line_Gross_Signed_Profit_Commission_SCC_RED,2),0) Line_Gross_Signed_Profit_Commission_SCC_RED
  , (coalesce(round(dp.Line_Gross_Signed_Profit_Commission_SCC_DP,2),0) - coalesce(round(red.Line_Gross_Signed_Profit_Commission_SCC_RED,2),0)) Line_Gross_Signed_Profit_Commission_SCC_VAR
  
  , coalesce(round(dp.Line_Gross_Signed_ReInstatement_Premium_SCC_DP,2),0) Line_Gross_Signed_ReInstatement_Premium_SCC_DP
  , coalesce(round(red.Line_Gross_Signed_ReInstatement_Premium_SCC_RED,2),0) Line_Gross_Signed_ReInstatement_Premium_SCC_RED
  , (coalesce(round(dp.Line_Gross_Signed_ReInstatement_Premium_SCC_DP,2),0) - coalesce(round(red.Line_Gross_Signed_ReInstatement_Premium_SCC_RED,2),0)) Line_Gross_Signed_ReInstatement_Premium_SCC_VAR
  
FROM vw_dpsigned dp
  FULL OUTER JOIN vw_redsigned red
    ON dp.dp_key = red.red_key
'''
)

# COMMAND ----------

var_signed_pivot_df.createOrReplaceTempView("vw_signed_pivot")

# COMMAND ----------

# DBTITLE 1,Unpivot results - Signed
pvt_signed_var_df = spark.sql(
'''
select 
    policy_reference
    , dp_key
    , red_key
    , year_of_account
    , business_entity
    , Sub_Class_Code
    , SCC
    , risk_code
    , stack(15, 
                'Line_Gross_Gross_Signed_Premium_SCC_DP', Line_Gross_Gross_Signed_Premium_SCC_DP
                , 'Line_Gross_Gross_Signed_Premium_SCC_RED', Line_Gross_Gross_Signed_Premium_SCC_RED
                , 'Line_Gross_Gross_Signed_Premium_SCC_VAR', Line_Gross_Gross_Signed_Premium_SCC_VAR
                
                , 'Line_Gross_Net_Signed_Premium_SCC_DP', Line_Gross_Net_Signed_Premium_SCC_DP
                , 'Line_Gross_Net_Signed_Premium_SCC_RED', Line_Gross_Net_Signed_Premium_SCC_RED
                , 'Line_Gross_Net_Signed_Premium_SCC_VAR', Line_Gross_Net_Signed_Premium_SCC_VAR
                
                , 'Line_Gross_Paid_Claim_SCC_DP', Line_Gross_Paid_Claim_SCC_DP
                , 'Line_Gross_Paid_Claim_SCC_RED', Line_Gross_Paid_Claim_SCC_RED
                , 'Line_Gross_Paid_Claim_SCC_VAR', Line_Gross_Paid_Claim_SCC_VAR
                
                , 'Line_Gross_Signed_Profit_Commission_SCC_DP', Line_Gross_Signed_Profit_Commission_SCC_DP
                , 'Line_Gross_Signed_Profit_Commission_SCC_RED', Line_Gross_Signed_Profit_Commission_SCC_RED
                , 'Line_Gross_Signed_Profit_Commission_SCC_VAR', Line_Gross_Signed_Profit_Commission_SCC_VAR
                
                , 'Line_Gross_Signed_ReInstatement_Premium_SCC_DP', Line_Gross_Signed_ReInstatement_Premium_SCC_DP
                , 'Line_Gross_Signed_ReInstatement_Premium_SCC_RED', Line_Gross_Signed_ReInstatement_Premium_SCC_RED
                , 'Line_Gross_Signed_ReInstatement_Premium_SCC_VAR', Line_Gross_Signed_ReInstatement_Premium_SCC_VAR
            ) as (Measure, Amount) from vw_signed_pivot
 ''' )#.display()

# COMMAND ----------

pvt_signed_var_df.createOrReplaceTempView("vw_signed_var")

# COMMAND ----------

# DBTITLE 1,Prep final data - Signed
signed_fnl_var_df = sql(
''' 
select
  Policy_Reference
  , 'NA' AS Unique_Claim_Reference
  , dp_key
  , red_key
  , Year_of_account
  , Business_Entity
  , Sub_Class_Code
  , SCC
  , Risk_Code
  , 'NA' as Reports_EPI
  , case
      when Measure in (
                        'Line_Gross_Gross_Signed_Premium_SCC_DP', 'Line_Gross_Gross_Signed_Premium_SCC_RED', 'Line_Gross_Gross_Signed_Premium_SCC_VAR'
                        , 'Line_Gross_Net_Signed_Premium_SCC_DP', 'Line_Gross_Net_Signed_Premium_SCC_RED', 'Line_Gross_Net_Signed_Premium_SCC_VAR'
                        , 'Line_Gross_Paid_Claim_SCC_DP', 'Line_Gross_Paid_Claim_SCC_RED', 'Line_Gross_Paid_Claim_SCC_VAR'
                        , 'Line_Gross_Signed_Profit_Commission_SCC_DP', 'Line_Gross_Signed_Profit_Commission_SCC_RED', 'Line_Gross_Signed_Profit_Commission_SCC_VAR'
                        , 'Line_Gross_Signed_ReInstatement_Premium_SCC_DP', 'Line_Gross_Signed_ReInstatement_Premium_SCC_RED', 'Line_Gross_Signed_ReInstatement_Premium_SCC_VAR'
                      )
      then 'SIGNED'
    end as Measure_Source
  , case
      when Measure in (
                        'Line_Gross_Gross_Signed_Premium_SCC_DP', 'Line_Gross_Gross_Signed_Premium_SCC_RED', 'Line_Gross_Gross_Signed_Premium_SCC_VAR'
                      )
      then 'SIGNED_PRM_GG_SCC'
      when Measure in (
                        'Line_Gross_Net_Signed_Premium_SCC_DP', 'Line_Gross_Net_Signed_Premium_SCC_RED', 'Line_Gross_Net_Signed_Premium_SCC_VAR'
                      )
      then 'SIGNED_PRM_GN_SCC'
      when Measure in (
                        'Line_Gross_Paid_Claim_SCC_DP', 'Line_Gross_Paid_Claim_SCC_RED', 'Line_Gross_Paid_Claim_SCC_VAR'
                      )
      then 'SIGNED_CLM_SCC'
      when Measure in (
                        'Line_Gross_Signed_Profit_Commission_SCC_DP', 'Line_Gross_Signed_Profit_Commission_SCC_RED', 'Line_Gross_Signed_Profit_Commission_SCC_VAR'
                      )
      then 'SIGNED_PC_SCC'
      when Measure in (
                        'Line_Gross_Signed_ReInstatement_Premium_SCC_DP', 'Line_Gross_Signed_ReInstatement_Premium_SCC_RED', 'Line_Gross_Signed_ReInstatement_Premium_SCC_VAR'
                      )
      then 'SIGNED_RIP_SCC'
    end as Measure_Group_Name
  , case
      when Measure in (
                        'Line_Gross_Gross_Signed_Premium_SCC_DP', 'Line_Gross_Net_Signed_Premium_SCC_DP', 'Line_Gross_Paid_Claim_SCC_DP'
                        , 'Line_Gross_Signed_Profit_Commission_SCC_DP', 'Line_Gross_Signed_ReInstatement_Premium_SCC_DP'
                      )
      then 'DP'
      when Measure in (
                        'Line_Gross_Gross_Signed_Premium_SCC_RED', 'Line_Gross_Net_Signed_Premium_SCC_RED', 'Line_Gross_Paid_Claim_SCC_RED'
                        , 'Line_Gross_Signed_Profit_Commission_SCC_RED', 'Line_Gross_Signed_ReInstatement_Premium_SCC_RED'
                      )
      then 'RED'
      when Measure in (
                        'Line_Gross_Gross_Signed_Premium_SCC_VAR', 'Line_Gross_Net_Signed_Premium_SCC_VAR', 'Line_Gross_Paid_Claim_SCC_VAR'
                        , 'Line_Gross_Signed_Profit_Commission_SCC_VAR', 'Line_Gross_Signed_ReInstatement_Premium_SCC_VAR'
                      )
      then 'VAR'
    end as Data_Type
  , Measure 
  , Amount
from vw_signed_var
''' 
)

# COMMAND ----------

signed_fnl_var_df.createOrReplaceTempView("vw_signed_variance")

# COMMAND ----------

# DBTITLE 1,Append the data to storage - Signed
(signed_fnl_var_df
 .coalesce(1)
 .write
 .mode("append")
 .option("header", "true")
 .format("delta")
 .save("dbfs:/mnt/raw/delta/red_dp_recon/RED_DP_Variance"))

# COMMAND ----------

# MAGIC %md
# MAGIC <h1> Load Claims Data </h1>

# COMMAND ----------

# DBTITLE 1,DP Data
dpclaim_df = sql(
'''
WITH DP AS 
(
  SELECT
    concat_ws('_', UPPER(dpclaim.policy_reference), dpclaim.year_id, dpclaim.business_entity, dpclaim.Sub_class_Code, dpclaim.SCC, dpclaim.Unique_Claim_Reference, dpclaim.risk_code) DP_KEY
    , UPPER(dpclaim.policy_reference) AS policy_reference 
    , dpclaim.year_id
    , dpclaim.business_entity
    , dpclaim.Sub_class_Code
    , dpclaim.SCC
    , dpclaim.Unique_Claim_Reference    
    , dpclaim.risk_code
    , dpclaim.measure_name
    , SUM(dpclaim.measure_amount) AS CLAIMS_AMOUNT
  FROM red_dp_recon.dp_claimsdata dpclaim
  WHERE dpclaim.measure_name in ('Line_Gross_Outstanding_Claim_SCC', 'Line_Gross_Paid_Claim_SCC', 'Line_Gross_Outstanding_Claim_Fee_SCC', 'Line_Gross_Paid_Claim_Fee_SCC'
                                      , 'Line_Gross_Reported_Outstanding_Claim_SCC', 'Line_Gross_Paid_Claim_Loss_Fund_SCC')
  GROUP BY dpclaim.policy_reference, dpclaim.year_id, dpclaim.business_entity, dpclaim.risk_code, dpclaim.measure_name
              , dpclaim.Sub_class_Code, dpclaim.SCC, dpclaim.Unique_Claim_Reference

)
SELECT
  DP_KEY
  , policy_reference
  , year_id
  , business_entity
  , Sub_class_Code
  , SCC
  , Unique_Claim_Reference
  , risk_code
  , ROUND(COALESCE(Line_Gross_Paid_Claim_SCC,0),2) AS Line_Gross_Paid_Claim_SCC_DP
  , ROUND(COALESCE(Line_Gross_Reported_Outstanding_Claim_SCC,0),2) AS Line_Gross_Outstanding_Claim_SCC_DP
  , ROUND(COALESCE(Line_Gross_Outstanding_Claim_Fee_SCC,0),2) AS Line_Gross_Outstanding_Claim_Fee_SCC_DP
  , ROUND(COALESCE(Line_Gross_Paid_Claim_Fee_SCC,0),2) AS Line_Gross_Paid_Claim_Fee_SCC_DP
  , ROUND(COALESCE(Line_Gross_Paid_Claim_Loss_Fund_SCC,0),2) AS Line_Gross_Paid_Claim_Loss_Fund_SCC_DP

FROM 
(
  SELECT
    DP_KEY
    , Policy_reference
    , year_id
    , business_entity
    , Sub_class_Code
    , SCC
    , Unique_Claim_Reference    
    , risk_code
    , measure_name
    , CLAIMS_AMOUNT
  FROM DP
) t
PIVOT 
(
SUM(CLAIMS_AMOUNT) FOR measure_name IN ('Line_Gross_Paid_Claim_SCC','Line_Gross_Reported_Outstanding_Claim_SCC', 'Line_Gross_Outstanding_Claim_Fee_SCC', 'Line_Gross_Paid_Claim_Fee_SCC', 'Line_Gross_Paid_Claim_Loss_Fund_SCC')
) 
ORDER BY policy_reference
''')#.display()

# COMMAND ----------

dpclaim_df.createOrReplaceTempView("vw_dpclaims")

# COMMAND ----------

# DBTITLE 1,RED Data
redclaim_df  = sql(
'''
WITH RED AS
(
  SELECT
    concat_ws('_', UPPER(redclaim.policy_reference), cast(redclaim.Year_of_Account as bigint), redclaim.Business_entity, redclaim.SubClass, redclaim.SCC, redclaim.BPR, redclaim.Risk_Code) RED_KEY
    , UPPER(redclaim.policy_reference) AS policy_reference 
    , cast(redclaim.Year_of_Account as bigint) AS Year_of_Account
    , redclaim.Business_entity
    , redclaim.SubClass
    , redclaim.SCC
    , redclaim.BPR AS Unique_Claim_Reference
    , redclaim.Risk_Code
    , redclaim.measure_name
    , SUM(redclaim.measure_amount) AS CLAIMS_AMOUNT
  FROM red_dp_recon.red_policy_measures_data redclaim
  WHERE redclaim.measure_name in ('Gross_Claim_SCM_OS', 'Gross_Claim_SCM_Paid', 'Gross_Claim_SCM_OS_Fees', 'Gross_Claim_SCM_Paid_Fees', 'SCM_Paid_Loss_Fund')
  GROUP BY redclaim.policy_reference, redclaim.measure_name, redclaim.Year_of_Account, redclaim.Business_entity, redclaim.Risk_Code
          , redclaim.SubClass, redclaim.SCC, redclaim.BPR

)
SELECT
   RED_KEY
  , policy_reference
  , Year_of_Account
  , Business_entity
  , SubClass
  , SCC
  , Unique_Claim_Reference
  , Risk_Code 
  , ROUND(COALESCE(Gross_Claim_SCM_OS,0),2) AS Line_Gross_Outstanding_Claim_SCC_RED
  , ROUND(COALESCE(Gross_Claim_SCM_Paid,0),2) AS Line_Gross_Paid_Claim_SCC_RED
  , ROUND(COALESCE(Gross_Claim_SCM_OS_Fees,0),2) AS Line_Gross_Outstanding_Claim_Fee_SCC_RED
  , ROUND(COALESCE(Gross_Claim_SCM_Paid_Fees,0),2) AS Line_Gross_Paid_Claim_Fee_SCC_RED
  , ROUND(COALESCE(SCM_Paid_Loss_Fund,0),2) AS Line_Gross_Paid_Claim_Loss_Fund_SCC_RED
FROM 
(
  SELECT
    RED_KEY
    , Policy_reference
    , Year_of_Account
    , Business_entity
    , SubClass
    , SCC
    , Unique_Claim_Reference    
    , Risk_Code
    , measure_name
    , CLAIMS_AMOUNT
  FROM RED
) t
PIVOT 
(
SUM(CLAIMS_AMOUNT) FOR measure_name IN ('Gross_Claim_SCM_OS', 'Gross_Claim_SCM_Paid', 'Gross_Claim_SCM_OS_Fees', 'Gross_Claim_SCM_Paid_Fees', 'SCM_Paid_Loss_Fund')
) 
ORDER BY policy_reference
''')#.display()

# COMMAND ----------

redclaim_df.createOrReplaceTempView("vw_redclaims")

# COMMAND ----------

# DBTITLE 1,Join DP and RED and exclude the bulk policies
var_claim_pivot_df = sql(
'''
select
  dp.dp_key
  , red.red_key
  , coalesce(dp.policy_reference, red.policy_reference) as Policy_Reference
  , coalesce(dp.year_id, red.Year_of_Account) AS Year_of_Account
  , coalesce(dp.business_entity, red.business_entity) AS business_entity
  , coalesce(dp.Sub_class_Code, red.SubClass) AS Sub_class_Code
  , coalesce(dp.SCC, red.SCC) AS SCC
  , coalesce(dp.Unique_Claim_Reference, red.Unique_Claim_Reference) AS Unique_Claim_Reference    
  , coalesce(dp.risk_code, red.risk_code) AS risk_code
 
  
  , coalesce(round(dp.Line_Gross_Paid_Claim_SCC_DP,2),0) Line_Gross_Paid_Claim_SCC_DP
  , coalesce(round(red.Line_Gross_Paid_Claim_SCC_RED,2),0) * -1 Line_Gross_Paid_Claim_SCC_RED
  , (coalesce(round(dp.Line_Gross_Paid_Claim_SCC_DP,2),0) - coalesce(round(red.Line_Gross_Paid_Claim_SCC_RED,2),0) * -1) Line_Gross_Paid_Claim_SCC_VAR
  
  , coalesce(round(dp.Line_Gross_Outstanding_Claim_SCC_DP,2),0) Line_Gross_Outstanding_Claim_SCC_DP
  , coalesce(round(red.Line_Gross_Outstanding_Claim_SCC_RED,2),0) * -1 Line_Gross_Outstanding_Claim_SCC_RED
  , (coalesce(round(dp.Line_Gross_Outstanding_Claim_SCC_DP,2),0) - coalesce(round(red.Line_Gross_Outstanding_Claim_SCC_RED,2),0) * -1) Line_Gross_Outstanding_Claim_SCC_VAR
  
  , coalesce(round(dp.Line_Gross_Outstanding_Claim_Fee_SCC_DP,2),0) Line_Gross_Outstanding_Claim_Fee_SCC_DP
  , coalesce(round(red.Line_Gross_Outstanding_Claim_Fee_SCC_RED,2),0) * -1 Line_Gross_Outstanding_Claim_Fee_SCC_RED
  , (coalesce(round(dp.Line_Gross_Outstanding_Claim_Fee_SCC_DP,2),0) - coalesce(round(red.Line_Gross_Outstanding_Claim_Fee_SCC_RED,2),0) * -1) Line_Gross_Outstanding_Claim_Fee_SCC_VAR
  
  , coalesce(round(dp.Line_Gross_Paid_Claim_Fee_SCC_DP,2),0) Line_Gross_Paid_Claim_Fee_SCC_DP
  , coalesce(round(red.Line_Gross_Paid_Claim_Fee_SCC_RED,2),0) * -1 Line_Gross_Paid_Claim_Fee_SCC_RED
  , (coalesce(round(dp.Line_Gross_Paid_Claim_Fee_SCC_DP,2),0) - coalesce(round(red.Line_Gross_Paid_Claim_Fee_SCC_RED,2),0) * -1) Line_Gross_Paid_Claim_Fee_SCC_VAR
  
  , coalesce(round(dp.Line_Gross_Paid_Claim_Loss_Fund_SCC_DP,2),0) Line_Gross_Paid_Claim_Loss_Fund_SCC_DP
  , coalesce(round(red.Line_Gross_Paid_Claim_Loss_Fund_SCC_RED,2),0) * -1 Line_Gross_Paid_Claim_Loss_Fund_SCC_RED
  , (coalesce(round(dp.Line_Gross_Paid_Claim_Loss_Fund_SCC_DP,2),0) - coalesce(round(red.Line_Gross_Paid_Claim_Loss_Fund_SCC_RED,2),0) * -1) Line_Gross_Paid_Claim_Loss_Fund_SCC_VAR
  
from vw_dpclaims dp
  full outer join vw_redclaims red
    on dp.dp_key = red.red_key
where not exists (
                    select *
                    from red_dp_recon.serco_policies ser
                    where dp.policy_reference = ser.policy_reference
                 )    
'''
)#.display()

# COMMAND ----------

var_claim_pivot_df.createOrReplaceTempView("vw_claim_pivot")

# COMMAND ----------

# DBTITLE 1,Unpivot the data
pvt_claim_var_df = spark.sql(
'''
select
  policy_reference
  , Unique_Claim_Reference
  , dp_key
  , red_key
  , year_of_account
  , business_entity
  , Sub_class_Code
  , SCC  
  , risk_code
  , stack(15,
              'Line_Gross_Paid_Claim_SCC_DP', Line_Gross_Paid_Claim_SCC_DP
              , 'Line_Gross_Paid_Claim_SCC_RED', Line_Gross_Paid_Claim_SCC_RED
              , 'Line_Gross_Paid_Claim_SCC_VAR', Line_Gross_Paid_Claim_SCC_VAR
              
              , 'Line_Gross_Outstanding_Claim_SCC_DP', Line_Gross_Outstanding_Claim_SCC_DP
              , 'Line_Gross_Outstanding_Claim_SCC_RED', Line_Gross_Outstanding_Claim_SCC_RED
              , 'Line_Gross_Outstanding_Claim_SCC_VAR', Line_Gross_Outstanding_Claim_SCC_VAR
              
              , 'Line_Gross_Outstanding_Claim_Fee_SCC_DP', Line_Gross_Outstanding_Claim_Fee_SCC_DP
              , 'Line_Gross_Outstanding_Claim_Fee_SCC_RED', Line_Gross_Outstanding_Claim_Fee_SCC_RED
              , 'Line_Gross_Outstanding_Claim_Fee_SCC_VAR', Line_Gross_Outstanding_Claim_Fee_SCC_VAR
              
              , 'Line_Gross_Paid_Claim_Fee_SCC_DP', Line_Gross_Paid_Claim_Fee_SCC_DP
              , 'Line_Gross_Paid_Claim_Fee_SCC_RED', Line_Gross_Paid_Claim_Fee_SCC_RED
              , 'Line_Gross_Paid_Claim_Fee_SCC_VAR', Line_Gross_Paid_Claim_Fee_SCC_VAR
              
              , 'Line_Gross_Paid_Claim_Loss_Fund_SCC_DP', Line_Gross_Paid_Claim_Loss_Fund_SCC_DP
              , 'Line_Gross_Paid_Claim_Loss_Fund_SCC_RED', Line_Gross_Paid_Claim_Loss_Fund_SCC_RED
              , 'Line_Gross_Paid_Claim_Loss_Fund_SCC_VAR', Line_Gross_Paid_Claim_Loss_Fund_SCC_VAR
        
          ) as (Measure, Amount)
from vw_claim_pivot
 ''' )#.display()

# COMMAND ----------

pvt_claim_var_df.createOrReplaceTempView("vw_claim_var")

# COMMAND ----------

# DBTITLE 1,Prep data for final view
claims_fnl_var_df = sql(
''' 
select
  Policy_Reference
  , Unique_Claim_Reference
  , dp_key
  , red_key
  , Year_of_Account
  , Business_Entity
  , Sub_class_Code
  , SCC    
  , Risk_Code
  , 'Policy_Level' as Reports_EPI
  , case
      when Measure in (
                        'Line_Gross_Paid_Claim_SCC_DP', 'Line_Gross_Paid_Claim_SCC_RED', 'Line_Gross_Paid_Claim_SCC_VAR'
                        , 'Line_Gross_Outstanding_Claim_SCC_DP', 'Line_Gross_Outstanding_Claim_SCC_RED', 'Line_Gross_Outstanding_Claim_SCC_VAR'
                        , 'Line_Gross_Outstanding_Claim_Fee_SCC_DP', 'Line_Gross_Outstanding_Claim_Fee_SCC_RED', 'Line_Gross_Outstanding_Claim_Fee_SCC_VAR'
                        , 'Line_Gross_Paid_Claim_Fee_SCC_DP', 'Line_Gross_Paid_Claim_Fee_SCC_RED', 'Line_Gross_Paid_Claim_Fee_SCC_VAR'
                        , 'Line_Gross_Paid_Claim_Loss_Fund_SCC_DP', 'Line_Gross_Paid_Claim_Loss_Fund_SCC_RED', 'Line_Gross_Paid_Claim_Loss_Fund_SCC_VAR'
                        )
      then 'CLAIMS'
    end as Measure_Source
  , case
      when Measure in (
                        'Line_Gross_Paid_Claim_SCC_DP', 'Line_Gross_Paid_Claim_SCC_RED', 'Line_Gross_Paid_Claim_SCC_VAR'
                      )
      then 'CLAIMS_PAID_SCC'
      when Measure in (
                        'Line_Gross_Outstanding_Claim_SCC_DP', 'Line_Gross_Outstanding_Claim_SCC_RED', 'Line_Gross_Outstanding_Claim_SCC_VAR'
                      )
      then 'CLAIMS_OS_SCC'
      when Measure in (
                        'Line_Gross_Outstanding_Claim_Fee_SCC_DP', 'Line_Gross_Outstanding_Claim_Fee_SCC_RED', 'Line_Gross_Outstanding_Claim_Fee_SCC_VAR'
                      )
      then 'CLAIMS_FEES_OS_SCC'
      when Measure in (
                        'Line_Gross_Paid_Claim_Fee_SCC_DP', 'Line_Gross_Paid_Claim_Fee_SCC_RED', 'Line_Gross_Paid_Claim_Fee_SCC_VAR'
                      )
      then 'CLAIMS_FEES_PAID_SCC'
      when Measure in (
                        'Line_Gross_Paid_Claim_Loss_Fund_SCC_DP', 'Line_Gross_Paid_Claim_Loss_Fund_SCC_RED', 'Line_Gross_Paid_Claim_Loss_Fund_SCC_VAR'
                      )
      then 'CLAIMS_PAID_LF_SCC'
    end as Measure_Group_Name
  , case
      when Measure in (
                        'Line_Gross_Paid_Claim_SCC_DP', 'Line_Gross_Outstanding_Claim_SCC_DP', 'Line_Gross_Outstanding_Claim_Fee_SCC_DP', 'Line_Gross_Paid_Claim_Fee_SCC_DP', 'Line_Gross_Paid_Claim_Loss_Fund_SCC_DP'
                      )
      then 'DP'
      when Measure in (
                        'Line_Gross_Paid_Claim_SCC_RED', 'Line_Gross_Outstanding_Claim_SCC_RED', 'Line_Gross_Outstanding_Claim_Fee_SCC_RED', 'Line_Gross_Paid_Claim_Fee_SCC_RED', 'Line_Gross_Paid_Claim_Loss_Fund_SCC_RED'
                      )
      then 'RED'
      when Measure in (
                        'Line_Gross_Paid_Claim_SCC_VAR', 'Line_Gross_Outstanding_Claim_SCC_VAR', 'Line_Gross_Outstanding_Claim_Fee_SCC_VAR', 'Line_Gross_Paid_Claim_Fee_SCC_VAR', 'Line_Gross_Paid_Claim_Loss_Fund_SCC_VAR'
                      )
      then 'VAR'
    end as Data_Type
   , Measure
   , Amount
from vw_claim_var
''' 
)#.display()

# COMMAND ----------

claims_fnl_var_df.createOrReplaceTempView("vw_claims_variance")

# COMMAND ----------

# DBTITLE 1,Append the data 
(claims_fnl_var_df
 .coalesce(1)
 .write
 .mode("append")
 .option("header", "true")
 .format("delta")
 .save("dbfs:/mnt/raw/delta/red_dp_recon/RED_DP_Variance"))

# COMMAND ----------

# DBTITLE 1,Drop and recreate the table
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS red_dp_recon.RED_DP_Variance;
# MAGIC CREATE TABLE red_dp_recon.RED_DP_Variance
# MAGIC USING delta
# MAGIC OPTIONS (path "dbfs:/mnt/raw/delta/red_dp_recon/RED_DP_Variance")

# COMMAND ----------

variance_analysis = sql(
''' 
SELECT 
     a.Policy_Reference
    ,a.Year_of_Account
    ,a.Business_Entity
    ,a.Sub_Class_Code
    ,a.SCC
    ,CASE WHEN a.Risk_Code IN ('##','[]','A','A2','A3','X','ZZ') THEN NULL ELSE a.Risk_Code END as Risk_Code
    ,a.Measure_Source
    ,a.Measure_Group_Name
    ,ROUND(SUM(CASE WHEN a.Data_Type = 'RED' THEN a.Amount ELSE 0 END),2) as RED_AMOUNT
    ,ROUND(SUM(CASE WHEN a.Data_Type = 'DP' THEN a.Amount ELSE 0 END),2) as DP_AMOUNT
    ,ROUND(SUM(CASE WHEN a.Data_Type = 'VAR' THEN a.Amount ELSE 0 END),2) as VAR_AMOUNT
    ,CASE WHEN a.Measure_Source = 'EPI' AND COALESCE(b.REPORTS_EPI,'') <> 'Y' THEN 'EPI NOT REPORTED' 
          WHEN a.Measure_Source = 'CLAIMS' AND COALESCE(c.Polcy_Classification,'') = 'Bulk' THEN 'BULK CLAIM'
          ELSE 'YES'
     END as EXPLANATION
FROM red_dp_recon.red_dp_variance a
LEFT OUTER JOIN red_dp_recon.red_epi_variance b on a.policy_reference = b.policy_reference
LEFT OUTER JOIN red_dp_recon.serco_policies c on a.policy_reference = c.policy_reference
WHERE 1=1
GROUP BY 
     a.Policy_Reference
    ,a.Year_of_Account
    ,a.Business_Entity
    ,a.Sub_Class_Code
    ,a.SCC
    ,CASE WHEN a.Risk_Code IN ('##','[]','A','A2','A3','X','ZZ') THEN NULL ELSE a.Risk_Code END 
    ,a.Measure_Source
    ,a.Measure_Group_Name
    ,CASE WHEN a.Measure_Source = 'EPI' AND COALESCE(b.REPORTS_EPI,'') <> 'Y' THEN 'EPI NOT REPORTED' 
          WHEN a.Measure_Source = 'CLAIMS' AND COALESCE(c.Polcy_Classification,'') = 'Bulk' THEN 'BULK CLAIM'
          ELSE 'YES'
     END   
HAVING
       ABS(SUM(CASE WHEN a.Data_Type = 'RED' THEN a.Amount ELSE 0 END)) > 0.5
    OR ABS(SUM(CASE WHEN a.Data_Type = 'DP' THEN a.Amount ELSE 0 END)) > 0.5
    OR ABS(SUM(CASE WHEN a.Data_Type = 'VAR' THEN a.Amount ELSE 0 END)) > 0.5
''' 
)

# COMMAND ----------

(variance_analysis
 .coalesce(1)
 .write
 .mode("overwrite")
 .option("header", "true")
 .format("delta")
 .save("dbfs:/mnt/raw/delta/red_dp_recon/variance_analysis"))

# COMMAND ----------

# DBTITLE 1,This will be used for external table in synapse
(variance_analysis
 .coalesce(1)
 .write
 .mode("overwrite")
 .option("header", "true")
 .format("delta")
 .save("dbfs:/mnt/extsynapse/red_dp_recon"))

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS red_dp_recon.variance_analysis;
# MAGIC CREATE TABLE red_dp_recon.variance_analysis
# MAGIC USING delta
# MAGIC OPTIONS (path "dbfs:/mnt/raw/delta/red_dp_recon/variance_analysis")