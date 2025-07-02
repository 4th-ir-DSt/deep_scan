# Databricks notebook source
# MAGIC %md ## Notebook for reconciliation of Phase A Facts

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql.functions import *
import pyodbc
import numpy
import re
import pandas as pd 

# COMMAND ----------

# DBTITLE 1,Setup Connection
#ASQLDB Metadata DB
# dbconnMeta = dbutils.secrets.get(scope = 'lza-da-kv-001-d', key = 'lza-dp-sqlacct-001-databricks-sql-connection')
# connMeta = pyodbc.connect(dbconnMeta, autocommit = True)
# cursorMeta = connMeta.cursor()

#Synapse Datawarehouse
dbconnDw = dbutils.secrets.get(scope = 'lza-da-kv-001-d', key = 'lza-dp-sqlacct-001-databricks-syn-connection')
connDw = pyodbc.connect(dbconnDw, autocommit = True)
cursorDw = connDw.cursor()

# COMMAND ----------

# DBTITLE 1,Grain Check for Phase A Fact - Should get no records in output
query_Fact_grain_check = """select 'fact_claim' as fact_name, fact_claim_hbk as hbk_col, dwh_valid_from, count(1) as count
from dwh.fact_claim
where dwh_action_type='I'
group by fact_claim_hbk, dwh_valid_from
having count(1)>1
UNION
select 'fact_limit' as fact_name, fact_limit_hbk as hbk_col, dwh_valid_from, count(1) as count
from dwh.fact_limit
where dwh_action_type='I'
group by fact_limit_hbk, dwh_valid_from
having count(1)>1
UNION
select 'Fact_Pricing_Rating' as fact_name, Fact_Pricing_Rating_hbk as hbk_col, dwh_valid_from, count(1) as count
from dwh.Fact_Pricing_Rating
where dwh_action_type='I'
group by Fact_Pricing_Rating_hbk, dwh_valid_from
having count(1)>1
UNION
select 'fact_signed_amount' as fact_name, fact_signed_amount_hbk as hbk_col, dwh_valid_from, count(1) as count
from dwh.fact_signed_amount
where dwh_action_type='I'
group by fact_signed_amount_hbk, dwh_valid_from
having count(1)>1
UNION
select 'fact_Estimated_Premium' as fact_name, fact_Estimated_Premium_hbk as hbk_col, Dwh_Valid_From, count(1) as count
from dwh.Fact_Estimated_Premium
where Dwh_Action_Type='I'
group by Fact_Estimated_Premium_hbk, dwh_valid_from
having count(1)>1
UNION
select 'fact_exchange_rate' as fact_name, fact_exchange_rate_hbk as hbk_col, dwh_valid_from, count(1) as count
from dwh.Fact_Exchange_Rate
where dwh_action_type='I'
group by fact_exchange_rate_hbk, dwh_valid_from
having count(1)>1"""

pd_df_Fact_grain_check = pd.read_sql_query(query_Fact_grain_check,connDw)

if pd_df_Fact_grain_check.empty:
  print('Output : Grain check is passed !!')
else:
    pd.set_option('display.max_columns', None)
    print('Output : Grain check is failed for below facts ')
    display(pd_df_Fact_grain_check)

# COMMAND ----------

# DBTITLE 1,Count Check for Phase A Fact - Should get no records in output
query_Fact_cnt_chk = """select Fact_Count.*,Fact_Stage_Count.Stage_Count,Fact_Count.Fact_Count- Fact_Stage_Count.Stage_Count as Difference_in_Count from 
(SELECT 'Fact_Claim' as Fact, COUNT(1) as Stage_Count
FROM staging.claim_transaction_detail 
WHERE lakeisactive = 1 
AND lakedeletedtimestamp is null 
AND (COALESCE(Whole_SCC, 0.00) <> 0.00 
OR COALESCE(Whole_OCC, 0.00) <> 0.00 
OR COALESCE(Market_SCC, 0.00) <> 0.00 
OR COALESCE(Market_OCC, 0.00) <> 0.00 
OR COALESCE(Line_SCC, 0.00) <> 0.00 
OR COALESCE(Line_OCC, 0.00) <> 0.00)
UNION ALL
select 'Fact_Limit' as Fact, COUNT(1) as Stage_Count from staging.limit limit
inner join (select * from staging.line where lakeisactive = 1 and lakedeletedtimestamp is null and 
COALESCE(line.calculated_line,0.00) <> 0.00 )line
on limit.Policy_Header_Reference = line.Policy_Header_Reference 
and limit.Policy_Section_Reference = line.Policy_Section_Reference
where limit.lakeisactive = 1 and limit.lakedeletedtimestamp is null and COALESCE(limit_amount,0.00) <> 0.00
UNION ALL
select 'Fact_Pricing_Rating' as Fact, COUNT(1) as Stage_Count from staging.pricing_rating pr 
inner join (select * from staging.line where lakeisactive =1 and lakedeletedtimestamp is null and (COALESCE(Calculated_Line, 0.00) <> 0.00 
OR COALESCE(Signed_Order, Written_Order) <> 0.00)) ln
on pr.Policy_Header_Reference = ln.Policy_Header_Reference 
AND pr.Policy_Section_Reference = ln.Policy_Section_Reference
where (pr.lakeisactive=1 and pr.lakedeletedtimestamp is null)
AND (COALESCE(Expiring_Whole_Premium_Written_SCC, 0.00) <> 0.00 
OR COALESCE(Benchmark_Percentage, 0.00) <> 0.00 
OR COALESCE(Expiring_Whole_Premium_Written_OCC, 0.00)  <> 0.00 
OR COALESCE(Change_In_Deduction_Attachment_Point_Percentage, 0.00) <> 0.00 
OR COALESCE(Change_In_Breadth_Cover_Percentage, 0.00) <> 0.00 
OR COALESCE(Change_In_Pure_Rate_Percentage, 0.00) <> 0.00 
OR COALESCE(Change_In_Other_Factors_Percentage, 0.00) <> 0.00)
UNION ALL
select 'Fact_Signed_Amount' as Fact, COUNT(1) as Stage_Count from staging.signing_transaction_detail 
where lakeisactive = 1 
 AND lakedeletedtimestamp is null
 AND (COALESCE(Whole_SCC, 0.00) <> 0.00 
 OR COALESCE(Whole_OCC, 0.00) <> 0.00 
 OR COALESCE(Market_SCC, 0.00) <> 0.00 
 OR COALESCE(Market_OCC, 0.00) <> 0.00 
 OR COALESCE(Line_SCC, 0.00) <> 0.00 
 OR COALESCE(Line_OCC, 0.00) <> 0.00)
 UNION ALL
select 'Fact_Estimated_Premium' as Fact, COUNT(1) as Stage_Count from staging.epi_transaction_detail where lakeisactive=1 and lakedeletedtimestamp is null
AND (COALESCE(Whole_SCC, 0.00) <> 0.00 
OR COALESCE(Whole_OCC, 0.00) <> 0.00 
OR COALESCE(Market_SCC, 0.00) <> 0.00 
OR COALESCE(Market_OCC, 0.00) <> 0.00 
OR COALESCE(Line_SCC, 0.00) <> 0.00
OR COALESCE(Line_OCC, 0.00) <> 0.00)
UNION ALL
SELECT 'Fact_Exchange_Rate' as Fact, COUNT(1) as Stage_Count FROM staging.exchange_rate 
WHERE Exchange_Rate_Type_Code = 'Daily Rate' 
AND lakeisactive=1 
AND lakedeletedtimestamp is null
AND (
 COALESCE(division_rate_of_exchange,0.00) <> 0.00 
 OR COALESCE(multiplication_rate_of_exchange,0.00) <> 0.00
 )) Fact_Stage_Count ,
 (SELECT  'Fact_Claim' as Fact, COUNT(1) as Fact_Count
FROM ( 
 SELECT Fact_Claim_HBK FROM
   (SELECT fact_claim_hbk,
     Hash_Value,
     Dwh_Action_Type,
     Dwh_Valid_From,
     ROW_NUMBER() OVER(PARTITION BY Fact_Claim_HBK,Dwh_Action_Type ORDER BY Dwh_Valid_From DESC) AS rn,
     DENSE_RANK() OVER(PARTITION BY Fact_Claim_HBK ORDER BY Dwh_Valid_From DESC) AS drn
     FROM dwh.Fact_Claim) fc
 WHERE fc.rn = 1 AND Dwh_Action_Type = 'I' and fc.drn = 1 
) fact
UNION ALL
select 'Fact_Limit' as Fact, COUNT(1) as Fact_Count from ( SELECT fact_limit_hbk FROM
        (SELECT fact_limit_hbk,Hash_Value as HVL,Dwh_Action_Type,Dwh_Valid_From,
         ROW_NUMBER() OVER(PARTITION BY fact_limit_hbk,Dwh_Action_Type ORDER BY Dwh_Valid_From DESC) AS rn,
         DENSE_RANK() OVER(PARTITION BY fact_limit_hbk ORDER BY Dwh_Valid_From DESC) AS drn
         FROM dwh.fact_limit) fl
    WHERE fl.rn = 1 AND Dwh_Action_Type = 'I' and fl.drn = 1  ) fact
UNION ALL
select 'Fact_Pricing_Rating' as Fact, COUNT(1) as Fact_Count from ( SELECT fact_pricing_rating_hbk FROM
        (SELECT fact_pricing_rating_hbk,Hash_Value as HVL,Dwh_Action_Type,Dwh_Valid_From,
         ROW_NUMBER() OVER(PARTITION BY fact_pricing_rating_hbk,Dwh_Action_Type ORDER BY Dwh_Valid_From DESC) AS rn,
         DENSE_RANK() OVER(PARTITION BY fact_pricing_rating_hbk ORDER BY Dwh_Valid_From DESC) AS drn
         FROM dwh.fact_pricing_rating) fpr
    WHERE fpr.rn = 1 AND Dwh_Action_Type = 'I' and fpr.drn = 1 ) fact
UNION ALL
SELECT  'Fact_Signed_Amount' as Fact, COUNT(1) as Fact_Count
FROM ( 
 SELECT Fact_Signed_Amount_HBK FROM
   (SELECT fact_Signed_Amount_hbk,
     Hash_Value,
     Dwh_Action_Type,
     Dwh_Valid_From,
     ROW_NUMBER() OVER(PARTITION BY Fact_Signed_Amount_HBK,Dwh_Action_Type ORDER BY Dwh_Valid_From DESC) AS rn,
     DENSE_RANK() OVER(PARTITION BY Fact_Signed_Amount_HBK ORDER BY Dwh_Valid_From DESC) AS drn
     FROM dwh.Fact_Signed_Amount) fsm
 WHERE fsm.rn = 1 AND Dwh_Action_Type = 'I' and fsm.drn = 1 
) fact
UNION ALL
select  'Fact_Estimated_Premium' as Fact, COUNT(1) as Fact_Count from ( SELECT Fact_Estimated_Premium_HBK FROM
        (SELECT Fact_Estimated_Premium_HBK,Hash_Value as HVL,Dwh_Action_Type,Dwh_Valid_From,
         ROW_NUMBER() OVER(PARTITION BY Fact_Estimated_Premium_HBK,Dwh_Action_Type ORDER BY Dwh_Valid_From DESC) AS rn,
         DENSE_RANK() OVER(PARTITION BY Fact_Estimated_Premium_HBK ORDER BY Dwh_Valid_From DESC) AS drn
         FROM dwh.Fact_Estimated_Premium) fl
    WHERE fl.rn = 1 AND Dwh_Action_Type = 'I' and fl.drn = 1 ) fact
UNION ALL
SELECT 'Fact_Exchange_Rate' as Fact, COUNT(1) as Fact_Count FROM 
( SELECT Fact_Exchange_Rate_HBK FROM
        (SELECT Fact_Exchange_Rate_HBK,Hash_Value as HVL,Dwh_Action_Type,Dwh_Valid_From, Exchange_Rate_Type_ID, dwh_created_batch_id,
         ROW_NUMBER() OVER(PARTITION BY Fact_Exchange_Rate_HBK,Dwh_Action_Type ORDER BY Dwh_Valid_From DESC) AS rn,
         DENSE_RANK() OVER(PARTITION BY Fact_Exchange_Rate_HBK ORDER BY Dwh_Valid_From DESC) AS drn
         FROM dwh.Fact_Exchange_Rate) fx
INNER JOIN dwh.Dim_Exchange_Rate_Type dim
on fx.Exchange_Rate_Type_ID = dim.Exchange_Rate_Type_ID 
and Dwh_Valid_To IS NULL
WHERE dim.Exchange_Rate_Type = 'Daily Rate' 
AND fx.dwh_created_batch_id > 0 
AND fx.rn = 1 
AND fx.Dwh_Action_Type = 'I' 
AND fx.drn = 1) fact) Fact_Count
where Fact_Stage_Count.Fact=Fact_Count.Fact
and Fact_Stage_Count.Stage_Count <> Fact_Count.Fact_Count"""

pd_df_Fact_cnt_chk = pd.read_sql_query(query_Fact_cnt_chk,connDw)

if pd_df_Fact_cnt_chk.empty:
  print('Output : Count check is passed !!')
else:
    pd.set_option('display.max_columns', None)
    print('Output : Count check is failed for below facts')
    display(pd_df_Fact_cnt_chk)

# COMMAND ----------

# DBTITLE 1,Metric Check (Fact Claim) for Phase A Fact - Should get no records in output

pd_metric_chk_fct_claim = """ 
select
'staging' as table_type,
SUM(
CAST(
Whole_Gross_Paid_Claim_Indemnity_SCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Paid_Claim_Indemnity_SCC,
SUM(
CAST(
Whole_Gross_Paid_Claim_Indemnity_OCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Paid_Claim_Indemnity_OCC,
SUM(
CAST(
Market_Gross_Paid_Claim_Indemnity_SCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Paid_Claim_Indemnity_SCC,
SUM(
CAST(
Market_Gross_Paid_Claim_Indemnity_OCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Paid_Claim_Indemnity_OCC,
SUM(
CAST(
Line_Gross_Paid_Claim_Indemnity_SCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Paid_Claim_Indemnity_SCC,
SUM(
CAST(
Line_Gross_Paid_Claim_Indemnity_OCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Paid_Claim_Indemnity_OCC,
SUM(
CAST(
Whole_Gross_Paid_Claim_Fee_SCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Paid_Claim_Fee_SCC,
SUM(
CAST(
Whole_Gross_Paid_Claim_Fee_OCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Paid_Claim_Fee_OCC,
SUM(
CAST(
Market_Gross_Paid_Claim_Fee_SCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Paid_Claim_Fee_SCC,
SUM(
CAST(
Market_Gross_Paid_Claim_Fee_OCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Paid_Claim_Fee_OCC,
SUM(
CAST(
Line_Gross_Paid_Claim_Fee_SCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Paid_Claim_Fee_SCC,
SUM(
CAST(
Line_Gross_Paid_Claim_Fee_OCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Paid_Claim_Fee_OCC,
SUM(
CAST(
Whole_Gross_Paid_Claim_SCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Paid_Claim_SCC,
SUM(
CAST(
Whole_Gross_Paid_Claim_OCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Paid_Claim_OCC,
SUM(
CAST(
Market_Gross_Paid_Claim_SCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Paid_Claim_SCC,
SUM(
CAST(
Market_Gross_Paid_Claim_OCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Paid_Claim_OCC,
SUM(
CAST(
Line_Gross_Paid_Claim_SCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Paid_Claim_SCC,
SUM(
CAST(
Line_Gross_Paid_Claim_OCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Paid_Claim_OCC,
SUM(
CAST(
Whole_Gross_Outstanding_Claim_Indemnity_SCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Outstanding_Claim_Indemnity_SCC,
SUM(
CAST(
Whole_Gross_Outstanding_Claim_Indemnity_OCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Outstanding_Claim_Indemnity_OCC,
SUM(
CAST(
Market_Gross_Outstanding_Claim_Indemnity_SCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Outstanding_Claim_Indemnity_SCC,
SUM(
CAST(
Market_Gross_Outstanding_Claim_Indemnity_OCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Outstanding_Claim_Indemnity_OCC,
SUM(
CAST(
Line_Gross_Outstanding_Claim_Indemnity_SCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Outstanding_Claim_Indemnity_SCC,
SUM(
CAST(
Line_Gross_Outstanding_Claim_Indemnity_OCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Outstanding_Claim_Indemnity_OCC,
SUM(
CAST(
Whole_Gross_Outstanding_Claim_Fee_SCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Outstanding_Claim_Fee_SCC,
SUM(
CAST(
Whole_Gross_Outstanding_Claim_Fee_OCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Outstanding_Claim_Fee_OCC,
SUM(
CAST(
Market_Gross_Outstanding_Claim_Fee_SCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Outstanding_Claim_Fee_SCC,
SUM(
CAST(
Market_Gross_Outstanding_Claim_Fee_OCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Outstanding_Claim_Fee_OCC,
SUM(
CAST(
Line_Gross_Outstanding_Claim_Fee_SCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Outstanding_Claim_Fee_SCC,
SUM(
CAST(
Line_Gross_Outstanding_Claim_Fee_OCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Outstanding_Claim_Fee_OCC,
SUM(
CAST(
Whole_Gross_Additional_Outstanding_Claim_Indemnity_SCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Additional_Outstanding_Claim_Indemnity_SCC,
SUM(
CAST(
Whole_Gross_Additional_Outstanding_Claim_Indemnity_OCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Additional_Outstanding_Claim_Indemnity_OCC,
SUM(
CAST(
Market_Gross_Additional_Outstanding_Claim_Indemnity_SCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Additional_Outstanding_Claim_Indemnity_SCC,
SUM(
CAST(
Market_Gross_Additional_Outstanding_Claim_Indemnity_OCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Additional_Outstanding_Claim_Indemnity_OCC,
SUM(
CAST(
Line_Gross_Additional_Outstanding_Claim_Indemnity_SCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Additional_Outstanding_Claim_Indemnity_SCC,
SUM(
CAST(
Line_Gross_Additional_Outstanding_Claim_Indemnity_OCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Additional_Outstanding_Claim_Indemnity_OCC,
SUM(
CAST(
Whole_Gross_Additional_Outstanding_Claim_Fee_SCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Additional_Outstanding_Claim_Fee_SCC,
SUM(
CAST(
Whole_Gross_Additional_Outstanding_Claim_Fee_OCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Additional_Outstanding_Claim_Fee_OCC,
SUM(
CAST(
Market_Gross_Additional_Outstanding_Claim_Fee_SCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Additional_Outstanding_Claim_Fee_SCC,
SUM(
CAST(
Market_Gross_Additional_Outstanding_Claim_Fee_OCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Additional_Outstanding_Claim_Fee_OCC,
SUM(
CAST(
Line_Gross_Additional_Outstanding_Claim_Fee_SCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Additional_Outstanding_Claim_Fee_SCC,
SUM(
CAST(
Line_Gross_Additional_Outstanding_Claim_Fee_OCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Additional_Outstanding_Claim_Fee_OCC,
SUM(
CAST(
Whole_Gross_Outstanding_Claim_SCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Outstanding_Claim_SCC,
SUM(
CAST(
Whole_Gross_Outstanding_Claim_OCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Outstanding_Claim_OCC,
SUM(
CAST(
Market_Gross_Outstanding_Claim_SCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Outstanding_Claim_SCC,
SUM(
CAST(
Market_Gross_Outstanding_Claim_OCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Outstanding_Claim_OCC,
SUM(
CAST(
Line_Gross_Outstanding_Claim_SCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Outstanding_Claim_SCC,
SUM(
CAST(
Line_Gross_Outstanding_Claim_OCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Outstanding_Claim_OCC,
SUM(
CAST(
Whole_Gross_Additional_Outstanding_Claim_SCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Additional_Outstanding_Claim_SCC,
SUM(
CAST(
Whole_Gross_Additional_Outstanding_Claim_OCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Additional_Outstanding_Claim_OCC,
SUM(
CAST(
Market_Gross_Additional_Outstanding_Claim_SCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Additional_Outstanding_Claim_SCC,
SUM(
CAST(
Market_Gross_Additional_Outstanding_Claim_OCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Additional_Outstanding_Claim_OCC,
SUM(
CAST(
Line_Gross_Additional_Outstanding_Claim_SCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Additional_Outstanding_Claim_SCC,
SUM(
CAST(
Line_Gross_Additional_Outstanding_Claim_OCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Additional_Outstanding_Claim_OCC,
SUM(
CAST(
Whole_Gross_Reported_Outstanding_Claim_SCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Reported_Outstanding_Claim_SCC,
SUM(
CAST(
Whole_Gross_Reported_Outstanding_Claim_OCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Reported_Outstanding_Claim_OCC,
SUM(
CAST(
Market_Gross_Reported_Outstanding_Claim_SCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Reported_Outstanding_Claim_SCC,
SUM(
CAST(
Market_Gross_Reported_Outstanding_Claim_OCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Reported_Outstanding_Claim_OCC,
SUM(
CAST(
Line_Gross_Reported_Outstanding_Claim_SCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Reported_Outstanding_Claim_SCC,
SUM(
CAST(
Line_Gross_Reported_Outstanding_Claim_OCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Reported_Outstanding_Claim_OCC,
SUM(
CAST(
Whole_Gross_Incurred_Claim_Indemnity_SCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Incurred_Claim_Indemnity_SCC,
SUM(
CAST(
Whole_Gross_Incurred_Claim_Indemnity_OCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Incurred_Claim_Indemnity_OCC,
SUM(
CAST(
Market_Gross_Incurred_Claim_Indemnity_SCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Incurred_Claim_Indemnity_SCC,
SUM(
CAST(
Market_Gross_Incurred_Claim_Indemnity_OCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Incurred_Claim_Indemnity_OCC,
SUM(
CAST(
Line_Gross_Incurred_Claim_Indemnity_SCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Incurred_Claim_Indemnity_SCC,
SUM(
CAST(
Line_Gross_Incurred_Claim_Indemnity_OCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Incurred_Claim_Indemnity_OCC,
SUM(
CAST(
Whole_Gross_Incurred_Claim_Fee_SCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Incurred_Claim_Fee_SCC,
SUM(
CAST(
Whole_Gross_Incurred_Claim_Fee_OCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Incurred_Claim_Fee_OCC,
SUM(
CAST(
Market_Gross_Incurred_Claim_Fee_SCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Incurred_Claim_Fee_SCC,
SUM(
CAST(
Market_Gross_Incurred_Claim_Fee_OCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Incurred_Claim_Fee_OCC,
SUM(
CAST(
Line_Gross_Incurred_Claim_Fee_SCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Incurred_Claim_Fee_SCC,
SUM(
CAST(
Line_Gross_Incurred_Claim_Fee_OCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Incurred_Claim_Fee_OCC,
SUM(
CAST(
Whole_Gross_Incurred_Claim_SCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Incurred_Claim_SCC,
SUM(
CAST(
Whole_Gross_Incurred_Claim_OCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Incurred_Claim_OCC,
SUM(
CAST(
Market_Gross_Incurred_Claim_SCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Incurred_Claim_SCC,
SUM(
CAST(
Market_Gross_Incurred_Claim_OCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Incurred_Claim_OCC,
SUM(
CAST(
Line_Gross_Incurred_Claim_SCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Incurred_Claim_SCC,
SUM(
CAST(
Line_Gross_Incurred_Claim_OCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Incurred_Claim_OCC,
SUM(
CAST(
Market_Gross_Paid_Claim_Loss_Fund_SCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Paid_Claim_Loss_Fund_SCC,
SUM(
CAST(
Line_Gross_Paid_Claim_Loss_Fund_SCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Paid_Claim_Loss_Fund_SCC,
SUM(
CAST(
Whole_Gross_Paid_Claim_Loss_Fund_SCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Paid_Claim_Loss_Fund_SCC,
SUM(
CAST(
Market_Gross_Paid_Claim_Loss_Fund_OCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Paid_Claim_Loss_Fund_OCC,
SUM(
CAST(
Line_Gross_Paid_Claim_Loss_Fund_OCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Paid_Claim_Loss_Fund_OCC,
SUM(
CAST(
Whole_Gross_Paid_Claim_Loss_Fund_OCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Paid_Claim_Loss_Fund_OCC
from
(
SELECT
lakevalidfromtimestamp,
lakedeletedtimestamp,
Whole_Gross_Paid_Claim_Indemnity_SCC,
Whole_Gross_Paid_Claim_Indemnity_OCC,
Market_Gross_Paid_Claim_Indemnity_SCC,
Market_Gross_Paid_Claim_Indemnity_OCC,
Line_Gross_Paid_Claim_Indemnity_SCC,
Line_Gross_Paid_Claim_Indemnity_OCC,
Whole_Gross_Paid_Claim_Fee_SCC,
Whole_Gross_Paid_Claim_Fee_OCC,
Market_Gross_Paid_Claim_Fee_SCC,
Market_Gross_Paid_Claim_Fee_OCC,
Line_Gross_Paid_Claim_Fee_SCC,
Line_Gross_Paid_Claim_Fee_OCC,
Whole_Gross_Paid_Claim_Indemnity_SCC + Whole_Gross_Paid_Claim_Fee_SCC AS Whole_Gross_Paid_Claim_SCC,
Whole_Gross_Paid_Claim_Indemnity_OCC + Whole_Gross_Paid_Claim_Fee_OCC AS Whole_Gross_Paid_Claim_OCC,
Market_Gross_Paid_Claim_Indemnity_SCC + Market_Gross_Paid_Claim_Fee_SCC AS Market_Gross_Paid_Claim_SCC,
Market_Gross_Paid_Claim_Indemnity_OCC + Market_Gross_Paid_Claim_Fee_OCC AS Market_Gross_Paid_Claim_OCC,
Line_Gross_Paid_Claim_Indemnity_SCC + Line_Gross_Paid_Claim_Fee_SCC AS Line_Gross_Paid_Claim_SCC,
Line_Gross_Paid_Claim_Indemnity_OCC + Line_Gross_Paid_Claim_Fee_OCC AS Line_Gross_Paid_Claim_OCC,
Whole_Gross_Outstanding_Claim_Indemnity_SCC,
Whole_Gross_Outstanding_Claim_Indemnity_OCC,
Market_Gross_Outstanding_Claim_Indemnity_SCC,
Market_Gross_Outstanding_Claim_Indemnity_OCC,
Line_Gross_Outstanding_Claim_Indemnity_SCC,
Line_Gross_Outstanding_Claim_Indemnity_OCC,
Whole_Gross_Outstanding_Claim_Fee_SCC,
Whole_Gross_Outstanding_Claim_Fee_OCC,
Market_Gross_Outstanding_Claim_Fee_SCC,
Market_Gross_Outstanding_Claim_Fee_OCC,
Line_Gross_Outstanding_Claim_Fee_SCC,
Line_Gross_Outstanding_Claim_Fee_OCC,
Whole_Gross_Additional_Outstanding_Claim_Indemnity_SCC,
Whole_Gross_Additional_Outstanding_Claim_Indemnity_OCC,
Market_Gross_Additional_Outstanding_Claim_Indemnity_SCC,
Market_Gross_Additional_Outstanding_Claim_Indemnity_OCC,
Line_Gross_Additional_Outstanding_Claim_Indemnity_SCC,
Line_Gross_Additional_Outstanding_Claim_Indemnity_OCC,
Whole_Gross_Additional_Outstanding_Claim_Fee_SCC,
Whole_Gross_Additional_Outstanding_Claim_Fee_OCC,
Market_Gross_Additional_Outstanding_Claim_Fee_SCC,
Market_Gross_Additional_Outstanding_Claim_Fee_OCC,
Line_Gross_Additional_Outstanding_Claim_Fee_SCC,
Line_Gross_Additional_Outstanding_Claim_Fee_OCC,
Whole_Gross_Outstanding_Claim_Indemnity_SCC + Whole_Gross_Outstanding_Claim_Fee_SCC AS Whole_Gross_Outstanding_Claim_SCC,
Whole_Gross_Outstanding_Claim_Indemnity_OCC + Whole_Gross_Outstanding_Claim_Fee_OCC AS Whole_Gross_Outstanding_Claim_OCC,
Market_Gross_Outstanding_Claim_Indemnity_SCC + Market_Gross_Outstanding_Claim_Fee_SCC AS Market_Gross_Outstanding_Claim_SCC,
Market_Gross_Outstanding_Claim_Indemnity_OCC + Market_Gross_Outstanding_Claim_Fee_OCC AS Market_Gross_Outstanding_Claim_OCC,
Line_Gross_Outstanding_Claim_Indemnity_SCC + Line_Gross_Outstanding_Claim_Fee_SCC AS Line_Gross_Outstanding_Claim_SCC,
Line_Gross_Outstanding_Claim_Indemnity_OCC + Line_Gross_Outstanding_Claim_Fee_OCC AS Line_Gross_Outstanding_Claim_OCC,
Whole_Gross_Additional_Outstanding_Claim_Indemnity_SCC + Whole_Gross_Additional_Outstanding_Claim_Fee_SCC AS Whole_Gross_Additional_Outstanding_Claim_SCC,
Whole_Gross_Additional_Outstanding_Claim_Indemnity_OCC + Whole_Gross_Additional_Outstanding_Claim_Fee_OCC AS Whole_Gross_Additional_Outstanding_Claim_OCC,
Market_Gross_Additional_Outstanding_Claim_Indemnity_SCC + Market_Gross_Additional_Outstanding_Claim_Fee_SCC AS Market_Gross_Additional_Outstanding_Claim_SCC,
Market_Gross_Additional_Outstanding_Claim_Indemnity_OCC + Market_Gross_Additional_Outstanding_Claim_Fee_OCC AS Market_Gross_Additional_Outstanding_Claim_OCC,
Line_Gross_Additional_Outstanding_Claim_Indemnity_SCC + Line_Gross_Additional_Outstanding_Claim_Fee_SCC AS Line_Gross_Additional_Outstanding_Claim_SCC,
Line_Gross_Additional_Outstanding_Claim_Indemnity_OCC + Line_Gross_Additional_Outstanding_Claim_Fee_OCC AS Line_Gross_Additional_Outstanding_Claim_OCC,
Whole_Gross_Outstanding_Claim_Indemnity_SCC + Whole_Gross_Outstanding_Claim_Fee_SCC + Whole_Gross_Additional_Outstanding_Claim_Indemnity_SCC + Whole_Gross_Additional_Outstanding_Claim_Fee_SCC AS Whole_Gross_Reported_Outstanding_Claim_SCC,
Whole_Gross_Outstanding_Claim_Indemnity_OCC + Whole_Gross_Outstanding_Claim_Fee_OCC + Whole_Gross_Additional_Outstanding_Claim_Indemnity_OCC + Whole_Gross_Additional_Outstanding_Claim_Fee_OCC AS Whole_Gross_Reported_Outstanding_Claim_OCC,
Market_Gross_Outstanding_Claim_Indemnity_SCC + Market_Gross_Outstanding_Claim_Fee_SCC + Market_Gross_Additional_Outstanding_Claim_Indemnity_SCC + Market_Gross_Additional_Outstanding_Claim_Fee_SCC AS Market_Gross_Reported_Outstanding_Claim_SCC,
Market_Gross_Outstanding_Claim_Indemnity_OCC + Market_Gross_Outstanding_Claim_Fee_OCC + Market_Gross_Additional_Outstanding_Claim_Indemnity_OCC + Market_Gross_Additional_Outstanding_Claim_Fee_OCC AS Market_Gross_Reported_Outstanding_Claim_OCC,
Line_Gross_Outstanding_Claim_Indemnity_SCC + Line_Gross_Outstanding_Claim_Fee_SCC + Line_Gross_Additional_Outstanding_Claim_Indemnity_SCC + Line_Gross_Additional_Outstanding_Claim_Fee_SCC AS Line_Gross_Reported_Outstanding_Claim_SCC,
Line_Gross_Outstanding_Claim_Indemnity_OCC + Line_Gross_Outstanding_Claim_Fee_OCC + Line_Gross_Additional_Outstanding_Claim_Indemnity_OCC + Line_Gross_Additional_Outstanding_Claim_Fee_OCC AS Line_Gross_Reported_Outstanding_Claim_OCC,
Whole_Gross_Paid_Claim_Indemnity_SCC + Whole_Gross_Outstanding_Claim_Indemnity_SCC AS Whole_Gross_Incurred_Claim_Indemnity_SCC,
Whole_Gross_Paid_Claim_Indemnity_OCC + Whole_Gross_Outstanding_Claim_Indemnity_OCC AS Whole_Gross_Incurred_Claim_Indemnity_OCC,
Market_Gross_Paid_Claim_Indemnity_SCC + Market_Gross_Outstanding_Claim_Indemnity_SCC AS Market_Gross_Incurred_Claim_Indemnity_SCC,
Market_Gross_Paid_Claim_Indemnity_OCC + Market_Gross_Outstanding_Claim_Indemnity_OCC AS Market_Gross_Incurred_Claim_Indemnity_OCC,
Line_Gross_Paid_Claim_Indemnity_SCC + Line_Gross_Outstanding_Claim_Indemnity_SCC AS Line_Gross_Incurred_Claim_Indemnity_SCC,
Line_Gross_Paid_Claim_Indemnity_OCC + Line_Gross_Outstanding_Claim_Indemnity_OCC AS Line_Gross_Incurred_Claim_Indemnity_OCC,
Whole_Gross_Paid_Claim_Fee_SCC + Whole_Gross_Outstanding_Claim_Fee_SCC AS Whole_Gross_Incurred_Claim_Fee_SCC,
Whole_Gross_Paid_Claim_Fee_OCC + Whole_Gross_Outstanding_Claim_Fee_OCC AS Whole_Gross_Incurred_Claim_Fee_OCC,
Market_Gross_Paid_Claim_Fee_SCC + Market_Gross_Outstanding_Claim_Fee_SCC AS Market_Gross_Incurred_Claim_Fee_SCC,
Market_Gross_Paid_Claim_Fee_OCC + Market_Gross_Outstanding_Claim_Fee_OCC AS Market_Gross_Incurred_Claim_Fee_OCC,
Line_Gross_Paid_Claim_Fee_SCC + Line_Gross_Outstanding_Claim_Fee_SCC AS Line_Gross_Incurred_Claim_Fee_SCC,
Line_Gross_Paid_Claim_Fee_OCC + Line_Gross_Outstanding_Claim_Fee_OCC AS Line_Gross_Incurred_Claim_Fee_OCC,
Whole_Gross_Paid_Claim_Indemnity_SCC + Whole_Gross_Paid_Claim_Fee_SCC + Whole_Gross_Outstanding_Claim_Indemnity_SCC + Whole_Gross_Outstanding_Claim_Fee_SCC + Whole_Gross_Additional_Outstanding_Claim_Indemnity_SCC + Whole_Gross_Additional_Outstanding_Claim_Fee_SCC AS Whole_Gross_Incurred_Claim_SCC,
Whole_Gross_Paid_Claim_Indemnity_OCC + Whole_Gross_Paid_Claim_Fee_OCC + Whole_Gross_Outstanding_Claim_Indemnity_OCC + Whole_Gross_Outstanding_Claim_Fee_OCC + Whole_Gross_Additional_Outstanding_Claim_Indemnity_OCC + Whole_Gross_Additional_Outstanding_Claim_Fee_OCC AS Whole_Gross_Incurred_Claim_OCC,
Market_Gross_Paid_Claim_Indemnity_SCC + Market_Gross_Paid_Claim_Fee_SCC + Market_Gross_Outstanding_Claim_Indemnity_SCC + Market_Gross_Outstanding_Claim_Fee_SCC + Market_Gross_Additional_Outstanding_Claim_Indemnity_SCC + Market_Gross_Additional_Outstanding_Claim_Fee_SCC AS Market_Gross_Incurred_Claim_SCC,
Market_Gross_Paid_Claim_Indemnity_OCC + Market_Gross_Paid_Claim_Fee_OCC + Market_Gross_Outstanding_Claim_Indemnity_OCC + Market_Gross_Outstanding_Claim_Fee_OCC + Market_Gross_Additional_Outstanding_Claim_Indemnity_OCC + Market_Gross_Additional_Outstanding_Claim_Fee_OCC AS Market_Gross_Incurred_Claim_OCC,
Line_Gross_Paid_Claim_Indemnity_SCC + Line_Gross_Paid_Claim_Fee_SCC + Line_Gross_Outstanding_Claim_Indemnity_SCC + Line_Gross_Outstanding_Claim_Fee_SCC + Line_Gross_Additional_Outstanding_Claim_Indemnity_SCC + Line_Gross_Additional_Outstanding_Claim_Fee_SCC AS Line_Gross_Incurred_Claim_SCC,
Line_Gross_Paid_Claim_Indemnity_OCC + Line_Gross_Paid_Claim_Fee_OCC + Line_Gross_Outstanding_Claim_Indemnity_OCC + Line_Gross_Outstanding_Claim_Fee_OCC + Line_Gross_Additional_Outstanding_Claim_Indemnity_OCC + Line_Gross_Additional_Outstanding_Claim_Fee_OCC AS Line_Gross_Incurred_Claim_OCC,
Market_Gross_Paid_Claim_Loss_Fund_SCC,
Line_Gross_Paid_Claim_Loss_Fund_SCC,
Whole_Gross_Paid_Claim_Loss_Fund_SCC,
Market_Gross_Paid_Claim_Loss_Fund_OCC,
Line_Gross_Paid_Claim_Loss_Fund_OCC,
Whole_Gross_Paid_Claim_Loss_Fund_OCC
FROM
(
SELECT
Case When Transaction_Line_Code = 'PIN' Then Whole_SCC Else 0.00000000 End as Whole_Gross_Paid_Claim_Indemnity_SCC,
Case When Transaction_Line_Code = 'PIN' Then Whole_OCC Else 0.00000000 End as Whole_Gross_Paid_Claim_Indemnity_OCC,
Case When Transaction_Line_Code = 'PIN' Then Market_SCC Else 0.00000000 End as Market_Gross_Paid_Claim_Indemnity_SCC,
Case When Transaction_Line_Code = 'PIN' Then Market_OCC Else 0.00000000 End as Market_Gross_Paid_Claim_Indemnity_OCC,
Case When Transaction_Line_Code = 'PIN' Then Line_SCC Else 0.00000000 End as Line_Gross_Paid_Claim_Indemnity_SCC,
Case When Transaction_Line_Code = 'PIN' Then Line_OCC Else 0.00000000 End as Line_Gross_Paid_Claim_Indemnity_OCC,
Case When Transaction_Line_Code = 'PFE' Then Whole_SCC Else 0.00000000 End as Whole_Gross_Paid_Claim_Fee_SCC,
Case When Transaction_Line_Code = 'PFE' Then Whole_OCC Else 0.00000000 End as Whole_Gross_Paid_Claim_Fee_OCC,
Case When Transaction_Line_Code = 'PFE' Then Market_SCC Else 0.00000000 End as Market_Gross_Paid_Claim_Fee_SCC,
Case When Transaction_Line_Code = 'PFE' Then Market_OCC Else 0.00000000 End as Market_Gross_Paid_Claim_Fee_OCC,
Case When Transaction_Line_Code = 'PFE' Then Line_SCC Else 0.00000000 End as Line_Gross_Paid_Claim_Fee_SCC,
Case When Transaction_Line_Code = 'PFE' Then Line_OCC Else 0.00000000 End as Line_Gross_Paid_Claim_Fee_OCC,
Case When Transaction_Line_Code = 'OIN' Then Whole_SCC Else 0.00000000 End as Whole_Gross_Outstanding_Claim_Indemnity_SCC,
Case When Transaction_Line_Code = 'OIN' Then Whole_OCC Else 0.00000000 End as Whole_Gross_Outstanding_Claim_Indemnity_OCC,
Case When Transaction_Line_Code = 'OIN' Then Market_SCC Else 0.00000000 End as Market_Gross_Outstanding_Claim_Indemnity_SCC,
Case When Transaction_Line_Code = 'OIN' Then Market_OCC Else 0.00000000 End as Market_Gross_Outstanding_Claim_Indemnity_OCC,
Case When Transaction_Line_Code = 'OIN' Then Line_SCC Else 0.00000000 End as Line_Gross_Outstanding_Claim_Indemnity_SCC,
Case When Transaction_Line_Code = 'OIN' Then Line_OCC Else 0.00000000 End as Line_Gross_Outstanding_Claim_Indemnity_OCC,
Case When Transaction_Line_Code = 'OFE' Then Whole_SCC Else 0.00000000 End as Whole_Gross_Outstanding_Claim_Fee_SCC,
Case When Transaction_Line_Code = 'OFE' Then Whole_OCC Else 0.00000000 End as Whole_Gross_Outstanding_Claim_Fee_OCC,
Case When Transaction_Line_Code = 'OFE' Then Market_SCC Else 0.00000000 End as Market_Gross_Outstanding_Claim_Fee_SCC,
Case When Transaction_Line_Code = 'OFE' Then Market_OCC Else 0.00000000 End as Market_Gross_Outstanding_Claim_Fee_OCC,
Case When Transaction_Line_Code = 'OFE' Then Line_SCC Else 0.00000000 End as Line_Gross_Outstanding_Claim_Fee_SCC,
Case When Transaction_Line_Code = 'OFE' Then Line_OCC Else 0.00000000 End as Line_Gross_Outstanding_Claim_Fee_OCC,
Case When Transaction_Line_Code = 'AOI' Then Whole_SCC Else 0.00000000 End as Whole_Gross_Additional_Outstanding_Claim_Indemnity_SCC,
Case When Transaction_Line_Code = 'AOI' Then Whole_OCC Else 0.00000000 End as Whole_Gross_Additional_Outstanding_Claim_Indemnity_OCC,
Case When Transaction_Line_Code = 'AOI' Then Market_SCC Else 0.00000000 End as Market_Gross_Additional_Outstanding_Claim_Indemnity_SCC,
Case When Transaction_Line_Code = 'AOI' Then Market_OCC Else 0.00000000 End as Market_Gross_Additional_Outstanding_Claim_Indemnity_OCC,
Case When Transaction_Line_Code = 'AOI' Then Line_SCC Else 0.00000000 End as Line_Gross_Additional_Outstanding_Claim_Indemnity_SCC,
Case When Transaction_Line_Code = 'AOI' Then Line_OCC Else 0.00000000 End as Line_Gross_Additional_Outstanding_Claim_Indemnity_OCC,
Case When Transaction_Line_Code = 'AOF' Then Whole_SCC Else 0.00000000 End as Whole_Gross_Additional_Outstanding_Claim_Fee_SCC,
Case When Transaction_Line_Code = 'AOF' Then Whole_OCC Else 0.00000000 End as Whole_Gross_Additional_Outstanding_Claim_Fee_OCC,
Case When Transaction_Line_Code = 'AOF' Then Market_SCC Else 0.00000000 End as Market_Gross_Additional_Outstanding_Claim_Fee_SCC,
Case When Transaction_Line_Code = 'AOF' Then Market_OCC Else 0.00000000 End as Market_Gross_Additional_Outstanding_Claim_Fee_OCC,
Case When Transaction_Line_Code = 'AOF' Then Line_SCC Else 0.00000000 End as Line_Gross_Additional_Outstanding_Claim_Fee_SCC,
Case When Transaction_Line_Code = 'AOF' Then Line_OCC Else 0.00000000 End as Line_Gross_Additional_Outstanding_Claim_Fee_OCC,
Case When Transaction_Line_Code = 'PCLF' Then Whole_SCC Else 0.00000000 End as Whole_Gross_Paid_Claim_Loss_Fund_SCC,
Case When Transaction_Line_Code = 'PCLF' Then Whole_OCC Else 0.00000000 End as Whole_Gross_Paid_Claim_Loss_Fund_OCC,
Case When Transaction_Line_Code = 'PCLF' Then Market_SCC Else 0.00000000 End as Market_Gross_Paid_Claim_Loss_Fund_SCC,
Case When Transaction_Line_Code = 'PCLF' Then Market_OCC Else 0.00000000 End as Market_Gross_Paid_Claim_Loss_Fund_OCC,
Case When Transaction_Line_Code = 'PCLF' Then Line_SCC Else 0.00000000 End as Line_Gross_Paid_Claim_Loss_Fund_SCC,
Case When Transaction_Line_Code = 'PCLF' Then Line_OCC Else 0.00000000 End as Line_Gross_Paid_Claim_Loss_Fund_OCC,
lakevalidFROMtimestamp,
lakedeletedtimestamp
FROM
Staging.Claim_Transaction_Detail
WHERE
lakeisactive = 1
AND (
COALESCE(Whole_SCC, 0.00) <> 0.00
OR COALESCE(Whole_OCC, 0.00) <> 0.00
OR COALESCE(Market_SCC, 0.00) <> 0.00
OR COALESCE(Market_OCC, 0.00) <> 0.00
OR COALESCE(Line_SCC, 0.00) <> 0.00
OR COALESCE(Line_OCC, 0.00) <> 0.00
)
) X
) z
UNION ALL 
select
'fact' as table_type,
SUM(
Whole_Gross_Paid_Claim_Indemnity_SCC
) AS Whole_Gross_Paid_Claim_Indemnity_SCC,
SUM(
Whole_Gross_Paid_Claim_Indemnity_OCC
) AS Whole_Gross_Paid_Claim_Indemnity_OCC,
SUM(
Market_Gross_Paid_Claim_Indemnity_SCC
) AS Market_Gross_Paid_Claim_Indemnity_SCC,
SUM(
Market_Gross_Paid_Claim_Indemnity_OCC
) AS Market_Gross_Paid_Claim_Indemnity_OCC,
SUM(
Line_Gross_Paid_Claim_Indemnity_SCC
) AS Line_Gross_Paid_Claim_Indemnity_SCC,
SUM(
Line_Gross_Paid_Claim_Indemnity_OCC
) AS Line_Gross_Paid_Claim_Indemnity_OCC,
SUM(Whole_Gross_Paid_Claim_Fee_SCC) AS Whole_Gross_Paid_Claim_Fee_SCC,
SUM(Whole_Gross_Paid_Claim_Fee_OCC) AS Whole_Gross_Paid_Claim_Fee_OCC,
SUM(
Market_Gross_Paid_Claim_Fee_SCC
) AS Market_Gross_Paid_Claim_Fee_SCC,
SUM(
Market_Gross_Paid_Claim_Fee_OCC
) AS Market_Gross_Paid_Claim_Fee_OCC,
SUM(Line_Gross_Paid_Claim_Fee_SCC) AS Line_Gross_Paid_Claim_Fee_SCC,
SUM(Line_Gross_Paid_Claim_Fee_OCC) AS Line_Gross_Paid_Claim_Fee_OCC,
SUM(Whole_Gross_Paid_Claim_SCC) AS Whole_Gross_Paid_Claim_SCC,
SUM(Whole_Gross_Paid_Claim_OCC) AS Whole_Gross_Paid_Claim_OCC,
SUM(Market_Gross_Paid_Claim_SCC) AS Market_Gross_Paid_Claim_SCC,
SUM(Market_Gross_Paid_Claim_OCC) AS Market_Gross_Paid_Claim_OCC,
SUM(Line_Gross_Paid_Claim_SCC) AS Line_Gross_Paid_Claim_SCC,
SUM(Line_Gross_Paid_Claim_OCC) AS Line_Gross_Paid_Claim_OCC,
SUM(
Whole_Gross_Outstanding_Claim_Indemnity_SCC
) AS Whole_Gross_Outstanding_Claim_Indemnity_SCC,
SUM(
Whole_Gross_Outstanding_Claim_Indemnity_OCC
) AS Whole_Gross_Outstanding_Claim_Indemnity_OCC,
SUM(
Market_Gross_Outstanding_Claim_Indemnity_SCC
) AS Market_Gross_Outstanding_Claim_Indemnity_SCC,
SUM(
Market_Gross_Outstanding_Claim_Indemnity_OCC
) AS Market_Gross_Outstanding_Claim_Indemnity_OCC,
SUM(
Line_Gross_Outstanding_Claim_Indemnity_SCC
) AS Line_Gross_Outstanding_Claim_Indemnity_SCC,
SUM(
Line_Gross_Outstanding_Claim_Indemnity_OCC
) AS Line_Gross_Outstanding_Claim_Indemnity_OCC,
SUM(
Whole_Gross_Outstanding_Claim_Fee_SCC
) AS Whole_Gross_Outstanding_Claim_Fee_SCC,
SUM(
Whole_Gross_Outstanding_Claim_Fee_OCC
) AS Whole_Gross_Outstanding_Claim_Fee_OCC,
SUM(
Market_Gross_Outstanding_Claim_Fee_SCC
) AS Market_Gross_Outstanding_Claim_Fee_SCC,
SUM(
Market_Gross_Outstanding_Claim_Fee_OCC
) AS Market_Gross_Outstanding_Claim_Fee_OCC,
SUM(
Line_Gross_Outstanding_Claim_Fee_SCC
) AS Line_Gross_Outstanding_Claim_Fee_SCC,
SUM(
Line_Gross_Outstanding_Claim_Fee_OCC
) AS Line_Gross_Outstanding_Claim_Fee_OCC,
SUM(
Whole_Gross_Additional_Outstanding_Claim_Indemnity_SCC
) AS Whole_Gross_Additional_Outstanding_Claim_Indemnity_SCC,
SUM(
Whole_Gross_Additional_Outstanding_Claim_Indemnity_OCC
) AS Whole_Gross_Additional_Outstanding_Claim_Indemnity_OCC,
SUM(
Market_Gross_Additional_Outstanding_Claim_Indemnity_SCC
) AS Market_Gross_Additional_Outstanding_Claim_Indemnity_SCC,
SUM(
Market_Gross_Additional_Outstanding_Claim_Indemnity_OCC
) AS Market_Gross_Additional_Outstanding_Claim_Indemnity_OCC,
SUM(
Line_Gross_Additional_Outstanding_Claim_Indemnity_SCC
) AS Line_Gross_Additional_Outstanding_Claim_Indemnity_SCC,
SUM(
Line_Gross_Additional_Outstanding_Claim_Indemnity_OCC
) AS Line_Gross_Additional_Outstanding_Claim_Indemnity_OCC,
SUM(
Whole_Gross_Additional_Outstanding_Claim_Fee_SCC
) AS Whole_Gross_Additional_Outstanding_Claim_Fee_SCC,
SUM(
Whole_Gross_Additional_Outstanding_Claim_Fee_OCC
) AS Whole_Gross_Additional_Outstanding_Claim_Fee_OCC,
SUM(
Market_Gross_Additional_Outstanding_Claim_Fee_SCC
) AS Market_Gross_Additional_Outstanding_Claim_Fee_SCC,
SUM(
Market_Gross_Additional_Outstanding_Claim_Fee_OCC
) AS Market_Gross_Additional_Outstanding_Claim_Fee_OCC,
SUM(
Line_Gross_Additional_Outstanding_Claim_Fee_SCC
) AS Line_Gross_Additional_Outstanding_Claim_Fee_SCC,
SUM(
Line_Gross_Additional_Outstanding_Claim_Fee_OCC
) AS Line_Gross_Additional_Outstanding_Claim_Fee_OCC,
SUM(
Whole_Gross_Outstanding_Claim_SCC
) AS Whole_Gross_Outstanding_Claim_SCC,
SUM(
Whole_Gross_Outstanding_Claim_OCC
) AS Whole_Gross_Outstanding_Claim_OCC,
SUM(
Market_Gross_Outstanding_Claim_SCC
) AS Market_Gross_Outstanding_Claim_SCC,
SUM(
Market_Gross_Outstanding_Claim_OCC
) AS Market_Gross_Outstanding_Claim_OCC,
SUM(
Line_Gross_Outstanding_Claim_SCC
) AS Line_Gross_Outstanding_Claim_SCC,
SUM(
Line_Gross_Outstanding_Claim_OCC
) AS Line_Gross_Outstanding_Claim_OCC,
SUM(
Whole_Gross_Additional_Outstanding_Claim_SCC
) AS Whole_Gross_Additional_Outstanding_Claim_SCC,
SUM(
Whole_Gross_Additional_Outstanding_Claim_OCC
) AS Whole_Gross_Additional_Outstanding_Claim_OCC,
SUM(
Market_Gross_Additional_Outstanding_Claim_SCC
) AS Market_Gross_Additional_Outstanding_Claim_SCC,
SUM(
Market_Gross_Additional_Outstanding_Claim_OCC
) AS Market_Gross_Additional_Outstanding_Claim_OCC,
SUM(
Line_Gross_Additional_Outstanding_Claim_SCC
) AS Line_Gross_Additional_Outstanding_Claim_SCC,
SUM(
CAST(
Line_Gross_Additional_Outstanding_Claim_OCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Additional_Outstanding_Claim_OCC,
SUM(
CAST(
Whole_Gross_Reported_Outstanding_Claim_SCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Reported_Outstanding_Claim_SCC,
SUM(
CAST(
Whole_Gross_Reported_Outstanding_Claim_OCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Reported_Outstanding_Claim_OCC,
SUM(
CAST(
Market_Gross_Reported_Outstanding_Claim_SCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Reported_Outstanding_Claim_SCC,
SUM(
CAST(
Market_Gross_Reported_Outstanding_Claim_OCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Reported_Outstanding_Claim_OCC,
SUM(
CAST(
Line_Gross_Reported_Outstanding_Claim_SCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Reported_Outstanding_Claim_SCC,
SUM(
CAST(
Line_Gross_Reported_Outstanding_Claim_OCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Reported_Outstanding_Claim_OCC,
SUM(
CAST(
Whole_Gross_Incurred_Claim_Indemnity_SCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Incurred_Claim_Indemnity_SCC,
SUM(
CAST(
Whole_Gross_Incurred_Claim_Indemnity_OCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Incurred_Claim_Indemnity_OCC,
SUM(
CAST(
Market_Gross_Incurred_Claim_Indemnity_SCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Incurred_Claim_Indemnity_SCC,
SUM(
CAST(
Market_Gross_Incurred_Claim_Indemnity_OCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Incurred_Claim_Indemnity_OCC,
SUM(
CAST(
Line_Gross_Incurred_Claim_Indemnity_SCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Incurred_Claim_Indemnity_SCC,
SUM(
CAST(
Line_Gross_Incurred_Claim_Indemnity_OCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Incurred_Claim_Indemnity_OCC,
SUM(
CAST(
Whole_Gross_Incurred_Claim_Fee_SCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Incurred_Claim_Fee_SCC,
SUM(
CAST(
Whole_Gross_Incurred_Claim_Fee_OCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Incurred_Claim_Fee_OCC,
SUM(
CAST(
Market_Gross_Incurred_Claim_Fee_SCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Incurred_Claim_Fee_SCC,
SUM(
CAST(
Market_Gross_Incurred_Claim_Fee_OCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Incurred_Claim_Fee_OCC,
SUM(
CAST(
Line_Gross_Incurred_Claim_Fee_SCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Incurred_Claim_Fee_SCC,
SUM(
CAST(
Line_Gross_Incurred_Claim_Fee_OCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Incurred_Claim_Fee_OCC,
SUM(
CAST(
Whole_Gross_Incurred_Claim_SCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Incurred_Claim_SCC,
SUM(
CAST(
Whole_Gross_Incurred_Claim_OCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Incurred_Claim_OCC,
SUM(
CAST(
Market_Gross_Incurred_Claim_SCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Incurred_Claim_SCC,
SUM(
CAST(
Market_Gross_Incurred_Claim_OCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Incurred_Claim_OCC,
SUM(
CAST(
Line_Gross_Incurred_Claim_SCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Incurred_Claim_SCC,
SUM(
CAST(
Line_Gross_Incurred_Claim_OCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Incurred_Claim_OCC,
SUM(
CAST(
Market_Gross_Paid_Claim_Loss_Fund_SCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Paid_Claim_Loss_Fund_SCC,
SUM(
CAST(
Line_Gross_Paid_Claim_Loss_Fund_SCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Paid_Claim_Loss_Fund_SCC,
SUM(
CAST(
Whole_Gross_Paid_Claim_Loss_Fund_SCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Paid_Claim_Loss_Fund_SCC,
SUM(
CAST(
Market_Gross_Paid_Claim_Loss_Fund_OCC AS DECIMAL(28, 2)
)
) AS Market_Gross_Paid_Claim_Loss_Fund_OCC,
SUM(
CAST(
Line_Gross_Paid_Claim_Loss_Fund_OCC AS DECIMAL(28, 2)
)
) AS Line_Gross_Paid_Claim_Loss_Fund_OCC,
SUM(
CAST(
Whole_Gross_Paid_Claim_Loss_Fund_OCC AS DECIMAL(28, 2)
)
) AS Whole_Gross_Paid_Claim_Loss_Fund_OCC
from dwh.fact_claim where dwh_ACTION_TYPE !='D'
"""
pd_dataframe_fact_claim = pd.read_sql_query(pd_metric_chk_fct_claim, connDw)

df_fact_claim = spark.createDataFrame(pd_dataframe_fact_claim)
df_fact_claim_count = df_fact_claim.drop('table_type').dropDuplicates().count()
if(df_fact_claim_count >1):
    print('Output : Metric check is failed for Fact Claim ')
    display(df_fact_claim)
else:
    print('Output : Metric check is passed for Fact Claim !!')


# COMMAND ----------

# DBTITLE 1,Metric Check (Fact Limit) for Phase A Fact - Should get no records in output
pd_metric_chk_fact_limit = """
select 'STAGING' as table_type, sum(limit_amount) as limit_lcc,
sum(CAST(CAST(COALESCE(limit.limit_amount,0.00) AS DECIMAL(28, 2)) * COALESCE(line.calculated_line,0.00) / 100 AS DECIMAL(28, 2))) AS line_size_lcc 
from (
select * from staging.limit WHERE lakeisactive = 1 and lakedeletedtimestamp is null AND COALESCE(limit_amount,0.00) <> 0.00
) limit
inner join
(select * from staging.line WHERE lakeisactive = 1 and lakedeletedtimestamp is null AND COALESCE(line.calculated_line,0.00) <> 0.00) line
on limit.Policy_Header_Reference = line.Policy_Header_Reference
and limit.Policy_Section_Reference = line.Policy_Section_Reference
union all
select 'FACT'  as table_type, sum(limit_lcc),COALESCE( sum(line_size_lcc),0.00) from dwh.fact_limit 
"""

pd_dataframe_fact_limit = pd.read_sql_query(pd_metric_chk_fact_limit, connDw)

df_fact_limit = spark.createDataFrame(pd_dataframe_fact_limit)
df_fact_limit_count = df_fact_limit.drop('table_type').dropDuplicates().count()
if(df_fact_limit_count >1):
    print('Output : Metric check is failed for Fact Limit ')
    display(df_fact_limit)
else:
    print('Output : Metric check is passed for Fact Limit !!')

# COMMAND ----------

# DBTITLE 1,Metric Check (Fact Pricing Rating) for Phase A Fact - Should get no records in output
pd_pricing_rating = """ 
SELECT 'staging' as table_type, SUM(CAST(market_benchmark_price_scc AS [decimal](28,2))) as market_benchmark_price_scc, SUM(CAST(line_benchmark_price_scc AS [decimal](28,2))) as line_benchmark_price_scc, SUM(CAST(whole_benchmark_price_scc AS [decimal](28,2))) as whole_benchmark_price_scc, SUM(CAST(market_benchmark_price_occ AS [decimal](28,2))) as market_benchmark_price_occ, SUM(CAST(line_benchmark_price_occ AS [decimal](28,2))) as line_benchmark_price_occ, SUM(CAST(whole_benchmark_price_occ AS [decimal](28,2))) as whole_benchmark_price_occ, SUM(CAST(market_change_in_attach_Point_scc AS [decimal](28,2))) as market_change_in_attach_Point_scc, SUM(CAST(line_change_in_attach_Point_scc AS [decimal](28,2))) as line_change_in_attach_Point_scc, SUM(CAST(whole_change_in_attach_Point_scc AS [decimal](28,2))) as whole_change_in_attach_Point_scc, SUM(CAST(market_change_in_attach_Point_occ AS [decimal](28,2))) as market_change_in_attach_Point_occ, SUM(CAST(line_change_in_attach_Point_occ AS [decimal](28,2))) as line_change_in_attach_Point_occ, SUM(CAST(whole_change_in_attach_Point_occ AS [decimal](28,2))) as whole_change_in_attach_Point_occ, SUM(CAST(market_change_in_breadth_cover_scc AS [decimal](28,2))) as market_change_in_breadth_cover_scc, SUM(CAST(line_change_in_breadth_cover_scc AS [decimal](28,2))) as line_change_in_breadth_cover_scc, SUM(CAST(whole_change_in_breadth_cover_scc AS [decimal](28,2))) as whole_change_in_breadth_cover_scc, SUM(CAST(market_change_in_breadth_cover_occ AS [decimal](28,2))) as market_change_in_breadth_cover_occ, SUM(CAST(line_change_in_breadth_cover_occ AS [decimal](28,2))) as line_change_in_breadth_cover_occ, SUM(CAST(whole_change_in_breadth_cover_occ AS [decimal](28,2))) as whole_change_in_breadth_cover_occ, SUM(CAST(market_change_in_pure_rate_scc AS [decimal](28,2))) as market_change_in_pure_rate_scc, SUM(CAST(line_change_in_pure_rate_scc AS [decimal](28,2))) as line_change_in_pure_rate_scc, SUM(CAST(whole_change_in_pure_rate_scc AS [decimal](28,2))) as whole_change_in_pure_rate_scc, SUM(CAST(market_change_in_pure_rate_occ AS [decimal](28,2))) as market_change_in_pure_rate_occ, SUM(CAST(line_change_in_pure_rate_occ AS [decimal](28,2))) as line_change_in_pure_rate_occ, SUM(CAST(whole_change_in_pure_rate_occ AS [decimal](28,2))) as whole_change_in_pure_rate_occ, SUM(CAST(market_change_in_other_factors_scc AS [decimal](28,2))) as market_change_in_other_factors_scc, SUM(CAST(line_change_in_other_factors_scc AS [decimal](28,2))) as line_change_in_other_factors_scc, SUM(CAST(whole_change_in_other_factors_scc AS [decimal](28,2))) as whole_change_in_other_factors_scc, SUM(CAST(market_change_in_other_factors_occ AS [decimal](28,2))) as market_change_in_other_factors_occ, SUM(CAST(line_change_in_other_factors_occ AS [decimal](28,2))) as line_change_in_other_factors_occ, SUM(CAST(whole_change_in_other_factors_occ AS [decimal](28,2))) as whole_change_in_other_factors_occ FROM ( SELECT CAST ( COALESCE(sl.signed_order, sl.written_order) * pr.expiring_whole_premium_written_scc * pr.benchmark_percentage AS [decimal](28,2)) as market_benchmark_price_scc, CAST ( sl.calculated_line * pr.expiring_whole_premium_written_scc * pr.benchmark_percentage AS [decimal](28,2)) as line_benchmark_price_scc, CAST ( 1 * pr.expiring_whole_premium_written_scc * pr.benchmark_percentage AS [decimal](28,2) ) as Whole_benchmark_price_scc, CAST ( COALESCE(sl.signed_order, sl.written_order) * pr.expiring_whole_premium_written_occ * pr.benchmark_percentage AS [decimal](28,2) ) as market_benchmark_price_occ, CAST ( sl.calculated_line * pr.expiring_whole_premium_written_occ * pr.benchmark_percentage AS [decimal](28,2) ) as line_benchmark_price_occ, CAST ( 1 * pr.expiring_whole_premium_written_occ * pr.benchmark_percentage AS [decimal](28,2) ) as whole_benchmark_price_occ, CAST ( COALESCE(sl.signed_order, sl.written_order) * pr.expiring_whole_premium_written_scc * pr.change_in_Deduction_attachment_Point_Percentage AS [decimal](28,2) ) as market_change_in_attach_Point_scc, CAST ( sl.calculated_line * pr.expiring_whole_premium_written_scc * pr.change_in_Deduction_attachment_Point_Percentage AS [decimal](28,2) ) as line_change_in_attach_point_scc, CAST ( 1 * pr.expiring_whole_premium_written_scc * pr.change_in_Deduction_attachment_Point_Percentage AS [decimal](28,2) ) as whole_change_in_attach_point_scc, CAST ( COALESCE(sl.signed_order, sl.written_order) * pr.expiring_whole_premium_written_occ * pr.change_in_Deduction_attachment_Point_Percentage AS [decimal](28,2) ) as market_change_in_attach_Point_occ, CAST ( sl.calculated_line * pr.expiring_whole_premium_written_occ * pr.change_in_Deduction_attachment_Point_Percentage AS [decimal](28,2) ) as line_change_in_attach_point_occ, CAST ( 1 * pr.expiring_whole_premium_written_occ * pr.change_in_Deduction_attachment_Point_Percentage AS [decimal](28,2) ) as whole_change_in_attach_point_occ, CAST ( COALESCE(sl.signed_order, sl.written_order) * pr.expiring_whole_premium_written_scc * pr.change_in_breadth_cover_percentage AS [decimal](28,2) ) as market_change_in_breadth_cover_scc, CAST ( sl.calculated_line * pr.expiring_whole_premium_written_scc * pr.change_in_breadth_cover_percentage AS [decimal](28,2) ) as line_change_in_breadth_cover_scc, CAST ( 1 * pr.expiring_whole_premium_written_scc * pr.change_in_breadth_cover_percentage AS [decimal](28,2) ) as whole_change_in_breadth_cover_scc, CAST ( COALESCE(sl.signed_order, sl.written_order) * pr.expiring_whole_premium_written_occ * pr.change_in_breadth_cover_percentage AS [decimal](28,2) ) as market_change_in_breadth_cover_occ, CAST ( sl.calculated_line * pr.expiring_whole_premium_written_occ * pr.change_in_breadth_cover_percentage AS [decimal](28,2) ) as line_change_in_breadth_cover_occ, CAST ( 1 * pr.expiring_whole_premium_written_occ * pr.change_in_breadth_cover_percentage AS [decimal](28,2) ) as whole_change_in_breadth_cover_occ, CAST ( COALESCE(sl.signed_order, sl.written_order) * pr.expiring_whole_premium_written_scc * pr.change_in_pure_rate_percentage AS [decimal](28,2) ) as market_change_in_pure_rate_scc, CAST ( sl.calculated_line * pr.expiring_whole_premium_written_scc * pr.change_in_pure_rate_percentage AS [decimal](28,2) ) as line_change_in_pure_rate_scc, CAST ( 1 * pr.expiring_whole_premium_written_scc * pr.change_in_pure_rate_percentage AS [decimal](28,2) ) as whole_change_in_pure_rate_scc, CAST ( COALESCE(sl.signed_order, sl.written_order) * pr.expiring_whole_premium_written_occ * pr.change_in_pure_rate_percentage AS [decimal](28,2) ) as market_change_in_pure_rate_occ, CAST ( sl.calculated_line * pr.expiring_whole_premium_written_occ * pr.change_in_pure_rate_percentage AS [decimal](28,2) ) as line_change_in_pure_rate_occ, CAST ( 1 * pr.expiring_whole_premium_written_occ * pr.change_in_pure_rate_percentage AS [decimal](28,2) ) as whole_change_in_pure_rate_occ, CAST ( COALESCE(sl.signed_order, sl.written_order) * pr.expiring_whole_premium_written_scc * pr.change_in_other_factors_percentage AS [decimal](28,2) ) as market_change_in_other_factors_scc, CAST ( sl.calculated_line * pr.expiring_whole_premium_written_scc * pr.change_in_other_factors_percentage AS [decimal](28,2) ) as line_change_in_other_factors_scc, CAST ( 1 * pr.expiring_whole_premium_written_scc * pr.change_in_other_factors_percentage AS [decimal](28,2) ) as whole_change_in_other_factors_scc, CAST ( COALESCE(sl.signed_order, sl.written_order) * pr.expiring_whole_premium_written_occ * pr.change_in_other_factors_percentage AS [decimal](28,2) ) as market_change_in_other_factors_occ, CAST ( sl.calculated_line * pr.expiring_whole_premium_written_occ * pr.change_in_other_factors_percentage AS [decimal](28,2) ) as line_change_in_other_factors_occ, CAST ( 1 * pr.expiring_whole_premium_written_occ * pr.change_in_other_factors_percentage AS [decimal](28,2) ) as whole_change_in_other_factors_occ From ( select * from staging.pricing_rating WHERE lakeisactive=1  and lakedeletedtimestamp is null 
AND (COALESCE(expiring_whole_premium_written_scc, 0.00) <> 0.00 
OR COALESCE(benchmark_percentage, 0.00) <> 0.00 
OR COALESCE(expiring_whole_premium_written_occ, 0.00)  <> 0.00 
OR COALESCE(change_in_Deduction_attachment_Point_Percentage, 0.00) <> 0.00 
OR COALESCE(change_in_breadth_cover_percentage, 0.00) <> 0.00 
OR COALESCE(change_in_pure_rate_percentage, 0.00) <> 0.00 
OR COALESCE(change_in_other_factors_percentage, 0.00) <> 0.00)) pr 
INNER JOIN ( SELECT * FROM staging.line WHERE lakeisactive=1  and lakedeletedtimestamp is null 
AND (COALESCE(calculated_line, 0.00) <> 0.00
OR COALESCE(signed_order, written_order) <> 0.00)
) sl ON pr.policy_section_reference = sl.policy_section_reference AND pr.policy_header_reference = sl.policy_header_reference ) a 

UNION ALL 

SELECT 'fact' as table_type, SUM(CAST(market_benchmark_price_scc AS [decimal](28,2))) as market_benchmark_price_scc, SUM(CAST(line_benchmark_price_scc AS [decimal](28,2))) as line_benchmark_price_scc, SUM(CAST(whole_benchmark_price_scc AS [decimal](28,2))) as whole_benchmark_price_scc, SUM(CAST(market_benchmark_price_occ AS [decimal](28,2))) as market_benchmark_price_occ, SUM(CAST(line_benchmark_price_occ AS [decimal](28,2))) as line_benchmark_price_occ, SUM(CAST(whole_benchmark_price_occ AS [decimal](28,2))) as whole_benchmark_price_occ, SUM(CAST(market_change_in_attach_Point_scc AS [decimal](28,2))) as market_change_in_attach_Point_scc, SUM(CAST(line_change_in_attach_Point_scc AS [decimal](28,2))) as line_change_in_attach_Point_scc, SUM(CAST(whole_change_in_attach_Point_scc AS [decimal](28,2))) as whole_change_in_attach_Point_scc, SUM(CAST(market_change_in_attach_Point_occ AS [decimal](28,2))) as market_change_in_attach_Point_occ, SUM(CAST(line_change_in_attach_Point_occ AS [decimal](28,2))) as line_change_in_attach_Point_occ, SUM(CAST(whole_change_in_attach_Point_occ AS [decimal](28,2))) as whole_change_in_attach_Point_occ, SUM(CAST(market_change_in_breadth_cover_scc AS [decimal](28,2))) as market_change_in_breadth_cover_scc, SUM(CAST(line_change_in_breadth_cover_scc AS [decimal](28,2))) as line_change_in_breadth_cover_scc, SUM(CAST(whole_change_in_breadth_cover_scc AS [decimal](28,2))) as whole_change_in_breadth_cover_scc, SUM(CAST(market_change_in_breadth_cover_occ AS [decimal](28,2))) as market_change_in_breadth_cover_occ, SUM(CAST(line_change_in_breadth_cover_occ AS [decimal](28,2))) as line_change_in_breadth_cover_occ, SUM(CAST(whole_change_in_breadth_cover_occ AS [decimal](28,2))) as whole_change_in_breadth_cover_occ, SUM(CAST(market_change_in_pure_rate_scc AS [decimal](28,2))) as market_change_in_pure_rate_scc, SUM(CAST(line_change_in_pure_rate_scc AS [decimal](28,2))) as line_change_in_pure_rate_scc, SUM(CAST(whole_change_in_pure_rate_scc AS [decimal](28,2))) as whole_change_in_pure_rate_scc, SUM(CAST(market_change_in_pure_rate_occ AS [decimal](28,2))) as market_change_in_pure_rate_occ, SUM(CAST(line_change_in_pure_rate_occ AS [decimal](28,2))) as line_change_in_pure_rate_occ, SUM(CAST(whole_change_in_pure_rate_occ AS [decimal](28,2))) as whole_change_in_pure_rate_occ, SUM(CAST(market_change_in_other_factors_scc AS [decimal](28,2))) as market_change_in_other_factors_scc, SUM(CAST(line_change_in_other_factors_scc AS [decimal](28,2))) as line_change_in_other_factors_scc, SUM(CAST(whole_change_in_other_factors_scc AS [decimal](28,2))) as whole_change_in_other_factors_scc, SUM(CAST(market_change_in_other_factors_occ AS [decimal](28,2))) as market_change_in_other_factors_occ, SUM(CAST(line_change_in_other_factors_occ AS [decimal](28,2))) as line_change_in_other_factors_occ, SUM(CAST(whole_change_in_other_factors_occ AS [decimal](28,2))) as whole_change_in_other_factors_occ FROM dwh.Fact_Pricing_Rating 
"""

pd_dataframe_fact_pricing_rating = pd.read_sql_query(pd_pricing_rating, connDw)

df_fact_pricing_rating = spark.createDataFrame(pd_dataframe_fact_pricing_rating)
df_fact_pricing_rating_count = df_fact_pricing_rating.drop('table_type').dropDuplicates().count()
if(df_fact_pricing_rating_count >1):
    print('Output : Metric check is failed for Fact Pricing Rating ')
    display(df_fact_pricing_rating)
else:
    print('Output : Metric check is passed for Fact Pricing Rating !!')


# COMMAND ----------

# DBTITLE 1,Metric Check (Fact Signed Amount) for Phase A Fact - Should get no records in output
pd_signed_amt = """ 
select 'staging' as table_type,
       sum(cast(Line_Gross_Gross_Signed_Premium_OCC as decimal(28, 2))) as Line_Gross_Gross_Signed_Premium_OCC,
       sum(cast(Whole_Gross_Gross_Signed_Premium_OCC as decimal(28, 2))) as Whole_Gross_Gross_Signed_Premium_OCC,
       sum(cast(Market_Gross_Net_Signed_Premium_SCC as decimal(28, 2))) as Market_Gross_Net_Signed_Premium_SCC,
       sum(cast(Line_Gross_Net_Signed_Premium_SCC as decimal(28, 2))) as Line_Gross_Net_Signed_Premium_SCC,
       sum(cast(Whole_Gross_Net_Signed_Premium_SCC as decimal(28, 2))) as Whole_Gross_Net_Signed_Premium_SCC,
       sum(cast(Market_Gross_Net_Signed_Premium_OCC as decimal(28, 2))) as Market_Gross_Net_Signed_Premium_OCC,
       sum(cast(Line_Gross_Net_Signed_Premium_OCC as decimal(28, 2))) as Line_Gross_Net_Signed_Premium_OCC,
       sum(cast(Whole_Gross_Net_Signed_Premium_OCC as decimal(28, 2))) as Whole_Gross_Net_Signed_Premium_OCC,
       sum(cast(Market_Gross_Signed_Acquisition_Cost_SCC as decimal(28, 2))) as Market_Gross_Signed_Acquisition_Cost_SCC,
       sum(cast(Line_Gross_Signed_Acquisition_Cost_SCC as decimal(28, 2))) as Line_Gross_Signed_Acquisition_Cost_SCC,
       sum(cast(Whole_Gross_Signed_Acquisition_Cost_SCC as decimal(28, 2))) as Whole_Gross_Signed_Acquisition_Cost_SCC,
       sum(cast(Market_Gross_Signed_Acquisition_Cost_OCC as decimal(28, 2))) as Market_Gross_Signed_Acquisition_Cost_OCC,
       sum(cast(Line_Gross_Signed_Acquisition_Cost_OCC as decimal(28, 2))) as Line_Gross_Signed_Acquisition_Cost_OCC,
       sum(cast(Whole_Gross_Signed_Acquisition_Cost_OCC as decimal(28, 2))) as Whole_Gross_Signed_Acquisition_Cost_OCC,
       sum(cast(Market_Gross_Signed_Profit_Commission_SCC as decimal(28, 2))) as Market_Gross_Signed_Profit_Commission_SCC,
       sum(cast(Line_Gross_Signed_Profit_Commission_SCC as decimal(28, 2))) as Line_Gross_Signed_Profit_Commission_SCC,
       sum(cast(Whole_Gross_Signed_Profit_Commission_SCC as decimal(28, 2))) as Whole_Gross_Signed_Profit_Commission_SCC,
       sum(cast(Market_Gross_Signed_Profit_Commission_OCC as decimal(28, 2))) as Market_Gross_Signed_Profit_Commission_OCC,
       sum(cast(Line_Gross_Signed_Profit_Commission_OCC as decimal(28, 2))) as Line_Gross_Signed_Profit_Commission_OCC,
       sum(cast(Whole_Gross_Signed_Profit_Commission_OCC as decimal(28, 2))) as Whole_Gross_Signed_Profit_Commission_OCC,
       sum(cast(Market_Gross_Signed_Reinstatement_Premium_SCC as decimal(28, 2))) as Market_Gross_Signed_Reinstatement_Premium_SCC,
       sum(cast(Line_Gross_Signed_Reinstatement_Premium_SCC as decimal(28, 2))) as Line_Gross_Signed_Reinstatement_Premium_SCC,
       sum(cast(Whole_Gross_Signed_Reinstatement_Premium_SCC as decimal(28, 2))) as Whole_Gross_Signed_Reinstatement_Premium_SCC,
       sum(cast(Market_Gross_Signed_Reinstatement_Premium_OCC as decimal(28, 2))) as Market_Gross_Signed_Reinstatement_Premium_OCC,
       sum(cast(Line_Gross_Signed_Reinstatement_Premium_OCC as decimal(28, 2))) as Line_Gross_Signed_Reinstatement_Premium_OCC,
       sum(cast(Whole_Gross_Signed_Reinstatement_Premium_OCC as decimal(28, 2))) as Whole_Gross_Signed_Reinstatement_Premium_OCC,
       sum(cast(Market_Gross_Paid_Claim_SCC as decimal(28, 2))) as Market_Gross_Paid_Claim_SCC,
       sum(cast(Line_Gross_Paid_Claim_SCC as decimal(28, 2))) as Line_Gross_Paid_Claim_SCC,
       sum(cast(Whole_Gross_Paid_Claim_SCC as decimal(28, 2))) as Whole_Gross_Paid_Claim_SCC,
       sum(cast(Market_Gross_Paid_Claim_OCC as decimal(28, 2))) as Market_Gross_Paid_Claim_OCC,
       sum(cast(Line_Gross_Paid_Claim_OCC as decimal(28, 2))) as Line_Gross_Paid_Claim_OCC,
       sum(cast(Whole_Gross_Paid_Claim_OCC as decimal(28, 2))) as Whole_Gross_Paid_Claim_OCC,
       sum(cast(Market_Gross_Paid_Claim_Loss_Fund_SCC as decimal(28, 2))) as Market_Gross_Paid_Claim_Loss_Fund_SCC,
       sum(cast(Line_Gross_Paid_Claim_Loss_Fund_SCC as decimal(28, 2))) as Line_Gross_Paid_Claim_Loss_Fund_SCC,
       sum(cast(Whole_Gross_Paid_Claim_Loss_Fund_SCC as decimal(28, 2))) as Whole_Gross_Paid_Claim_Loss_Fund_SCC,
       sum(cast(Market_Gross_Paid_Claim_Loss_Fund_OCC as decimal(28, 2))) as Market_Gross_Paid_Claim_Loss_Fund_OCC,
       sum(cast(Line_Gross_Paid_Claim_Loss_Fund_OCC as decimal(28, 2))) as Line_Gross_Paid_Claim_Loss_Fund_OCC,
       sum(cast(Whole_Gross_Paid_Claim_Loss_Fund_OCC as decimal(28, 2))) as Whole_Gross_Paid_Claim_Loss_Fund_OCC,
       sum(cast(Market_Gross_Signed_Overseas_Tax_SCC as decimal(28, 2))) as Market_Gross_Signed_Overseas_Tax_SCC,
       sum(cast(Line_Gross_Signed_Overseas_Tax_SCC as decimal(28, 2))) as Line_Gross_Signed_Overseas_Tax_SCC,
       sum(cast(Whole_Gross_Signed_Overseas_Tax_SCC as decimal(28, 2))) as Whole_Gross_Signed_Overseas_Tax_SCC,
       sum(cast(Market_Gross_Signed_Overseas_Tax_OCC as decimal(28, 2))) as Market_Gross_Signed_Overseas_Tax_OCC,
       sum(cast(Line_Gross_Signed_Overseas_Tax_OCC as decimal(28, 2))) as Line_Gross_Signed_Overseas_Tax_OCC,
       sum(cast(Whole_Gross_Signed_Overseas_Tax_OCC as decimal(28, 2))) as Whole_Gross_Signed_Overseas_Tax_OCC
from
(
    SELECT lakevalidfromtimestamp,
           lakedeletedtimestamp,
           Market_Gross_Gross_Signed_Premium_SCC,
           Line_Gross_Gross_Signed_Premium_SCC,
           Whole_Gross_Gross_Signed_Premium_SCC,
           Market_Gross_Gross_Signed_Premium_OCC,
           Line_Gross_Gross_Signed_Premium_OCC,
           Whole_Gross_Gross_Signed_Premium_OCC,
           Market_Gross_Net_Signed_Premium_SCC,
           Line_Gross_Net_Signed_Premium_SCC,
           Whole_Gross_Net_Signed_Premium_SCC,
           Market_Gross_Net_Signed_Premium_OCC,
           Line_Gross_Net_Signed_Premium_OCC,
           Whole_Gross_Net_Signed_Premium_OCC,
           Market_Gross_Signed_Acquisition_Cost_SCC,
           Line_Gross_Signed_Acquisition_Cost_SCC,
           Whole_Gross_Signed_Acquisition_Cost_SCC,
           Market_Gross_Signed_Acquisition_Cost_OCC,
           Line_Gross_Signed_Acquisition_Cost_OCC,
           Whole_Gross_Signed_Acquisition_Cost_OCC,
           Market_Gross_Signed_Profit_Commission_SCC,
           Line_Gross_Signed_Profit_Commission_SCC,
           Whole_Gross_Signed_Profit_Commission_SCC,
           Market_Gross_Signed_Profit_Commission_OCC,
           Line_Gross_Signed_Profit_Commission_OCC,
           Whole_Gross_Signed_Profit_Commission_OCC,
           Market_Gross_Signed_Reinstatement_Premium_SCC,
           Line_Gross_Signed_Reinstatement_Premium_SCC,
           Whole_Gross_Signed_Reinstatement_Premium_SCC,
           Market_Gross_Signed_Reinstatement_Premium_OCC,
           Line_Gross_Signed_Reinstatement_Premium_OCC,
           Whole_Gross_Signed_Reinstatement_Premium_OCC,
           Market_Gross_Paid_Claim_SCC,
           Line_Gross_Paid_Claim_SCC,
           Whole_Gross_Paid_Claim_SCC,
           Market_Gross_Paid_Claim_OCC,
           Line_Gross_Paid_Claim_OCC,
           Whole_Gross_Paid_Claim_OCC,
           Market_Gross_Paid_Claim_Loss_Fund_SCC,
           Line_Gross_Paid_Claim_Loss_Fund_SCC,
           Whole_Gross_Paid_Claim_Loss_Fund_SCC,
           Market_Gross_Paid_Claim_Loss_Fund_OCC,
           Line_Gross_Paid_Claim_Loss_Fund_OCC,
           Whole_Gross_Paid_Claim_Loss_Fund_OCC,
           Market_Gross_Signed_Overseas_Tax_SCC,
           Line_Gross_Signed_Overseas_Tax_SCC,
           Whole_Gross_Signed_Overseas_Tax_SCC,
           Market_Gross_Signed_Overseas_Tax_OCC,
           Line_Gross_Signed_Overseas_Tax_OCC,
           Whole_Gross_Signed_Overseas_Tax_OCC
    FROM
    (
        SELECT CASE
                   WHEN Transaction_Line_Code = 'GG' THEN
                       Whole_SCC
                   ELSE
                       0.00000000
               END AS Whole_Gross_Gross_Signed_Premium_SCC,
               CASE
                   WHEN Transaction_Line_Code = 'GG' THEN
                       Whole_OCC
                   ELSE
                       0.00000000
               END AS Whole_Gross_Gross_Signed_Premium_OCC,
               CASE
                   WHEN Transaction_Line_Code = 'GG' THEN
                       Market_SCC
                   ELSE
                       0.00000000
               END AS Market_Gross_Gross_Signed_Premium_SCC,
               CASE
                   WHEN Transaction_Line_Code = 'GG' THEN
                       Market_OCC
                   ELSE
                       0.00000000
               END AS Market_Gross_Gross_Signed_Premium_OCC,
               CASE
                   WHEN Transaction_Line_Code = 'GG' THEN
                       Line_SCC
                   ELSE
                       0.00000000
               END AS Line_Gross_Gross_Signed_Premium_SCC,
               CASE
                   WHEN Transaction_Line_Code = 'GG' THEN
                       Line_OCC
                   ELSE
                       0.00000000
               END AS Line_Gross_Gross_Signed_Premium_OCC,
               CASe
                   When Transaction_Line_Code = 'GN' Then
                       Whole_SCC
                   Else
                       0.00000000
               End AS Whole_Gross_Net_Signed_Premium_SCC,
               CASe
                   When Transaction_Line_Code = 'GN' Then
                       Whole_OCC
                   Else
                       0.00000000
               End AS Whole_Gross_Net_Signed_Premium_OCC,
               CASe
                   When Transaction_Line_Code = 'GN' Then
                       Market_SCC
                   Else
                       0.00000000
               End AS Market_Gross_Net_Signed_Premium_SCC,
               CASe
                   When Transaction_Line_Code = 'GN' Then
                       Market_OCC
                   Else
                       0.00000000
               End AS Market_Gross_Net_Signed_Premium_OCC,
               CASe
                   When Transaction_Line_Code = 'GN' Then
                       Line_SCC
                   Else
                       0.00000000
               End AS Line_Gross_Net_Signed_Premium_SCC,
               CASe
                   When Transaction_Line_Code = 'GN' Then
                       Line_OCC
                   Else
                       0.00000000
               End AS Line_Gross_Net_Signed_Premium_OCC,
               CASe
                   When Transaction_Line_Code = 'PC' Then
                       Whole_SCC
                   Else
                       0.00000000
               End AS Whole_Gross_Signed_Profit_Commission_SCC,
               CASe
                   When Transaction_Line_Code = 'PC' Then
                       Whole_OCC
                   Else
                       0.00000000
               End AS Whole_Gross_Signed_Profit_Commission_OCC,
               CASe
                   When Transaction_Line_Code = 'PC' Then
                       Market_SCC
                   Else
                       0.00000000
               End AS Market_Gross_Signed_Profit_Commission_SCC,
               CASe
                   When Transaction_Line_Code = 'PC' Then
                       Market_OCC
                   Else
                       0.00000000
               End AS Market_Gross_Signed_Profit_Commission_OCC,
               CASe
                   When Transaction_Line_Code = 'PC' Then
                       Line_SCC
                   Else
                       0.00000000
               End AS Line_Gross_Signed_Profit_Commission_SCC,
               CASe
                   When Transaction_Line_Code = 'PC' Then
                       Line_OCC
                   Else
                       0.00000000
               End AS Line_Gross_Signed_Profit_Commission_OCC,
               CASe
                   When Transaction_Line_Code = 'RIN' Then
                       Whole_SCC
                   Else
                       0.00000000
               End AS Whole_Gross_Signed_Reinstatement_Premium_SCC,
               CASe
                   When Transaction_Line_Code = 'RIN' Then
                       Whole_OCC
                   Else
                       0.00000000
               End AS Whole_Gross_Signed_Reinstatement_Premium_OCC,
               CASe
                   When Transaction_Line_Code = 'RIN' Then
                       Market_SCC
                   Else
                       0.00000000
               End AS Market_Gross_Signed_Reinstatement_Premium_SCC,
               CASe
                   When Transaction_Line_Code = 'RIN' Then
                       Market_OCC
                   Else
                       0.00000000
               End AS Market_Gross_Signed_Reinstatement_Premium_OCC,
               CASe
                   When Transaction_Line_Code = 'RIN' Then
                       Line_SCC
                   Else
                       0.00000000
               End AS Line_Gross_Signed_Reinstatement_Premium_SCC,
               CASe
                   When Transaction_Line_Code = 'RIN' Then
                       Line_OCC
                   Else
                       0.00000000
               End AS Line_Gross_Signed_Reinstatement_Premium_OCC,
               CASe
                   When Transaction_Line_Code = 'PIN' Then
                       Whole_SCC
                   Else
                       0.00000000
               End AS Whole_Gross_Paid_Claim_SCC,
               CASe
                   When Transaction_Line_Code = 'PIN' Then
                       Whole_OCC
                   Else
                       0.00000000
               End AS Whole_Gross_Paid_Claim_OCC,
               CASe
                   When Transaction_Line_Code = 'PIN' Then
                       Market_SCC
                   Else
                       0.00000000
               End AS Market_Gross_Paid_Claim_SCC,
               CASe
                   When Transaction_Line_Code = 'PIN' Then
                       Market_OCC
                   Else
                       0.00000000
               End AS Market_Gross_Paid_Claim_OCC,
               CASe
                   When Transaction_Line_Code = 'PIN' Then
                       Line_SCC
                   Else
                       0.00000000
               End AS Line_Gross_Paid_Claim_SCC,
               CASe
                   When Transaction_Line_Code = 'PIN' Then
                       Line_OCC
                   Else
                       0.00000000
               End AS Line_Gross_Paid_Claim_OCC,
               CASe
                   When Transaction_Line_Code = 'ACQ' Then
                       Whole_SCC
                   Else
                       0.00000000
               End AS Whole_Gross_Signed_Acquisition_Cost_SCC,
               CASe
                   When Transaction_Line_Code = 'ACQ' Then
                       Whole_OCC
                   Else
                       0.00000000
               End AS Whole_Gross_Signed_Acquisition_Cost_OCC,
               CASe
                   When Transaction_Line_Code = 'ACQ' Then
                       Market_SCC
                   Else
                       0.00000000
               End AS Market_Gross_Signed_Acquisition_Cost_SCC,
               CASe
                   When Transaction_Line_Code = 'ACQ' Then
                       Market_OCC
                   Else
                       0.00000000
               End AS Market_Gross_Signed_Acquisition_Cost_OCC,
               CASe
                   When Transaction_Line_Code = 'ACQ' Then
                       Line_SCC
                   Else
                       0.00000000
               End AS Line_Gross_Signed_Acquisition_Cost_SCC,
               CASe
                   When Transaction_Line_Code = 'ACQ' Then
                       Line_OCC
                   Else
                       0.00000000
               End AS Line_Gross_Signed_Acquisition_Cost_OCC,
               CASe
                   When Transaction_Line_Code = 'PCLF' Then
                       Whole_SCC
                   Else
                       0.00000000
               End AS Whole_Gross_Paid_Claim_Loss_Fund_SCC,
               CASe
                   When Transaction_Line_Code = 'PCLF' Then
                       Whole_OCC
                   Else
                       0.00000000
               End AS Whole_Gross_Paid_Claim_Loss_Fund_OCC,
               CASe
                   When Transaction_Line_Code = 'PCLF' Then
                       Market_SCC
                   Else
                       0.00000000
               End AS Market_Gross_Paid_Claim_Loss_Fund_SCC,
               CASe
                   When Transaction_Line_Code = 'PCLF' Then
                       Market_OCC
                   Else
                       0.00000000
               End AS Market_Gross_Paid_Claim_Loss_Fund_OCC,
               CASe
                   When Transaction_Line_Code = 'PCLF' Then
                       Line_SCC
                   Else
                       0.00000000
               End AS Line_Gross_Paid_Claim_Loss_Fund_SCC,
               CASe
                   When Transaction_Line_Code = 'PCLF' Then
                       Line_OCC
                   Else
                       0.00000000
               End AS Line_Gross_Paid_Claim_Loss_Fund_OCC,
               CASe
                   When Transaction_Line_Code = 'OIT' Then
                       Whole_SCC
                   Else
                       0.00000000
               End AS Whole_Gross_Signed_OverseAS_Tax_SCC,
               CASe
                   When Transaction_Line_Code = 'OIT' Then
                       Whole_OCC
                   Else
                       0.00000000
               End AS Whole_Gross_Signed_OverseAS_Tax_OCC,
               CASe
                   When Transaction_Line_Code = 'OIT' Then
                       Market_SCC
                   Else
                       0.00000000
               End AS Market_Gross_Signed_OverseAS_Tax_SCC,
               CASe
                   When Transaction_Line_Code = 'OIT' Then
                       Market_OCC
                   Else
                       0.00000000
               End AS Market_Gross_Signed_OverseAS_Tax_OCC,
               CASe
                   When Transaction_Line_Code = 'OIT' Then
                       Line_SCC
                   Else
                       0.00000000
               End AS Line_Gross_Signed_OverseAS_Tax_SCC,
               CASe
                   When Transaction_Line_Code = 'OIT' Then
                       Line_OCC
                   Else
                       0.00000000
               End AS Line_Gross_Signed_OverseAS_Tax_OCC,
               CAST(SUBSTRING(
                                 HASHBYTES(
                                              'SHA2_512',
                                              COALESCE(CAST(Signing_Transaction_Reference AS NVARCHAR(200)), '')
                                          ),
                                 1,
                                 7
                             ) AS BIGINT) AS Signing_Transaction_HBK,
               CAST(SUBSTRING(HASHBYTES('SHA2_512', COALESCE(CAST(Transaction_Line_Code AS NVARCHAR(200)), '')), 1, 7) AS BIGINT) AS Transaction_Line_HBK,
               CAST(SUBSTRING(
                                 HASHBYTES(
                                              'SHA2_512',
                                              CONCAT_WS(
                                                           '|',
                                                           COALESCE(CAST(Signing_Transaction_Reference AS NVARCHAR(200)), ''),
                                                           COALESCE(CAST(Transaction_Line_Code AS NVARCHAR(200)), '')
                                                       )
                                          ),
                                 1,
                                 7
                             ) AS BIGINT) AS Fact_Signed_Amount_HBK,
               lakevalidFROMtimestamp,
               lakedeletedtimestamp
        FROM staging.Signing_Transaction_Detail
        WHERE lakeisactive = 1
              and lakedeletedtimestamp is null
              AND (
                      COALESCE(Whole_SCC, 0.00) <> 0.00
                      OR COALESCE(Whole_OCC, 0.00) <> 0.00
                      OR COALESCE(Market_SCC, 0.00) <> 0.00
                      OR COALESCE(Market_OCC, 0.00) <> 0.00
                      OR COALESCE(Line_SCC, 0.00) <> 0.00
                      OR COALESCE(Line_OCC, 0.00) <> 0.00
                  )
    ) X
) z
union all
select 'fact' as table_type,
       sum(Line_Gross_Gross_Signed_Premium_OCC) as Line_Gross_Gross_Signed_Premium_OCC,
       sum(Whole_Gross_Gross_Signed_Premium_OCC) as Whole_Gross_Gross_Signed_Premium_OCC,
       sum(Market_Gross_Net_Signed_Premium_SCC) as Market_Gross_Net_Signed_Premium_SCC,
       sum(Line_Gross_Net_Signed_Premium_SCC) as Line_Gross_Net_Signed_Premium_SCC,
       sum(Whole_Gross_Net_Signed_Premium_SCC) as Whole_Gross_Net_Signed_Premium_SCC,
       sum(Market_Gross_Net_Signed_Premium_OCC) as Market_Gross_Net_Signed_Premium_OCC,
       sum(Line_Gross_Net_Signed_Premium_OCC) as Line_Gross_Net_Signed_Premium_OCC,
       sum(Whole_Gross_Net_Signed_Premium_OCC) as Whole_Gross_Net_Signed_Premium_OCC,
       sum(Market_Gross_Signed_Acquisition_Cost_SCC) as Market_Gross_Signed_Acquisition_Cost_SCC,
       sum(Line_Gross_Signed_Acquisition_Cost_SCC) as Line_Gross_Signed_Acquisition_Cost_SCC,
       sum(Whole_Gross_Signed_Acquisition_Cost_SCC) as Whole_Gross_Signed_Acquisition_Cost_SCC,
       sum(Market_Gross_Signed_Acquisition_Cost_OCC) as Market_Gross_Signed_Acquisition_Cost_OCC,
       sum(Line_Gross_Signed_Acquisition_Cost_OCC) as Line_Gross_Signed_Acquisition_Cost_OCC,
       sum(Whole_Gross_Signed_Acquisition_Cost_OCC) as Whole_Gross_Signed_Acquisition_Cost_OCC,
       sum(Market_Gross_Signed_Profit_Commission_SCC) as Market_Gross_Signed_Profit_Commission_SCC,
       sum(Line_Gross_Signed_Profit_Commission_SCC) as Line_Gross_Signed_Profit_Commission_SCC,
       sum(Whole_Gross_Signed_Profit_Commission_SCC) as Whole_Gross_Signed_Profit_Commission_SCC,
       sum(Market_Gross_Signed_Profit_Commission_OCC) as Market_Gross_Signed_Profit_Commission_OCC,
       sum(Line_Gross_Signed_Profit_Commission_OCC) as Line_Gross_Signed_Profit_Commission_OCC,
       sum(Whole_Gross_Signed_Profit_Commission_OCC) as Whole_Gross_Signed_Profit_Commission_OCC,
       sum(Market_Gross_Signed_Reinstatement_Premium_SCC) as Market_Gross_Signed_Reinstatement_Premium_SCC,
       sum(Line_Gross_Signed_Reinstatement_Premium_SCC) as Line_Gross_Signed_Reinstatement_Premium_SCC,
       sum(Whole_Gross_Signed_Reinstatement_Premium_SCC) as Whole_Gross_Signed_Reinstatement_Premium_SCC,
       sum(Market_Gross_Signed_Reinstatement_Premium_OCC) as Market_Gross_Signed_Reinstatement_Premium_OCC,
       sum(Line_Gross_Signed_Reinstatement_Premium_OCC) as Line_Gross_Signed_Reinstatement_Premium_OCC,
       sum(Whole_Gross_Signed_Reinstatement_Premium_OCC) as Whole_Gross_Signed_Reinstatement_Premium_OCC,
       sum(Market_Gross_Paid_Claim_SCC) as Market_Gross_Paid_Claim_SCC,
       sum(Line_Gross_Paid_Claim_SCC) as Line_Gross_Paid_Claim_SCC,
       sum(Whole_Gross_Paid_Claim_SCC) as Whole_Gross_Paid_Claim_SCC,
       sum(Market_Gross_Paid_Claim_OCC) as Market_Gross_Paid_Claim_OCC,
       sum(Line_Gross_Paid_Claim_OCC) as Line_Gross_Paid_Claim_OCC,
       sum(Whole_Gross_Paid_Claim_OCC) as Whole_Gross_Paid_Claim_OCC,
       sum(Market_Gross_Paid_Claim_Loss_Fund_SCC) as Market_Gross_Paid_Claim_Loss_Fund_SCC,
       sum(Line_Gross_Paid_Claim_Loss_Fund_SCC) as Line_Gross_Paid_Claim_Loss_Fund_SCC,
       sum(Whole_Gross_Paid_Claim_Loss_Fund_SCC) as Whole_Gross_Paid_Claim_Loss_Fund_SCC,
       sum(Market_Gross_Paid_Claim_Loss_Fund_OCC) as Market_Gross_Paid_Claim_Loss_Fund_OCC,
       sum(Line_Gross_Paid_Claim_Loss_Fund_OCC) as Line_Gross_Paid_Claim_Loss_Fund_OCC,
       sum(Whole_Gross_Paid_Claim_Loss_Fund_OCC) as Whole_Gross_Paid_Claim_Loss_Fund_OCC,
       sum(Market_Gross_Signed_Overseas_Tax_SCC) as Market_Gross_Signed_Overseas_Tax_SCC,
       sum(Line_Gross_Signed_Overseas_Tax_SCC) as Line_Gross_Signed_Overseas_Tax_SCC,
       sum(Whole_Gross_Signed_Overseas_Tax_SCC) as Whole_Gross_Signed_Overseas_Tax_SCC,
       sum(Market_Gross_Signed_Overseas_Tax_OCC) as Market_Gross_Signed_Overseas_Tax_OCC,
       sum(Line_Gross_Signed_Overseas_Tax_OCC) as Line_Gross_Signed_Overseas_Tax_OCC,
       sum(Whole_Gross_Signed_Overseas_Tax_OCC) as Whole_Gross_Signed_Overseas_Tax_OCC
from dwh.fact_signed_amount
"""

pd_dataframe_fact_signed_amount = pd.read_sql_query(pd_signed_amt, connDw)

df_fact_signed_amount = spark.createDataFrame(pd_dataframe_fact_signed_amount)
df_fact_signed_amount_count = df_fact_signed_amount.drop('table_type').dropDuplicates().count()
if(df_fact_signed_amount_count >1):
    print('Output : Metric check is failed for Fact Signed Amount ')
    display(df_fact_signed_amount)
else:
    print('Output : Metric check is passed for Fact Signed Amount !!')


# COMMAND ----------

# DBTITLE 1,Metric Check (Fact Estimated Premium) for Phase A Fact - Should get no records in output
pd_estimated_premium = """
SELECT 'Staging' AS table_type,
       SUM(CAST(x.Whole_Gross_Net_Estimated_Premium_SCC AS DECIMAL(28, 2))) AS Whole_Gross_Net_Estimated_Premium_SCC,
       SUM(CAST(x.Whole_Gross_Net_Estimated_Premium_OCC AS DECIMAL(28, 2))) AS Whole_Gross_Net_Estimated_Premium_OCC,
       SUM(CAST(x.Market_Gross_Net_Estimated_Premium_SCC AS DECIMAL(28, 2))) AS Market_Gross_Net_Estimated_Premium_SCC,
       SUM(CAST(x.Market_Gross_Net_Estimated_Premium_OCC AS DECIMAL(28, 2))) AS Market_Gross_Net_Estimated_Premium_OCC,
       SUM(CAST(x.Line_Gross_Net_Estimated_Premium_SCC AS DECIMAL(28, 2))) AS Line_Gross_Net_Estimated_Premium_SCC,
       SUM(CAST(x.Line_Gross_Net_Estimated_Premium_OCC AS DECIMAL(28, 2))) AS Line_Gross_Net_Estimated_Premium_OCC,
       SUM(CAST(x.Whole_Gross_Gross_Estimated_Premium_SCC AS DECIMAL(28, 2))) AS Whole_Gross_Gross_Estimated_Premium_SCC,
       SUM(CAST(x.Whole_Gross_Gross_Estimated_Premium_OCC AS DECIMAL(28, 2))) AS Whole_Gross_Gross_Estimated_Premium_OCC,
       SUM(CAST(x.Market_Gross_Gross_Estimated_Premium_SCC AS DECIMAL(28, 2))) AS Market_Gross_Gross_Estimated_Premium_SCC,
       SUM(CAST(x.Market_Gross_Gross_Estimated_Premium_OCC AS DECIMAL(28, 2))) AS Market_Gross_Gross_Estimated_Premium_OCC,
       SUM(CAST(x.Line_Gross_Gross_Estimated_Premium_SCC AS DECIMAL(28, 2))) AS Line_Gross_Gross_Estimated_Premium_SCC,
       SUM(CAST(x.Line_Gross_Gross_Estimated_Premium_OCC AS DECIMAL(28, 2))) AS Line_Gross_Gross_Estimated_Premium_OCC,
       SUM(CAST(x.Whole_Gross_Estimated_Acquisition_cost_SCC AS DECIMAL(28, 2))) AS Whole_Gross_Estimated_Acquisition_cost_SCC,
       SUM(CAST(x.Whole_Gross_Estimated_Acquisition_cost_OCC AS DECIMAL(28, 2))) AS Whole_Gross_Estimated_Acquisition_cost_OCC,
       SUM(CAST(x.Market_Gross_Estimated_Acquisition_cost_SCC AS DECIMAL(28, 2))) AS Market_Gross_Estimated_Acquisition_cost_SCC,
       SUM(CAST(x.Market_Gross_Estimated_Acquisition_cost_OCC AS DECIMAL(28, 2))) AS Market_Gross_Estimated_Acquisition_cost_OCC,
       SUM(CAST(x.Line_Gross_Estimated_Acquisition_cost_SCC AS DECIMAL(28, 2))) AS Line_Gross_Estimated_Acquisition_cost_SCC,
       SUM(CAST(x.Line_Gross_Estimated_Acquisition_cost_OCC AS DECIMAL(28, 2))) AS Line_Gross_Estimated_Acquisition_cost_OCC
FROM
(
    SELECT epi_trans_dtl.Whole_Gross_Net_Estimated_Premium_SCC,
           epi_trans_dtl.Whole_Gross_Net_Estimated_Premium_OCC,
           epi_trans_dtl.Market_Gross_Net_Estimated_Premium_SCC,
           epi_trans_dtl.Market_Gross_Net_Estimated_Premium_OCC,
           epi_trans_dtl.Line_Gross_Net_Estimated_Premium_SCC,
           epi_trans_dtl.Line_Gross_Net_Estimated_Premium_OCC,
           epi_trans_dtl.Whole_Gross_Gross_Estimated_Premium_SCC,
           epi_trans_dtl.Whole_Gross_Gross_Estimated_Premium_OCC,
           epi_trans_dtl.Market_Gross_Gross_Estimated_Premium_SCC,
           epi_trans_dtl.Market_Gross_Gross_Estimated_Premium_OCC,
           epi_trans_dtl.Line_Gross_Gross_Estimated_Premium_SCC,
           epi_trans_dtl.Line_Gross_Gross_Estimated_Premium_OCC,
           epi_trans_dtl.Whole_Gross_Estimated_Acquisition_cost_SCC,
           epi_trans_dtl.Whole_Gross_Estimated_Acquisition_cost_OCC,
           epi_trans_dtl.Market_Gross_Estimated_Acquisition_cost_SCC,
           epi_trans_dtl.Market_Gross_Estimated_Acquisition_cost_OCC,
           epi_trans_dtl.Line_Gross_Estimated_Acquisition_cost_SCC,
           epi_trans_dtl.Line_Gross_Estimated_Acquisition_cost_OCC,
           --epi_trans_dtl.EPI_Transaction_HBK,
           epi_trans_dtl.lakeValidFromTimestamp,
           epi_trans_dtl.lakeDeletedTimestamp
    FROM
    (
        SELECT CASE
                   WHEN etd.Transaction_Line_Code = 'GN' THEN
                       etd.Whole_SCC
                   ELSE
                       0.00000000
               END AS Whole_Gross_Net_Estimated_Premium_SCC,
               CASE
                   WHEN etd.Transaction_Line_Code = 'GN' THEN
                       etd.Whole_OCC
                   ELSE
                       0.00000000
               END AS Whole_Gross_Net_Estimated_Premium_OCC,
               CASE
                   WHEN etd.Transaction_Line_Code = 'GN' THEN
                       etd.Market_SCC
                   ELSE
                       0.00000000
               END AS Market_Gross_Net_Estimated_Premium_SCC,
               CASE
                   WHEN etd.Transaction_Line_Code = 'GN' THEN
                       etd.Market_OCC
                   ELSE
                       0.00000000
               END AS Market_Gross_Net_Estimated_Premium_OCC,
               CASE
                   WHEN etd.Transaction_Line_Code = 'GN' THEN
                       etd.Line_SCC
                   ELSE
                       0.00000000
               END AS Line_Gross_Net_Estimated_Premium_SCC,
               CASE
                   WHEN etd.Transaction_Line_Code = 'GN' THEN
                       etd.Line_OCC
                   ELSE
                       0.00000000
               END AS Line_Gross_Net_Estimated_Premium_OCC,
               CASE
                   WHEN etd.Transaction_Line_Code = 'GG' THEN
                       etd.Whole_SCC
                   ELSE
                       0.00000000
               END AS Whole_Gross_Gross_Estimated_Premium_SCC,
               CASE
                   WHEN etd.Transaction_Line_Code = 'GG' THEN
                       etd.Whole_OCC
                   ELSE
                       0.00000000
               END AS Whole_Gross_Gross_Estimated_Premium_OCC,
               CASE
                   WHEN etd.Transaction_Line_Code = 'GG' THEN
                       etd.Market_SCC
                   ELSE
                       0.00000000
               END AS Market_Gross_Gross_Estimated_Premium_SCC,
               CASE
                   WHEN etd.Transaction_Line_Code = 'GG' THEN
                       etd.Market_OCC
                   ELSE
                       0.00000000
               END AS Market_Gross_Gross_Estimated_Premium_OCC,
               CASE
                   WHEN etd.Transaction_Line_Code = 'GG' THEN
                       etd.Line_SCC
                   ELSE
                       0.00000000
               END AS Line_Gross_Gross_Estimated_Premium_SCC,
               CASE
                   WHEN etd.Transaction_Line_Code = 'GG' THEN
                       etd.Line_OCC
                   ELSE
                       0.00000000
               END AS Line_Gross_Gross_Estimated_Premium_OCC,
               CASE
                   WHEN etd.Transaction_Line_Code = 'ACQ' THEN
                       etd.Whole_SCC
                   ELSE
                       0.00000000
               END AS Whole_Gross_Estimated_Acquisition_cost_SCC,
               CASE
                   WHEN etd.Transaction_Line_Code = 'ACQ' THEN
                       etd.Whole_OCC
                   ELSE
                       0.00000000
               END AS Whole_Gross_Estimated_Acquisition_cost_OCC,
               CASE
                   WHEN etd.Transaction_Line_Code = 'ACQ' THEN
                       etd.Market_SCC
                   ELSE
                       0.00000000
               END AS Market_Gross_Estimated_Acquisition_cost_SCC,
               CASE
                   WHEN etd.Transaction_Line_Code = 'ACQ' THEN
                       etd.Market_OCC
                   ELSE
                       0.00000000
               END AS Market_Gross_Estimated_Acquisition_cost_OCC,
               CASE
                   WHEN etd.Transaction_Line_Code = 'ACQ' THEN
                       etd.Line_SCC
                   ELSE
                       0.00000000
               END AS Line_Gross_Estimated_Acquisition_cost_SCC,
               CASE
                   WHEN etd.Transaction_Line_Code = 'ACQ' THEN
                       etd.Line_OCC
                   ELSE
                       0.00000000
               END AS Line_Gross_Estimated_Acquisition_cost_OCC,
               etd.lakeValidFromTimestamp,
               etd.lakeDeletedTimestamp
        FROM Staging.EPI_Transaction_Detail etd
        WHERE lakeisactive = 1
              and lakedeletedtimestamp is null
              AND (
                      COALESCE(Whole_SCC, 0.00) <> 0.00
                      OR COALESCE(Whole_OCC, 0.00) <> 0.00
                      OR COALESCE(Market_SCC, 0.00) <> 0.00
                      OR COALESCE(Market_OCC, 0.00) <> 0.00
                      OR COALESCE(Line_SCC, 0.00) <> 0.00
                      OR COALESCE(Line_OCC, 0.00) <> 0.00
                  )
    ) epi_trans_dtl
) x
UNION ALL
SELECT 'Fact' AS table_type,
       SUM(cast(fep.Whole_Gross_Net_Estimated_Premium_SCC as DECIMAL(28, 2))) AS Whole_Gross_Net_Estimated_Premium_SCC,
       SUM(cast(fep.Whole_Gross_Net_Estimated_Premium_OCC as DECIMAL(28, 2))) AS Whole_Gross_Net_Estimated_Premium_OCC,
       SUM(cast(fep.Market_Gross_Net_Estimated_Premium_SCC as DECIMAL(28, 2))) AS Market_Gross_Net_Estimated_Premium_SCC,
       SUM(cast(fep.Market_Gross_Net_Estimated_Premium_OCC as DECIMAL(28, 2))) AS Market_Gross_Net_Estimated_Premium_OCC,
       SUM(cast(fep.Line_Gross_Net_Estimated_Premium_SCC as DECIMAL(28, 2))) AS Line_Gross_Net_Estimated_Premium_SCC,
       SUM(cast(fep.Line_Gross_Net_Estimated_Premium_OCC as DECIMAL(28, 2))) AS Line_Gross_Net_Estimated_Premium_OCC,
       SUM(cast(fep.Whole_Gross_Gross_Estimated_Premium_SCC as DECIMAL(28, 2))) AS Whole_Gross_Gross_Estimated_Premium_SCC,
       SUM(cast(fep.Whole_Gross_Gross_Estimated_Premium_OCC as DECIMAL(28, 2))) AS Whole_Gross_Gross_Estimated_Premium_OCC,
       SUM(cast(fep.Market_Gross_Gross_Estimated_Premium_SCC as DECIMAL(28, 2))) AS Market_Gross_Gross_Estimated_Premium_SCC,
       SUM(cast(fep.Market_Gross_Gross_Estimated_Premium_OCC as DECIMAL(28, 2))) AS Market_Gross_Gross_Estimated_Premium_OCC,
       SUM(cast(fep.Line_Gross_Gross_Estimated_Premium_SCC as DECIMAL(28, 2))) AS Line_Gross_Gross_Estimated_Premium_SCC,
       SUM(cast(fep.Line_Gross_Gross_Estimated_Premium_OCC as DECIMAL(28, 2))) AS Line_Gross_Gross_Estimated_Premium_OCC,
       SUM(cast(fep.Whole_Gross_Estimated_Acquisition_cost_SCC as DECIMAL(28, 2))) AS Whole_Gross_Estimated_Acquisition_cost_SCC,
       SUM(cast(fep.Whole_Gross_Estimated_Acquisition_cost_OCC as DECIMAL(28, 2))) AS Whole_Gross_Estimated_Acquisition_cost_OCC,
       SUM(cast(fep.Market_Gross_Estimated_Acquisition_cost_SCC as DECIMAL(28, 2))) AS Market_Gross_Estimated_Acquisition_cost_SCC,
       SUM(cast(fep.Market_Gross_Estimated_Acquisition_cost_OCC as DECIMAL(28, 2))) AS Market_Gross_Estimated_Acquisition_cost_OCC,
       SUM(cast(fep.Line_Gross_Estimated_Acquisition_cost_SCC as DECIMAL(28, 2))) AS Line_Gross_Estimated_Acquisition_cost_SCC,
       SUM(cast(fep.Line_Gross_Estimated_Acquisition_cost_OCC as DECIMAL(28, 2))) AS Line_Gross_Estimated_Acquisition_cost_OCC
FROM dwh.Fact_Estimated_Premium fep

"""

pd_dataframe_fact_estimated_premium = pd.read_sql_query(pd_estimated_premium, connDw)

df_fact_estimated_premium = spark.createDataFrame(pd_dataframe_fact_estimated_premium)
df_fact_estimated_premium_count = df_fact_estimated_premium.drop('table_type').dropDuplicates().count()
if(df_fact_estimated_premium_count >1):
    print('Output : Metric check is failed for Fact Estimated Premium ')
    display(df_fact_estimated_premium)
else:
    print('Output : Metric check is passed for Fact Estimated Premium !!')


# COMMAND ----------

# DBTITLE 1,Metric Check (Fact_Exchange_Rate) for Phase A Fact - Should get no records in output
pd_exchange_rate_chk = """
SELECT 'STG' AS table_type,
       sum(Cast(Division_Rate_Of_Exchange AS DECIMAL(28, 18))) as Division_Exchange_Rate,
       sum(Cast(Multiplication_Rate_Of_Exchange AS DECIMAL(28, 18))) as Multiplication_Exchange_Rate
FROM Staging.Exchange_Rate
WHERE lakeisactive = 1
      AND lakedeletedtimestamp is null
      AND (
              COALESCE(Division_Rate_Of_Exchange, 0.00) <> 0.00
              OR COALESCE(Multiplication_Rate_Of_Exchange, 0.00) <> 0.00
          )
      AND From_Currency_Code IN (
                                    SELECT distinct
                                        Currency_Code
                                    FROM Staging.Currency
                                    WHERE lakeisactive = 1
                                          AND lakedeletedtimestamp is null
                                )
      AND To_Currency_Code IN (
                                  SELECT distinct
                                      Currency_Code
                                  FROM Staging.Currency
                                  WHERE lakeisactive = 1
                                        AND lakedeletedtimestamp is null
                              )
UNION ALL
select 'FACT' AS table_type,
       sum(Division_Exchange_Rate),
       sum(Multiplication_Exchange_Rate)
from dwh.fact_Exchange_Rate
where dwh_ACTION_TYPE != 'D'
"""

pd_dataframe_fact_exchange_rate = pd.read_sql_query(pd_exchange_rate_chk, connDw)

pd_fact_exchange_rate = spark.createDataFrame(pd_dataframe_fact_exchange_rate)
pd_fact_exchange_rate_count = pd_fact_exchange_rate.drop('table_type').dropDuplicates().count()
if(pd_fact_exchange_rate_count >1):
    print('Output : Metric check is failed for Fact Exchange Rate ')
    display(pd_fact_exchange_rate)
else:
    print('Output : Metric check is passed for Fact Exchange Rate !!')


# COMMAND ----------

# DBTITLE 1,Distinct Check (Fact Claim) for Phase A Fact - Should get no records in output
pd_dstnct_chk_fct_claim = """ Select *
from(select 'Fact_Claim_HBK' as col, count(distinct Fact_Claim_HBK) as cou
     from dwh.fact_claim
     having count(distinct Fact_Claim_HBK)=1
     union all
     select 'Party_ID_Underwriter' as col, count(distinct Party_ID_Underwriter) as cou
     from dwh.fact_claim
     having count(distinct Party_ID_Underwriter)=1
     union all
     select 'Party_Original_ID_Underwriter' as col, count(distinct Party_Original_ID_Underwriter) as cou
     from dwh.fact_claim
     having count(distinct Party_Original_ID_Underwriter)=1
     union all
     select 'Party_ID_Claim_Handler' as col, count(distinct Party_ID_Claim_Handler) as cou
     from dwh.fact_claim
     having count(distinct Party_ID_Claim_Handler)=1
     union all
     select 'Party_Original_ID_Claim_Handler' as col, count(distinct Party_Original_ID_Claim_Handler) as cou
     from dwh.fact_claim
     having count(distinct Party_Original_ID_Claim_Handler)=1
     union all
     select 'Party_ID_Claim_Insured' as col, count(distinct Party_ID_Claim_Insured) as cou
     from dwh.fact_claim
     having count(distinct Party_ID_Claim_Insured)=1
     union all
     select 'Party_Original_ID_Claim_Insured' as col, count(distinct Party_Original_ID_Claim_Insured) as cou
     from dwh.fact_claim
     having count(distinct Party_Original_ID_Claim_Insured)=1
     union all
     select 'Party_ID_Broker' as col, count(distinct Party_ID_Broker) as cou
     from dwh.fact_claim
     having count(distinct Party_ID_Broker)=1
     union all
     select 'Party_Original_ID_Broker' as col, count(distinct Party_Original_ID_Broker) as cou
     from dwh.fact_claim
     having count(distinct Party_Original_ID_Broker)=1
     union all
     select 'Party_ID_Insured' as col, count(distinct Party_ID_Insured) as cou
     from dwh.fact_claim
     having count(distinct Party_ID_Insured)=1
     union all
     select 'Party_Original_ID_Insured' as col, count(distinct Party_Original_ID_Insured) as cou
     from dwh.fact_claim
     having count(distinct Party_Original_ID_Insured)=1
     union all
     select 'Party_ID_Reinsured' as col, count(distinct Party_ID_Reinsured) as cou
     from dwh.fact_claim
     having count(distinct Party_ID_Reinsured)=1
     union all
     select 'Party_Original_ID_Reinsured' as col, count(distinct Party_Original_ID_Reinsured) as cou
     from dwh.fact_claim
     having count(distinct Party_Original_ID_Reinsured)=1
     union all
     select 'Location_ID_Domicile' as col, count(distinct Location_ID_Domicile) as cou
     from dwh.fact_claim
     having count(distinct Location_ID_Domicile)=1
     union all
     select 'Location_Original_ID_Domicile' as col, count(distinct Location_Original_ID_Domicile) as cou
     from dwh.fact_claim
     having count(distinct Location_Original_ID_Domicile)=1
     union all
     select 'Location_ID_Risk' as col, count(distinct Location_ID_Risk) as cou
     from dwh.fact_claim
     having count(distinct Location_ID_Risk)=1
     union all
     select 'Location_Original_ID_Risk' as col, count(distinct Location_Original_ID_Risk) as cou
     from dwh.fact_claim
     having count(distinct Location_Original_ID_Risk)=1
     union all
     select 'Location_ID_Loss' as col, count(distinct Location_ID_Loss) as cou
     from dwh.fact_claim
     having count(distinct Location_ID_Loss)=1
     union all
     select 'Location_Original_ID_Loss' as col, count(distinct Location_Original_ID_Loss) as cou
     from dwh.fact_claim
     having count(distinct Location_Original_ID_Loss)=1
     union all
     select 'Location_ID_FIL_2' as col, count(distinct Location_ID_FIL_2) as cou
     from dwh.fact_claim
     having count(distinct Location_ID_FIL_2)=1
     union all
     select 'Location_Original_ID_FIL_2' as col, count(distinct Location_Original_ID_FIL_2) as cou
     from dwh.fact_claim
     having count(distinct Location_Original_ID_FIL_2)=1
     union all
     select 'Location_ID_FIL_4' as col, count(distinct Location_ID_FIL_4) as cou
     from dwh.fact_claim
     having count(distinct Location_ID_FIL_4)=1
     union all
     select 'Location_Original_ID_FIL_4' as col, count(distinct Location_Original_ID_FIL_4) as cou
     from dwh.fact_claim
     having count(distinct Location_Original_ID_FIL_4)=1
     union all
     select 'Office_ID_Fronting' as col, count(distinct Office_ID_Fronting) as cou
     from dwh.fact_claim
     having count(distinct Office_ID_Fronting)=1
     union all
     select 'Office_Original_ID_Fronting' as col, count(distinct Office_Original_ID_Fronting) as cou
     from dwh.fact_claim
     having count(distinct Office_Original_ID_Fronting)=1
     union all
     select 'Office_ID_Underwriting' as col, count(distinct Office_ID_Underwriting) as cou
     from dwh.fact_claim
     having count(distinct Office_ID_Underwriting)=1
     union all
     select 'Office_Original_ID_Underwriting' as col, count(distinct Office_Original_ID_Underwriting) as cou
     from dwh.fact_claim
     having count(distinct Office_Original_ID_Underwriting)=1
     union all
     select 'Office_ID_Claim' as col, count(distinct Office_ID_Claim) as cou
     from dwh.fact_claim
     having count(distinct Office_ID_Claim)=1
     union all
     select 'Office_Original_ID_Claim' as col, count(distinct Office_Original_ID_Claim) as cou
     from dwh.fact_claim
     having count(distinct Office_Original_ID_Claim)=1
     union all
     select 'Business_Entity_ID' as col, count(distinct Business_Entity_ID) as cou
     from dwh.fact_claim
     having count(distinct Business_Entity_ID)=1
     union all
     select 'Business_Entity_Original_ID' as col, count(distinct Business_Entity_Original_ID) as cou
     from dwh.fact_claim
     having count(distinct Business_Entity_Original_ID)=1
     union all
     select 'Business_Entity_ID_Group' as col, count(distinct Business_Entity_ID_Group) as cou
     from dwh.fact_claim
     having count(distinct Business_Entity_ID_Group)=1
     union all
     select 'Business_Entity_Original_ID_Group' as col, count(distinct Business_Entity_Original_ID_Group) as cou
     from dwh.fact_claim
     having count(distinct Business_Entity_Original_ID_Group)=1
     union all
     select 'Business_Entity_ID_Finance_Group' as col, count(distinct Business_Entity_ID_Finance_Group) as cou
     from dwh.fact_claim
     having count(distinct Business_Entity_ID_Finance_Group)=1
     union all
     select 'Business_Entity_Original_ID_Finance_Group' as col, count(distinct Business_Entity_Original_ID_Finance_Group) as cou
     from dwh.fact_claim
     having count(distinct Business_Entity_Original_ID_Finance_Group)=1
     union all
     select 'Business_Entity_ID_Actuarial_Group' as col, count(distinct Business_Entity_ID_Actuarial_Group) as cou
     from dwh.fact_claim
     having count(distinct Business_Entity_ID_Actuarial_Group)=1
     union all
     select 'Business_Entity_Original_ID_Actuarial_Group' as col, count(distinct Business_Entity_Original_ID_Actuarial_Group) as cou
     from dwh.fact_claim
     having count(distinct Business_Entity_Original_ID_Actuarial_Group)=1
     union all
     select 'Currency_ID_Original' as col, count(distinct Currency_ID_Original) as cou
     from dwh.fact_claim
     having count(distinct Currency_ID_Original)=1
     union all
     select 'Currency_Original_ID_Original' as col, count(distinct Currency_Original_ID_Original) as cou
     from dwh.fact_claim
     having count(distinct Currency_Original_ID_Original)=1
     union all
     select 'Currency_ID_Settlement' as col, count(distinct Currency_ID_Settlement) as cou
     from dwh.fact_claim
     having count(distinct Currency_ID_Settlement)=1
     union all
     select 'Currency_Original_ID_Settlement' as col, count(distinct Currency_Original_ID_Settlement) as cou
     from dwh.fact_claim
     having count(distinct Currency_Original_ID_Settlement)=1
     union all
     select 'Policy_Header_ID' as col, count(distinct Policy_Header_ID) as cou
     from dwh.fact_claim
     having count(distinct Policy_Header_ID)=1
     union all
     select 'Policy_Header_Original_ID' as col, count(distinct Policy_Header_Original_ID) as cou
     from dwh.fact_claim
     having count(distinct Policy_Header_Original_ID)=1
     union all
     select 'Policy_Section_ID' as col, count(distinct Policy_Section_ID) as cou
     from dwh.fact_claim
     having count(distinct Policy_Section_ID)=1
     union all
     select 'Policy_Section_Original_ID' as col, count(distinct Policy_Section_Original_ID) as cou
     from dwh.fact_claim
     having count(distinct Policy_Section_Original_ID)=1
     union all
     select 'Signing_Transaction_ID' as col, count(distinct Signing_Transaction_ID) as cou
     from dwh.fact_claim
     having count(distinct Signing_Transaction_ID)=1
     union all
     select 'Signing_Transaction_Original_ID' as col, count(distinct Signing_Transaction_Original_ID) as cou
     from dwh.fact_claim
     having count(distinct Signing_Transaction_Original_ID)=1
     union all
     select 'Signing_Message_Detail_ID' as col, count(distinct Signing_Message_Detail_ID) as cou
     from dwh.fact_claim
     having count(distinct Signing_Message_Detail_ID)=1
     union all
     select 'Signing_Message_Detail_Original_ID' as col, count(distinct Signing_Message_Detail_Original_ID) as cou
     from dwh.fact_claim
     having count(distinct Signing_Message_Detail_Original_ID)=1
     union all
     select 'Claim_ID' as col, count(distinct Claim_ID) as cou
     from dwh.fact_claim
     having count(distinct Claim_ID)=1
     union all
     select 'Claim_Original_ID' as col, count(distinct Claim_Original_ID) as cou
     from dwh.fact_claim
     having count(distinct Claim_Original_ID)=1
     union all
     select 'Claim_Line_ID' as col, count(distinct Claim_Line_ID) as cou
     from dwh.fact_claim
     having count(distinct Claim_Line_ID)=1
     union all
     select 'Claim_Line_Original_ID' as col, count(distinct Claim_Line_Original_ID) as cou
     from dwh.fact_claim
     having count(distinct Claim_Line_Original_ID)=1
     union all
     select 'Claim_Message_Detail_ID' as col, count(distinct Claim_Message_Detail_ID) as cou
     from dwh.fact_claim
     having count(distinct Claim_Message_Detail_ID)=1
     union all
     select 'Claim_Message_Detail_Original_ID' as col, count(distinct Claim_Message_Detail_Original_ID) as cou
     from dwh.fact_claim
     having count(distinct Claim_Message_Detail_Original_ID)=1
     union all
     select 'Claim_Loss_Narrative_ID' as col, count(distinct Claim_Loss_Narrative_ID) as cou
     from dwh.fact_claim
     having count(distinct Claim_Loss_Narrative_ID)=1
     union all
     select 'Claim_Loss_Narrative_Original_ID' as col, count(distinct Claim_Loss_Narrative_Original_ID) as cou
     from dwh.fact_claim
     having count(distinct Claim_Loss_Narrative_Original_ID)=1
     union all
     select 'Claim_Transaction_ID' as col, count(distinct Claim_Transaction_ID) as cou
     from dwh.fact_claim
     having count(distinct Claim_Transaction_ID)=1
     union all
     select 'Claim_Transaction_Original_ID' as col, count(distinct Claim_Transaction_Original_ID) as cou
     from dwh.fact_claim
     having count(distinct Claim_Transaction_Original_ID)=1
     union all
     select 'Transaction_Type_ID' as col, count(distinct Transaction_Type_ID) as cou
     from dwh.fact_claim
     having count(distinct Transaction_Type_ID)=1
     union all
     select 'Transaction_Type_Original_ID' as col, count(distinct Transaction_Type_Original_ID) as cou
     from dwh.fact_claim
     having count(distinct Transaction_Type_Original_ID)=1
     union all
     select 'Transaction_Line_ID' as col, count(distinct Transaction_Line_ID) as cou
     from dwh.fact_claim
     having count(distinct Transaction_Line_ID)=1
     union all
     select 'Transaction_Line_Original_ID' as col, count(distinct Transaction_Line_Original_ID) as cou
     from dwh.fact_claim
     having count(distinct Transaction_Line_Original_ID)=1
     union all
     select 'Trust_Fund_ID' as col, count(distinct Trust_Fund_ID) as cou
     from dwh.fact_claim
     having count(distinct Trust_Fund_ID)=1
     union all
     select 'Trust_Fund_Original_ID' as col, count(distinct Trust_Fund_Original_ID) as cou
     from dwh.fact_claim
     having count(distinct Trust_Fund_Original_ID)=1
     union all
     select 'Coverage_ID' as col, count(distinct Coverage_ID) as cou
     from dwh.fact_claim
     having count(distinct Coverage_ID)=1
     union all
     select 'Coverage_Original_ID' as col, count(distinct Coverage_Original_ID) as cou
     from dwh.fact_claim
     having count(distinct Coverage_Original_ID)=1
     union all
     select 'Item_ID' as col, count(distinct Item_ID) as cou
     from dwh.fact_claim
     having count(distinct Item_ID)=1
     union all
     select 'Item_Original_ID' as col, count(distinct Item_Original_ID) as cou
     from dwh.fact_claim
     having count(distinct Item_Original_ID)=1
     union all
     select 'Line_ID' as col, count(distinct Line_ID) as cou
     from dwh.fact_claim
     having count(distinct Line_ID)=1
     union all
     select 'Line_Original_ID' as col, count(distinct Line_Original_ID) as cou
     from dwh.fact_claim
     having count(distinct Line_Original_ID)=1
     union all
     select 'Endorsement_ID' as col, count(distinct Endorsement_ID) as cou
     from dwh.fact_claim
     having count(distinct Endorsement_ID)=1
     union all
     select 'Endorsement_Original_ID' as col, count(distinct Endorsement_Original_ID) as cou
     from dwh.fact_claim
     having count(distinct Endorsement_Original_ID)=1
     union all
     select 'Loss_Event_ID' as col, count(distinct Loss_Event_ID) as cou
     from dwh.fact_claim
     having count(distinct Loss_Event_ID)=1
     union all
     select 'Loss_Event_Original_ID' as col, count(distinct Loss_Event_Original_ID) as cou
     from dwh.fact_claim
     having count(distinct Loss_Event_Original_ID)=1
     union all
     select 'Peril_ID' as col, count(distinct Peril_ID) as cou
     from dwh.fact_claim
     having count(distinct Peril_ID)=1
     union all
     select 'Peril_Original_ID' as col, count(distinct Peril_Original_ID) as cou
     from dwh.fact_claim
     having count(distinct Peril_Original_ID)=1
     union all
     select 'Data_Source_ID' as col, count(distinct Data_Source_ID) as cou
     from dwh.fact_claim
     having count(distinct Data_Source_ID)=1
     union all
     select 'Data_Source_Original_ID' as col, count(distinct Data_Source_Original_ID) as cou
     from dwh.fact_claim
     having count(distinct Data_Source_Original_ID)=1
     union all
     select 'FIL_2_ID' as col, count(distinct FIL_2_ID) as cou
     from dwh.fact_claim
     having count(distinct FIL_2_ID)=1
     union all
     select 'FIL_2_Original_ID' as col, count(distinct FIL_2_Original_ID) as cou
     from dwh.fact_claim
     having count(distinct FIL_2_Original_ID)=1
     union all
     select 'FIL_4_ID' as col, count(distinct FIL_4_ID) as cou
     from dwh.fact_claim
     having count(distinct FIL_4_ID)=1
     union all
     select 'FIL_4_Original_ID' as col, count(distinct FIL_4_Original_ID) as cou
     from dwh.fact_claim
     having count(distinct FIL_4_Original_ID)=1
     union all
     select 'Lloyds_Risk_Code_ID' as col, count(distinct Lloyds_Risk_Code_ID) as cou
     from dwh.fact_claim
     having count(distinct Lloyds_Risk_Code_ID)=1
     union all
     select 'Lloyds_Risk_Code_Original_ID' as col, count(distinct Lloyds_Risk_Code_Original_ID) as cou
     from dwh.fact_claim
     having count(distinct Lloyds_Risk_Code_Original_ID)=1
     union all
     select 'Lead_Type_ID' as col, count(distinct Lead_Type_ID) as cou
     from dwh.fact_claim
     having count(distinct Lead_Type_ID)=1
     union all
     select 'Business_Classification_ID' as col, count(distinct Business_Classification_ID) as cou
     from dwh.fact_claim
     having count(distinct Business_Classification_ID)=1
     union all
     select 'Business_Classification_Original_ID' as col, count(distinct Business_Classification_Original_ID) as cou
     from dwh.fact_claim
     having count(distinct Business_Classification_Original_ID)=1
     union all
     select 'Planning_Classification_ID' as col, count(distinct Planning_Classification_ID) as cou
     from dwh.fact_claim
     having count(distinct Planning_Classification_ID)=1
     union all
     select 'Planning_Classification_Original_ID' as col, count(distinct Planning_Classification_Original_ID) as cou
     from dwh.fact_claim
     having count(distinct Planning_Classification_Original_ID)=1
     union all
     select 'Date_ID_Inception' as col, count(distinct Date_ID_Inception) as cou
     from dwh.fact_claim
     having count(distinct Date_ID_Inception)=1
     union all
     select 'Date_ID_Source_Transaction' as col, count(distinct Date_ID_Source_Transaction) as cou
     from dwh.fact_claim
     having count(distinct Date_ID_Source_Transaction)=1
     union all
     select 'Date_ID_Expiry' as col, count(distinct Date_ID_Expiry) as cou
     from dwh.fact_claim
     having count(distinct Date_ID_Expiry)=1
     union all
     select 'Date_ID_Accounting' as col, count(distinct Date_ID_Accounting) as cou
     from dwh.fact_claim
     having count(distinct Date_ID_Accounting)=1
     union all
     select 'Accounting_Period_ID' as col, count(distinct Accounting_Period_ID) as cou
     from dwh.fact_claim
     having count(distinct Accounting_Period_ID)=1
     union all
     select 'Accounting_Period_ID_Message' as col, count(distinct Accounting_Period_ID_Message) as cou
     from dwh.fact_claim
     having count(distinct Accounting_Period_ID_Message)=1
     union all
     select 'Accounting_Period_ID_Source' as col, count(distinct Accounting_Period_ID_Source) as cou
     from dwh.fact_claim
     having count(distinct Accounting_Period_ID_Source)=1
     union all
     select 'Development_Period_ID_Accident_Year' as col, count(distinct Development_Period_ID_Accident_Year) as cou
     from dwh.fact_claim
     having count(distinct Development_Period_ID_Accident_Year)=1
     union all
     select 'Development_Period_ID_Year_Of_Account' as col, count(distinct Development_Period_ID_Year_Of_Account) as cou
     from dwh.fact_claim
     having count(distinct Development_Period_ID_Year_Of_Account)=1
     union all
     select 'Development_Period_ID_Inception_Date' as col, count(distinct Development_Period_ID_Inception_Date) as cou
     from dwh.fact_claim
     having count(distinct Development_Period_ID_Inception_Date)=1
     union all
     select 'Year_ID_Accident' as col, count(distinct Year_ID_Accident) as cou
     from dwh.fact_claim
     having count(distinct Year_ID_Accident)=1
     union all
     select 'Year_ID_Year_Of_Account' as col, count(distinct Year_ID_Year_Of_Account) as cou
     from dwh.fact_claim
     having count(distinct Year_ID_Year_Of_Account)=1
     union all
     select 'Business_Process_Duplicate_Claim_Indicator' as col, count(distinct Business_Process_Duplicate_Claim_Indicator) as cou
     from dwh.fact_claim
     having count(distinct Business_Process_Duplicate_Claim_Indicator)=1
     union all
     select 'Legacy_Business_Entity_Indicator' as col, count(distinct Legacy_Business_Entity_Indicator) as cou
     from dwh.fact_claim
     having count(distinct Legacy_Business_Entity_Indicator)=1
     union all
     select 'Market_Gross_Paid_Claim_Indemnity_SCC' as col, count(distinct Market_Gross_Paid_Claim_Indemnity_SCC) as cou
     from dwh.fact_claim
     having count(distinct Market_Gross_Paid_Claim_Indemnity_SCC)=1
     union all
     select 'Line_Gross_Paid_Claim_Indemnity_SCC' as col, count(distinct Line_Gross_Paid_Claim_Indemnity_SCC) as cou
     from dwh.fact_claim
     having count(distinct Line_Gross_Paid_Claim_Indemnity_SCC)=1
     union all
     select 'Whole_Gross_Paid_Claim_Indemnity_SCC' as col, count(distinct Whole_Gross_Paid_Claim_Indemnity_SCC) as cou
     from dwh.fact_claim
     having count(distinct Whole_Gross_Paid_Claim_Indemnity_SCC)=1
     union all
     select 'Market_Gross_Paid_Claim_Indemnity_OCC' as col, count(distinct Market_Gross_Paid_Claim_Indemnity_OCC) as cou
     from dwh.fact_claim
     having count(distinct Market_Gross_Paid_Claim_Indemnity_OCC)=1
     union all
     select 'Line_Gross_Paid_Claim_Indemnity_OCC' as col, count(distinct Line_Gross_Paid_Claim_Indemnity_OCC) as cou
     from dwh.fact_claim
     having count(distinct Line_Gross_Paid_Claim_Indemnity_OCC)=1
     union all
     select 'Whole_Gross_Paid_Claim_Indemnity_OCC' as col, count(distinct Whole_Gross_Paid_Claim_Indemnity_OCC) as cou
     from dwh.fact_claim
     having count(distinct Whole_Gross_Paid_Claim_Indemnity_OCC)=1
     union all
     select 'Market_Gross_Paid_Claim_Fee_SCC' as col, count(distinct Market_Gross_Paid_Claim_Fee_SCC) as cou
     from dwh.fact_claim
     having count(distinct Market_Gross_Paid_Claim_Fee_SCC)=1
     union all
     select 'Line_Gross_Paid_Claim_Fee_SCC' as col, count(distinct Line_Gross_Paid_Claim_Fee_SCC) as cou
     from dwh.fact_claim
     having count(distinct Line_Gross_Paid_Claim_Fee_SCC)=1
     union all
     select 'Whole_Gross_Paid_Claim_Fee_SCC' as col, count(distinct Whole_Gross_Paid_Claim_Fee_SCC) as cou
     from dwh.fact_claim
     having count(distinct Whole_Gross_Paid_Claim_Fee_SCC)=1
     union all
     select 'Market_Gross_Paid_Claim_Fee_OCC' as col, count(distinct Market_Gross_Paid_Claim_Fee_OCC) as cou
     from dwh.fact_claim
     having count(distinct Market_Gross_Paid_Claim_Fee_OCC)=1
     union all
     select 'Line_Gross_Paid_Claim_Fee_OCC' as col, count(distinct Line_Gross_Paid_Claim_Fee_OCC) as cou
     from dwh.fact_claim
     having count(distinct Line_Gross_Paid_Claim_Fee_OCC)=1
     union all
     select 'Whole_Gross_Paid_Claim_Fee_OCC' as col, count(distinct Whole_Gross_Paid_Claim_Fee_OCC) as cou
     from dwh.fact_claim
     having count(distinct Whole_Gross_Paid_Claim_Fee_OCC)=1
     union all
     select 'Market_Gross_Paid_Claim_SCC' as col, count(distinct Market_Gross_Paid_Claim_SCC) as cou
     from dwh.fact_claim
     having count(distinct Market_Gross_Paid_Claim_SCC)=1
     union all
     select 'Line_Gross_Paid_Claim_SCC' as col, count(distinct Line_Gross_Paid_Claim_SCC) as cou
     from dwh.fact_claim
     having count(distinct Line_Gross_Paid_Claim_SCC)=1
     union all
     select 'Whole_Gross_Paid_Claim_SCC' as col, count(distinct Whole_Gross_Paid_Claim_SCC) as cou
     from dwh.fact_claim
     having count(distinct Whole_Gross_Paid_Claim_SCC)=1
     union all
     select 'Market_Gross_Paid_Claim_OCC' as col, count(distinct Market_Gross_Paid_Claim_OCC) as cou
     from dwh.fact_claim
     having count(distinct Market_Gross_Paid_Claim_OCC)=1
     union all
     select 'Line_Gross_Paid_Claim_OCC' as col, count(distinct Line_Gross_Paid_Claim_OCC) as cou
     from dwh.fact_claim
     having count(distinct Line_Gross_Paid_Claim_OCC)=1
     union all
     select 'Whole_Gross_Paid_Claim_OCC' as col, count(distinct Whole_Gross_Paid_Claim_OCC) as cou
     from dwh.fact_claim
     having count(distinct Whole_Gross_Paid_Claim_OCC)=1
     union all
     select 'Market_Gross_Outstanding_Claim_Indemnity_SCC' as col, count(distinct Market_Gross_Outstanding_Claim_Indemnity_SCC) as cou
     from dwh.fact_claim
     having count(distinct Market_Gross_Outstanding_Claim_Indemnity_SCC)=1
     union all
     select 'Line_Gross_Outstanding_Claim_Indemnity_SCC' as col, count(distinct Line_Gross_Outstanding_Claim_Indemnity_SCC) as cou
     from dwh.fact_claim
     having count(distinct Line_Gross_Outstanding_Claim_Indemnity_SCC)=1
     union all
     select 'Whole_Gross_Outstanding_Claim_Indemnity_SCC' as col, count(distinct Whole_Gross_Outstanding_Claim_Indemnity_SCC) as cou
     from dwh.fact_claim
     having count(distinct Whole_Gross_Outstanding_Claim_Indemnity_SCC)=1
     union all
     select 'Market_Gross_Outstanding_Claim_Indemnity_OCC' as col, count(distinct Market_Gross_Outstanding_Claim_Indemnity_OCC) as cou
     from dwh.fact_claim
     having count(distinct Market_Gross_Outstanding_Claim_Indemnity_OCC)=1
     union all
     select 'Line_Gross_Outstanding_Claim_Indemnity_OCC' as col, count(distinct Line_Gross_Outstanding_Claim_Indemnity_OCC) as cou
     from dwh.fact_claim
     having count(distinct Line_Gross_Outstanding_Claim_Indemnity_OCC)=1
     union all
     select 'Whole_Gross_Outstanding_Claim_Indemnity_OCC' as col, count(distinct Whole_Gross_Outstanding_Claim_Indemnity_OCC) as cou
     from dwh.fact_claim
     having count(distinct Whole_Gross_Outstanding_Claim_Indemnity_OCC)=1
     union all
     select 'Market_Gross_Outstanding_Claim_Fee_SCC' as col, count(distinct Market_Gross_Outstanding_Claim_Fee_SCC) as cou
     from dwh.fact_claim
     having count(distinct Market_Gross_Outstanding_Claim_Fee_SCC)=1
     union all
     select 'Line_Gross_Outstanding_Claim_Fee_SCC' as col, count(distinct Line_Gross_Outstanding_Claim_Fee_SCC) as cou
     from dwh.fact_claim
     having count(distinct Line_Gross_Outstanding_Claim_Fee_SCC)=1
     union all
     select 'Whole_Gross_Outstanding_Claim_Fee_SCC' as col, count(distinct Whole_Gross_Outstanding_Claim_Fee_SCC) as cou
     from dwh.fact_claim
     having count(distinct Whole_Gross_Outstanding_Claim_Fee_SCC)=1
     union all
     select 'Market_Gross_Outstanding_Claim_Fee_OCC' as col, count(distinct Market_Gross_Outstanding_Claim_Fee_OCC) as cou
     from dwh.fact_claim
     having count(distinct Market_Gross_Outstanding_Claim_Fee_OCC)=1
     union all
     select 'Line_Gross_Outstanding_Claim_Fee_OCC' as col, count(distinct Line_Gross_Outstanding_Claim_Fee_OCC) as cou
     from dwh.fact_claim
     having count(distinct Line_Gross_Outstanding_Claim_Fee_OCC)=1
     union all
     select 'Whole_Gross_Outstanding_Claim_Fee_OCC' as col, count(distinct Whole_Gross_Outstanding_Claim_Fee_OCC) as cou
     from dwh.fact_claim
     having count(distinct Whole_Gross_Outstanding_Claim_Fee_OCC)=1
     union all
     select 'Market_Gross_Outstanding_Claim_SCC' as col, count(distinct Market_Gross_Outstanding_Claim_SCC) as cou
     from dwh.fact_claim
     having count(distinct Market_Gross_Outstanding_Claim_SCC)=1
     union all
     select 'Line_Gross_Outstanding_Claim_SCC' as col, count(distinct Line_Gross_Outstanding_Claim_SCC) as cou
     from dwh.fact_claim
     having count(distinct Line_Gross_Outstanding_Claim_SCC)=1
     union all
     select 'Whole_Gross_Outstanding_Claim_SCC' as col, count(distinct Whole_Gross_Outstanding_Claim_SCC) as cou
     from dwh.fact_claim
     having count(distinct Whole_Gross_Outstanding_Claim_SCC)=1
     union all
     select 'Market_Gross_Outstanding_Claim_OCC' as col, count(distinct Market_Gross_Outstanding_Claim_OCC) as cou
     from dwh.fact_claim
     having count(distinct Market_Gross_Outstanding_Claim_OCC)=1
     union all
     select 'Line_Gross_Outstanding_Claim_OCC' as col, count(distinct Line_Gross_Outstanding_Claim_OCC) as cou
     from dwh.fact_claim
     having count(distinct Line_Gross_Outstanding_Claim_OCC)=1
     union all
     select 'Whole_Gross_Outstanding_Claim_OCC' as col, count(distinct Whole_Gross_Outstanding_Claim_OCC) as cou
     from dwh.fact_claim
     having count(distinct Whole_Gross_Outstanding_Claim_OCC)=1
     union all
     select 'Market_Gross_Additional_Outstanding_Claim_Indemnity_SCC' as col, count(distinct Market_Gross_Additional_Outstanding_Claim_Indemnity_SCC) as cou
     from dwh.fact_claim
     having count(distinct Market_Gross_Additional_Outstanding_Claim_Indemnity_SCC)=1
     union all
     select 'Line_Gross_Additional_Outstanding_Claim_Indemnity_SCC' as col, count(distinct Line_Gross_Additional_Outstanding_Claim_Indemnity_SCC) as cou
     from dwh.fact_claim
     having count(distinct Line_Gross_Additional_Outstanding_Claim_Indemnity_SCC)=1
     union all
     select 'Whole_Gross_Additional_Outstanding_Claim_Indemnity_SCC' as col, count(distinct Whole_Gross_Additional_Outstanding_Claim_Indemnity_SCC) as cou
     from dwh.fact_claim
     having count(distinct Whole_Gross_Additional_Outstanding_Claim_Indemnity_SCC)=1
     union all
     select 'Market_Gross_Additional_Outstanding_Claim_Indemnity_OCC' as col, count(distinct Market_Gross_Additional_Outstanding_Claim_Indemnity_OCC) as cou
     from dwh.fact_claim
     having count(distinct Market_Gross_Additional_Outstanding_Claim_Indemnity_OCC)=1
     union all
     select 'Line_Gross_Additional_Outstanding_Claim_Indemnity_OCC' as col, count(distinct Line_Gross_Additional_Outstanding_Claim_Indemnity_OCC) as cou
     from dwh.fact_claim
     having count(distinct Line_Gross_Additional_Outstanding_Claim_Indemnity_OCC)=1
     union all
     select 'Whole_Gross_Additional_Outstanding_Claim_Indemnity_OCC' as col, count(distinct Whole_Gross_Additional_Outstanding_Claim_Indemnity_OCC) as cou
     from dwh.fact_claim
     having count(distinct Whole_Gross_Additional_Outstanding_Claim_Indemnity_OCC)=1
     union all
     select 'Market_Gross_Additional_Outstanding_Claim_Fee_SCC' as col, count(distinct Market_Gross_Additional_Outstanding_Claim_Fee_SCC) as cou
     from dwh.fact_claim
     having count(distinct Market_Gross_Additional_Outstanding_Claim_Fee_SCC)=1
     union all
     select 'Line_Gross_Additional_Outstanding_Claim_Fee_SCC' as col, count(distinct Line_Gross_Additional_Outstanding_Claim_Fee_SCC) as cou
     from dwh.fact_claim
     having count(distinct Line_Gross_Additional_Outstanding_Claim_Fee_SCC)=1
     union all
     select 'Whole_Gross_Additional_Outstanding_Claim_Fee_SCC' as col, count(distinct Whole_Gross_Additional_Outstanding_Claim_Fee_SCC) as cou
     from dwh.fact_claim
     having count(distinct Whole_Gross_Additional_Outstanding_Claim_Fee_SCC)=1
     union all
     select 'Market_Gross_Additional_Outstanding_Claim_Fee_OCC' as col, count(distinct Market_Gross_Additional_Outstanding_Claim_Fee_OCC) as cou
     from dwh.fact_claim
     having count(distinct Market_Gross_Additional_Outstanding_Claim_Fee_OCC)=1
     union all
     select 'Line_Gross_Additional_Outstanding_Claim_Fee_OCC' as col, count(distinct Line_Gross_Additional_Outstanding_Claim_Fee_OCC) as cou
     from dwh.fact_claim
     having count(distinct Line_Gross_Additional_Outstanding_Claim_Fee_OCC)=1
     union all
     select 'Whole_Gross_Additional_Outstanding_Claim_Fee_OCC' as col, count(distinct Whole_Gross_Additional_Outstanding_Claim_Fee_OCC) as cou
     from dwh.fact_claim
     having count(distinct Whole_Gross_Additional_Outstanding_Claim_Fee_OCC)=1
     union all
     select 'Market_Gross_Additional_Outstanding_Claim_SCC' as col, count(distinct Market_Gross_Additional_Outstanding_Claim_SCC) as cou
     from dwh.fact_claim
     having count(distinct Market_Gross_Additional_Outstanding_Claim_SCC)=1
     union all
     select 'Line_Gross_Additional_Outstanding_Claim_SCC' as col, count(distinct Line_Gross_Additional_Outstanding_Claim_SCC) as cou
     from dwh.fact_claim
     having count(distinct Line_Gross_Additional_Outstanding_Claim_SCC)=1
     union all
     select 'Whole_Gross_Additional_Outstanding_Claim_SCC' as col, count(distinct Whole_Gross_Additional_Outstanding_Claim_SCC) as cou
     from dwh.fact_claim
     having count(distinct Whole_Gross_Additional_Outstanding_Claim_SCC)=1
     union all
     select 'Market_Gross_Additional_Outstanding_Claim_OCC' as col, count(distinct Market_Gross_Additional_Outstanding_Claim_OCC) as cou
     from dwh.fact_claim
     having count(distinct Market_Gross_Additional_Outstanding_Claim_OCC)=1
     union all
     select 'Line_Gross_Additional_Outstanding_Claim_OCC' as col, count(distinct Line_Gross_Additional_Outstanding_Claim_OCC) as cou
     from dwh.fact_claim
     having count(distinct Line_Gross_Additional_Outstanding_Claim_OCC)=1
     union all
     select 'Whole_Gross_Additional_Outstanding_Claim_OCC' as col, count(distinct Whole_Gross_Additional_Outstanding_Claim_OCC) as cou
     from dwh.fact_claim
     having count(distinct Whole_Gross_Additional_Outstanding_Claim_OCC)=1
     union all
     select 'Market_Gross_Reported_Outstanding_Claim_SCC' as col, count(distinct Market_Gross_Reported_Outstanding_Claim_SCC) as cou
     from dwh.fact_claim
     having count(distinct Market_Gross_Reported_Outstanding_Claim_SCC)=1
     union all
     select 'Line_Gross_Reported_Outstanding_Claim_SCC' as col, count(distinct Line_Gross_Reported_Outstanding_Claim_SCC) as cou
     from dwh.fact_claim
     having count(distinct Line_Gross_Reported_Outstanding_Claim_SCC)=1
     union all
     select 'Whole_Gross_Reported_Outstanding_Claim_SCC' as col, count(distinct Whole_Gross_Reported_Outstanding_Claim_SCC) as cou
     from dwh.fact_claim
     having count(distinct Whole_Gross_Reported_Outstanding_Claim_SCC)=1
     union all
     select 'Market_Gross_Reported_Outstanding_Claim_OCC' as col, count(distinct Market_Gross_Reported_Outstanding_Claim_OCC) as cou
     from dwh.fact_claim
     having count(distinct Market_Gross_Reported_Outstanding_Claim_OCC)=1
     union all
     select 'Line_Gross_Reported_Outstanding_Claim_OCC' as col, count(distinct Line_Gross_Reported_Outstanding_Claim_OCC) as cou
     from dwh.fact_claim
     having count(distinct Line_Gross_Reported_Outstanding_Claim_OCC)=1
     union all
     select 'Whole_Gross_Reported_Outstanding_Claim_OCC' as col, count(distinct Whole_Gross_Reported_Outstanding_Claim_OCC) as cou
     from dwh.fact_claim
     having count(distinct Whole_Gross_Reported_Outstanding_Claim_OCC)=1
     union all
     select 'Market_Gross_Incurred_Claim_Indemnity_SCC' as col, count(distinct Market_Gross_Incurred_Claim_Indemnity_SCC) as cou
     from dwh.fact_claim
     having count(distinct Market_Gross_Incurred_Claim_Indemnity_SCC)=1
     union all
     select 'Line_Gross_Incurred_Claim_Indemnity_SCC' as col, count(distinct Line_Gross_Incurred_Claim_Indemnity_SCC) as cou
     from dwh.fact_claim
     having count(distinct Line_Gross_Incurred_Claim_Indemnity_SCC)=1
     union all
     select 'Whole_Gross_Incurred_Claim_Indemnity_SCC' as col, count(distinct Whole_Gross_Incurred_Claim_Indemnity_SCC) as cou
     from dwh.fact_claim
     having count(distinct Whole_Gross_Incurred_Claim_Indemnity_SCC)=1
     union all
     select 'Market_Gross_Incurred_Claim_Indemnity_OCC' as col, count(distinct Market_Gross_Incurred_Claim_Indemnity_OCC) as cou
     from dwh.fact_claim
     having count(distinct Market_Gross_Incurred_Claim_Indemnity_OCC)=1
     union all
     select 'Line_Gross_Incurred_Claim_Indemnity_OCC' as col, count(distinct Line_Gross_Incurred_Claim_Indemnity_OCC) as cou
     from dwh.fact_claim
     having count(distinct Line_Gross_Incurred_Claim_Indemnity_OCC)=1
     union all
     select 'Whole_Gross_Incurred_Claim_Indemnity_OCC' as col, count(distinct Whole_Gross_Incurred_Claim_Indemnity_OCC) as cou
     from dwh.fact_claim
     having count(distinct Whole_Gross_Incurred_Claim_Indemnity_OCC)=1
     union all
     select 'Market_Gross_Incurred_Claim_Fee_SCC' as col, count(distinct Market_Gross_Incurred_Claim_Fee_SCC) as cou
     from dwh.fact_claim
     having count(distinct Market_Gross_Incurred_Claim_Fee_SCC)=1
     union all
     select 'Line_Gross_Incurred_Claim_Fee_SCC' as col, count(distinct Line_Gross_Incurred_Claim_Fee_SCC) as cou
     from dwh.fact_claim
     having count(distinct Line_Gross_Incurred_Claim_Fee_SCC)=1
     union all
     select 'Whole_Gross_Incurred_Claim_Fee_SCC' as col, count(distinct Whole_Gross_Incurred_Claim_Fee_SCC) as cou
     from dwh.fact_claim
     having count(distinct Whole_Gross_Incurred_Claim_Fee_SCC)=1
     union all
     select 'Market_Gross_Incurred_Claim_Fee_OCC' as col, count(distinct Market_Gross_Incurred_Claim_Fee_OCC) as cou
     from dwh.fact_claim
     having count(distinct Market_Gross_Incurred_Claim_Fee_OCC)=1
     union all
     select 'Line_Gross_Incurred_Claim_Fee_OCC' as col, count(distinct Line_Gross_Incurred_Claim_Fee_OCC) as cou
     from dwh.fact_claim
     having count(distinct Line_Gross_Incurred_Claim_Fee_OCC)=1
     union all
     select 'Whole_Gross_Incurred_Claim_Fee_OCC' as col, count(distinct Whole_Gross_Incurred_Claim_Fee_OCC) as cou
     from dwh.fact_claim
     having count(distinct Whole_Gross_Incurred_Claim_Fee_OCC)=1
     union all
     select 'Market_Gross_Incurred_Claim_SCC' as col, count(distinct Market_Gross_Incurred_Claim_SCC) as cou
     from dwh.fact_claim
     having count(distinct Market_Gross_Incurred_Claim_SCC)=1
     union all
     select 'Line_Gross_Incurred_Claim_SCC' as col, count(distinct Line_Gross_Incurred_Claim_SCC) as cou
     from dwh.fact_claim
     having count(distinct Line_Gross_Incurred_Claim_SCC)=1
     union all
     select 'Whole_Gross_Incurred_Claim_SCC' as col, count(distinct Whole_Gross_Incurred_Claim_SCC) as cou
     from dwh.fact_claim
     having count(distinct Whole_Gross_Incurred_Claim_SCC)=1
     union all
     select 'Market_Gross_Incurred_Claim_OCC' as col, count(distinct Market_Gross_Incurred_Claim_OCC) as cou
     from dwh.fact_claim
     having count(distinct Market_Gross_Incurred_Claim_OCC)=1
     union all
     select 'Line_Gross_Incurred_Claim_OCC' as col, count(distinct Line_Gross_Incurred_Claim_OCC) as cou
     from dwh.fact_claim
     having count(distinct Line_Gross_Incurred_Claim_OCC)=1
     union all
     select 'Whole_Gross_Incurred_Claim_OCC' as col, count(distinct Whole_Gross_Incurred_Claim_OCC) as cou
     from dwh.fact_claim
     having count(distinct Whole_Gross_Incurred_Claim_OCC)=1
     union all
     select 'Market_Gross_Paid_Claim_Loss_Fund_SCC' as col, count(distinct Market_Gross_Paid_Claim_Loss_Fund_SCC) as cou
     from dwh.fact_claim
     having count(distinct Market_Gross_Paid_Claim_Loss_Fund_SCC)=1
     union all
     select 'Line_Gross_Paid_Claim_Loss_Fund_SCC' as col, count(distinct Line_Gross_Paid_Claim_Loss_Fund_SCC) as cou
     from dwh.fact_claim
     having count(distinct Line_Gross_Paid_Claim_Loss_Fund_SCC)=1
     union all
     select 'Whole_Gross_Paid_Claim_Loss_Fund_SCC' as col, count(distinct Whole_Gross_Paid_Claim_Loss_Fund_SCC) as cou
     from dwh.fact_claim
     having count(distinct Whole_Gross_Paid_Claim_Loss_Fund_SCC)=1
     union all
     select 'Market_Gross_Paid_Claim_Loss_Fund_OCC' as col, count(distinct Market_Gross_Paid_Claim_Loss_Fund_OCC) as cou
     from dwh.fact_claim
     having count(distinct Market_Gross_Paid_Claim_Loss_Fund_OCC)=1
     union all
     select 'Line_Gross_Paid_Claim_Loss_Fund_OCC' as col, count(distinct Line_Gross_Paid_Claim_Loss_Fund_OCC) as cou
     from dwh.fact_claim
     having count(distinct Line_Gross_Paid_Claim_Loss_Fund_OCC)=1
     union all
     select 'Whole_Gross_Paid_Claim_Loss_Fund_OCC' as col, count(distinct Whole_Gross_Paid_Claim_Loss_Fund_OCC) as cou
     from dwh.fact_claim
     having count(distinct Whole_Gross_Paid_Claim_Loss_Fund_OCC)=1)fct
where cou>1 """


pd_df_dstnct_fct_claim = pd.read_sql_query(pd_dstnct_chk_fct_claim, connDw)

if pd_df_dstnct_fct_claim.empty:
    print('Output : Distinct check is passed fro Fact Claim !!')
else:
    pd.set_option('display.max_columns', None)
    print('Output : Distinct check is failed Fact Claim')
    display(pd_df_dstnct_fct_claim)

# COMMAND ----------

# DBTITLE 1,Distinct Check (Fact Limit) for Phase A Fact - Should get no records in output
pd_dstnct_fact_limit = """ Select *
from(select 'fact_limit_hbk' as col, count(distinct fact_limit_hbk) as cou
     from dwh.fact_limit
     having count(distinct fact_limit_hbk)=1
     union all
     select 'limit_id' as col, count(distinct limit_id) as cou
     from dwh.fact_limit
     having count(distinct limit_id)=1
     union all
     select 'limit_original_id' as col, count(distinct limit_original_id) as cou
     from dwh.fact_limit
     having count(distinct limit_original_id)=1
     union all
     select 'policy_section_id' as col, count(distinct policy_section_id) as cou
     from dwh.fact_limit
     having count(distinct policy_section_id)=1
     union all
     select 'policy_section_original_id' as col, count(distinct policy_section_original_id) as cou
     from dwh.fact_limit
     having count(distinct policy_section_original_id)=1
     union all
     select 'line_id' as col, count(distinct line_id) as cou
     from dwh.fact_limit
     having count(distinct line_id)=1
     union all
     select 'line_original_id' as col, count(distinct line_original_id) as cou
     from dwh.fact_limit
     having count(distinct line_original_id)=1
     union all
     select 'coverage_id' as col, count(distinct coverage_id) as cou
     from dwh.fact_limit
     having count(distinct coverage_id)=1
     union all
     select 'coverage_original_id' as col, count(distinct coverage_original_id) as cou
     from dwh.fact_limit
     having count(distinct coverage_original_id)=1
     union all
     select 'item_id' as col, count(distinct item_id) as cou
     from dwh.fact_limit
     having count(distinct item_id)=1
     union all
     select 'item_original_id' as col, count(distinct item_original_id) as cou
     from dwh.fact_limit
     having count(distinct item_original_id)=1
     union all
     select 'date_id_inception' as col, count(distinct date_id_inception) as cou
     from dwh.fact_limit
     having count(distinct date_id_inception)=1
     union all
     select 'date_id_expiry' as col, count(distinct date_id_expiry) as cou
     from dwh.fact_limit
     having count(distinct date_id_expiry)=1
     union all
     select 'date_id_source_transaction' as col, count(distinct date_id_source_transaction) as cou
     from dwh.fact_limit
     having count(distinct date_id_source_transaction)=1
     union all
     select 'business_classification_id' as col, count(distinct business_classification_id) as cou
     from dwh.fact_limit
     having count(distinct business_classification_id)=1
     union all
     select 'business_classification_original_id' as col, count(distinct business_classification_original_id) as cou
     from dwh.fact_limit
     having count(distinct business_classification_original_id)=1
     union all
     select 'planning_classification_id' as col, count(distinct planning_classification_id) as cou
     from dwh.fact_limit
     having count(distinct planning_classification_id)=1
     union all
     select 'planning_classification_original_id' as col, count(distinct planning_classification_original_id) as cou
     from dwh.fact_limit
     having count(distinct planning_classification_original_id)=1
     union all
     select 'year_id_year_of_account' as col, count(distinct year_id_year_of_account) as cou
     from dwh.fact_limit
     having count(distinct year_id_year_of_account)=1
     union all
     select 'policy_header_id' as col, count(distinct policy_header_id) as cou
     from dwh.fact_limit
     having count(distinct policy_header_id)=1
     union all
     select 'policy_header_original_id' as col, count(distinct policy_header_original_id) as cou
     from dwh.fact_limit
     having count(distinct policy_header_original_id)=1
     union all
     select 'Data_Source_ID' as col, count(distinct Data_Source_ID) as cou
     from dwh.fact_limit
     having count(distinct Data_Source_ID)=1
     union all
     select 'Data_Source_original_id' as col, count(distinct Data_Source_original_id) as cou
     from dwh.fact_limit
     having count(distinct Data_Source_original_id)=1
     union all
     select 'business_entity_id' as col, count(distinct business_entity_id) as cou
     from dwh.fact_limit
     having count(distinct business_entity_id)=1
     union all
     select 'business_entity_original_id' as col, count(distinct business_entity_original_id) as cou
     from dwh.fact_limit
     having count(distinct business_entity_original_id)=1
     union all
     select 'office_id_underwriting' as col, count(distinct office_id_underwriting) as cou
     from dwh.fact_limit
     having count(distinct office_id_underwriting)=1
     union all
     select 'office_original_id_underwriting' as col, count(distinct office_original_id_underwriting) as cou
     from dwh.fact_limit
     having count(distinct office_original_id_underwriting)=1
     union all
     select 'office_id_fronting' as col, count(distinct office_id_fronting) as cou
     from dwh.fact_limit
     having count(distinct office_id_fronting)=1
     union all
     select 'office_original_id_fronting' as col, count(distinct office_original_id_fronting) as cou
     from dwh.fact_limit
     having count(distinct office_original_id_fronting)=1
     union all
     select 'location_id_risk' as col, count(distinct location_id_risk) as cou
     from dwh.fact_limit
     having count(distinct location_id_risk)=1
     union all
     select 'location_original_id_risk' as col, count(distinct location_original_id_risk) as cou
     from dwh.fact_limit
     having count(distinct location_original_id_risk)=1
     union all
     select 'location_id_limit' as col, count(distinct location_id_limit) as cou
     from dwh.fact_limit
     having count(distinct location_id_limit)=1
     union all
     select 'location_original_id_limit' as col, count(distinct location_original_id_limit) as cou
     from dwh.fact_limit
     having count(distinct location_original_id_limit)=1
     union all
     select 'Business_Entity_ID_Group' as col, count(distinct Business_Entity_ID_Group) as cou
     from dwh.fact_limit
     having count(distinct Business_Entity_ID_Group)=1
     union all
     select 'Business_Entity_original_id_Group' as col, count(distinct Business_Entity_original_id_Group) as cou
     from dwh.fact_limit
     having count(distinct Business_Entity_original_id_Group)=1
     union all
     select 'Business_Entity_ID_Finance_Group' as col, count(distinct Business_Entity_ID_Finance_Group) as cou
     from dwh.fact_limit
     having count(distinct Business_Entity_ID_Finance_Group)=1
     union all
     select 'Business_Entity_original_id_Finance_Group' as col, count(distinct Business_Entity_original_id_Finance_Group) as cou
     from dwh.fact_limit
     having count(distinct Business_Entity_original_id_Finance_Group)=1
     union all
     select 'Business_Entity_ID_Actuarial_Group' as col, count(distinct Business_Entity_ID_Actuarial_Group) as cou
     from dwh.fact_limit
     having count(distinct Business_Entity_ID_Actuarial_Group)=1
     union all
     select 'Business_Entity_original_id_Actuarial_Group' as col, count(distinct Business_Entity_original_id_Actuarial_Group) as cou
     from dwh.fact_limit
     having count(distinct Business_Entity_original_id_Actuarial_Group)=1
     union all
     select 'Accounting_Period_ID' as col, count(distinct Accounting_Period_ID) as cou
     from dwh.fact_limit
     having count(distinct Accounting_Period_ID)=1
     union all
     select 'currency_id_limit' as col, count(distinct currency_id_limit) as cou
     from dwh.fact_limit
     having count(distinct currency_id_limit)=1
     union all
     select 'currency_original_id_limit' as col, count(distinct currency_original_id_limit) as cou
     from dwh.fact_limit
     having count(distinct currency_original_id_limit)=1
     union all
     select 'limit_lcc' as col, count(distinct limit_lcc) as cou
     from dwh.fact_limit
     having count(distinct limit_lcc)=1
     union all
     select 'line_size_lcc' as col, count(distinct line_size_lcc) as cou
     from dwh.fact_limit
     having count(distinct line_size_lcc)=1
     union all
     select 'peril_id' as col, count(distinct peril_id) as cou
     from dwh.fact_limit
     having count(distinct peril_id)=1
     union all
     select 'peril_original_id' as col, count(distinct peril_original_id) as cou
     from dwh.fact_limit
     having count(distinct peril_original_id)=1
     union all
     select 'date_id_accounting' as col, count(distinct date_id_accounting) as cou
     from dwh.fact_limit
     having count(distinct date_id_accounting)=1
     union all
     select 'location_id_domicile' as col, count(distinct location_id_domicile) as cou
     from dwh.fact_limit
     having count(distinct location_id_domicile)=1
     union all
     select 'location_original_id_domicile' as col, count(distinct location_original_id_domicile) as cou
     from dwh.fact_limit
     having count(distinct location_original_id_domicile)=1
     union all
     select 'party_id_underwriter' as col, count(distinct party_id_underwriter) as cou
     from dwh.fact_limit
     having count(distinct party_id_underwriter)=1
     union all
     select 'party_original_id_underwriter' as col, count(distinct party_original_id_underwriter) as cou
     from dwh.fact_limit
     having count(distinct party_original_id_underwriter)=1
     union all
     select 'party_id_insured' as col, count(distinct party_id_insured) as cou
     from dwh.fact_limit
     having count(distinct party_id_insured)=1
     union all
     select 'party_original_id_insured' as col, count(distinct party_original_id_insured) as cou
     from dwh.fact_limit
     having count(distinct party_original_id_insured)=1
     union all
     select 'party_id_reinsured' as col, count(distinct party_id_reinsured) as cou
     from dwh.fact_limit
     having count(distinct party_id_reinsured)=1
     union all
     select 'party_original_id_reinsured' as col, count(distinct party_original_id_reinsured) as cou
     from dwh.fact_limit
     having count(distinct party_original_id_reinsured)=1
     union all
     select 'party_id_broker' as col, count(distinct party_id_broker) as cou
     from dwh.fact_limit
     having count(distinct party_id_broker)=1
     union all
     select 'party_original_id_broker' as col, count(distinct party_original_id_broker) as cou
     from dwh.fact_limit
     having count(distinct party_original_id_broker)=1)fct
where cou >1 """

pd_df_dstnct_fct_limit = pd.read_sql_query(pd_dstnct_fact_limit,connDw)

if pd_df_dstnct_fct_limit.empty:
    print('Output : Distinct check is passed for Fact Limit !!')
else:
    pd.set_option('display.max_columns', None)    
    print('Output : Distinct check is failed for for Fact Limit ')
    display(pd_df_dstnct_fct_limit)

# COMMAND ----------

# DBTITLE 1,Distinct Check (Fact Pricing Rating) for Phase A Fact - Should get no records in output
pd_dstnct_pricing_rating = """ Select *
from(select ' Party_ID_Underwriter ' as col, count(distinct Party_ID_Underwriter) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Party_ID_Underwriter)=1
     union all
     select ' Party_Original_ID_Underwriter ' as col, count(distinct Party_Original_ID_Underwriter) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Party_Original_ID_Underwriter)=1
     union all
     select ' Party_ID_Broker ' as col, count(distinct Party_ID_Broker) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Party_ID_Broker)=1
     union all
     select ' Party_Original_ID_Broker ' as col, count(distinct Party_Original_ID_Broker) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Party_Original_ID_Broker)=1
     union all
     select ' Party_ID_Coverholder ' as col, count(distinct Party_ID_Coverholder) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Party_ID_Coverholder)=1
     union all
     select ' Party_Original_ID_Coverholder ' as col, count(distinct Party_Original_ID_Coverholder) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Party_Original_ID_Coverholder)=1
     union all
     select ' Party_ID_Insured ' as col, count(distinct Party_ID_Insured) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Party_ID_Insured)=1
     union all
     select ' Party_Original_ID_Insured ' as col, count(distinct Party_Original_ID_Insured) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Party_Original_ID_Insured)=1
     union all
     select ' Party_ID_Reinsured ' as col, count(distinct Party_ID_Reinsured) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Party_ID_Reinsured)=1
     union all
     select ' Party_Original_ID_Reinsured ' as col, count(distinct Party_Original_ID_Reinsured) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Party_Original_ID_Reinsured)=1
     union all
     select ' Location_ID_Domicile ' as col, count(distinct Location_ID_Domicile) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Location_ID_Domicile)=1
     union all
     select ' Location_Original_ID_Domicile ' as col, count(distinct Location_Original_ID_Domicile) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Location_Original_ID_Domicile)=1
     union all
     select ' Location_ID_Risk ' as col, count(distinct Location_ID_Risk) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Location_ID_Risk)=1
     union all
     select ' Location_Original_ID_Risk ' as col, count(distinct Location_Original_ID_Risk) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Location_Original_ID_Risk)=1
     union all
     select ' Office_ID_fronting ' as col, count(distinct Office_ID_fronting) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Office_ID_fronting)=1
     union all
     select ' Office_Original_ID_fronting ' as col, count(distinct Office_Original_ID_fronting) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Office_Original_ID_fronting)=1
     union all
     select ' Office_ID_Underwriting ' as col, count(distinct Office_ID_Underwriting) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Office_ID_Underwriting)=1
     union all
     select ' Office_Original_ID_Underwriting ' as col, count(distinct Office_Original_ID_Underwriting) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Office_Original_ID_Underwriting)=1
     union all
     select ' Business_Entity_ID ' as col, count(distinct Business_Entity_ID) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Business_Entity_ID)=1
     union all
     select ' Business_Entity_Original_ID ' as col, count(distinct Business_Entity_Original_ID) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Business_Entity_Original_ID)=1
     union all
     select ' Business_Entity_ID_Group ' as col, count(distinct Business_Entity_ID_Group) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Business_Entity_ID_Group)=1
     union all
     select ' Business_Entity_Original_ID_Group ' as col, count(distinct Business_Entity_Original_ID_Group) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Business_Entity_Original_ID_Group)=1
     union all
     select ' Business_Entity_ID_Finance_Group ' as col, count(distinct Business_Entity_ID_Finance_Group) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Business_Entity_ID_Finance_Group)=1
     union all
     select ' Business_Entity_Original_ID_Finance_Group ' as col, count(distinct Business_Entity_Original_ID_Finance_Group) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Business_Entity_Original_ID_Finance_Group)=1
     union all
     select ' Business_Entity_ID_Actuarial_Group ' as col, count(distinct Business_Entity_ID_Actuarial_Group) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Business_Entity_ID_Actuarial_Group)=1
     union all
     select ' Business_Entity_Original_ID_Actuarial_Group ' as col, count(distinct Business_Entity_Original_ID_Actuarial_Group) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Business_Entity_Original_ID_Actuarial_Group)=1
     union all
     select ' Currency_ID_Original ' as col, count(distinct Currency_ID_Original) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Currency_ID_Original)=1
     union all
     select ' Currency_Original_ID_Original ' as col, count(distinct Currency_Original_ID_Original) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Currency_Original_ID_Original)=1
     union all
     select ' Currency_ID_Settlement ' as col, count(distinct Currency_ID_Settlement) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Currency_ID_Settlement)=1
     union all
     select ' Currency_Original_ID_Settlement ' as col, count(distinct Currency_Original_ID_Settlement) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Currency_Original_ID_Settlement)=1
     union all
     select ' Policy_Header_ID ' as col, count(distinct Policy_Header_ID) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Policy_Header_ID)=1
     union all
     select ' Policy_Header_Original_ID ' as col, count(distinct Policy_Header_Original_ID) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Policy_Header_Original_ID)=1
     union all
     select ' Policy_section_ID ' as col, count(distinct Policy_section_ID) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Policy_section_ID)=1
     union all
     select ' Policy_section_Original_ID ' as col, count(distinct Policy_section_Original_ID) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Policy_section_Original_ID)=1
     union all
     select ' Pricing_Rating_ID ' as col, count(distinct Pricing_Rating_ID) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Pricing_Rating_ID)=1
     union all
     select ' Pricing_Rating_Original_ID ' as col, count(distinct Pricing_Rating_Original_ID) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Pricing_Rating_Original_ID)=1
     union all
     select ' Coverage_ID ' as col, count(distinct Coverage_ID) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Coverage_ID)=1
     union all
     select ' Coverage_Original_ID ' as col, count(distinct Coverage_Original_ID) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Coverage_Original_ID)=1
     union all
     select ' Item_ID ' as col, count(distinct Item_ID) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Item_ID)=1
     union all
     select ' Item_Original_ID ' as col, count(distinct Item_Original_ID) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Item_Original_ID)=1
     union all
     select ' Line_ID ' as col, count(distinct Line_ID) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Line_ID)=1
     union all
     select ' Line_Original_ID ' as col, count(distinct Line_Original_ID) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Line_Original_ID)=1
     union all
     select ' Data_Source_ID ' as col, count(distinct Data_Source_ID) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Data_Source_ID)=1
     union all
     select ' Lloyds_Risk_Code_ID ' as col, count(distinct Lloyds_Risk_Code_ID) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Lloyds_Risk_Code_ID)=1
     union all
     select ' Lloyds_Risk_Code_Original_ID ' as col, count(distinct Lloyds_Risk_Code_Original_ID) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Lloyds_Risk_Code_Original_ID)=1
     union all
     select ' Lead_Type_ID ' as col, count(distinct Lead_Type_ID) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Lead_Type_ID)=1
     union all
     select ' Business_Classification_ID ' as col, count(distinct Business_Classification_ID) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Business_Classification_ID)=1
     union all
     select ' Business_Classification_Original_ID ' as col, count(distinct Business_Classification_Original_ID) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Business_Classification_Original_ID)=1
     union all
     select ' Planning_Classification_ID ' as col, count(distinct Planning_Classification_ID) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Planning_Classification_ID)=1
     union all
     select ' Planning_Classification_Original_ID ' as col, count(distinct Planning_Classification_Original_ID) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Planning_Classification_Original_ID)=1
     union all
     select ' Date_ID_Inception ' as col, count(distinct Date_ID_Inception) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Date_ID_Inception)=1
     union all
     select ' Date_ID_Source_Transaction ' as col, count(distinct Date_ID_Source_Transaction) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Date_ID_Source_Transaction)=1
     union all
     select ' Date_ID_Expiry ' as col, count(distinct Date_ID_Expiry) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Date_ID_Expiry)=1
     union all
     select ' Date_ID_Accounting ' as col, count(distinct Date_ID_Accounting) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Date_ID_Accounting)=1
     union all
     select ' Accounting_Period_ID ' as col, count(distinct Accounting_Period_ID) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Accounting_Period_ID)=1
     union all
     select ' Year_ID_Year_of_account ' as col, count(distinct Year_ID_Year_of_account) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct Year_ID_Year_of_account)=1
     union all
     select ' market_benchmark_price_scc ' as col, count(distinct market_benchmark_price_scc) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct market_benchmark_price_scc)=1
     union all
     select ' line_benchmark_price_scc ' as col, count(distinct line_benchmark_price_scc) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct line_benchmark_price_scc)=1
     union all
     select ' whole_benchmark_price_scc ' as col, count(distinct whole_benchmark_price_scc) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct whole_benchmark_price_scc)=1
     union all
     select ' market_benchmark_price_occ ' as col, count(distinct market_benchmark_price_occ) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct market_benchmark_price_occ)=1
     union all
     select ' line_benchmark_price_occ ' as col, count(distinct line_benchmark_price_occ) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct line_benchmark_price_occ)=1
     union all
     select ' whole_benchmark_price_occ ' as col, count(distinct whole_benchmark_price_occ) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct whole_benchmark_price_occ)=1
     union all
     select ' market_change_in_attach_Point_scc ' as col, count(distinct market_change_in_attach_Point_scc) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct market_change_in_attach_Point_scc)=1
     union all
     select ' line_change_in_attach_Point_scc ' as col, count(distinct line_change_in_attach_Point_scc) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct line_change_in_attach_Point_scc)=1
     union all
     select ' whole_change_in_attach_Point_scc ' as col, count(distinct whole_change_in_attach_Point_scc) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct whole_change_in_attach_Point_scc)=1
     union all
     select ' market_change_in_attach_Point_occ ' as col, count(distinct market_change_in_attach_Point_occ) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct market_change_in_attach_Point_occ)=1
     union all
     select ' line_change_in_attach_Point_occ ' as col, count(distinct line_change_in_attach_Point_occ) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct line_change_in_attach_Point_occ)=1
     union all
     select ' whole_change_in_attach_Point_occ ' as col, count(distinct whole_change_in_attach_Point_occ) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct whole_change_in_attach_Point_occ)=1
     union all
     select ' market_change_in_breadth_cover_scc ' as col, count(distinct market_change_in_breadth_cover_scc) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct market_change_in_breadth_cover_scc)=1
     union all
     select ' line_change_in_breadth_cover_scc ' as col, count(distinct line_change_in_breadth_cover_scc) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct line_change_in_breadth_cover_scc)=1
     union all
     select ' whole_change_in_breadth_cover_scc ' as col, count(distinct whole_change_in_breadth_cover_scc) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct whole_change_in_breadth_cover_scc)=1
     union all
     select ' market_change_in_breadth_cover_occ ' as col, count(distinct market_change_in_breadth_cover_occ) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct market_change_in_breadth_cover_occ)=1
     union all
     select ' line_change_in_breadth_cover_occ ' as col, count(distinct line_change_in_breadth_cover_occ) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct line_change_in_breadth_cover_occ)=1
     union all
     select ' whole_change_in_breadth_cover_occ ' as col, count(distinct whole_change_in_breadth_cover_occ) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct whole_change_in_breadth_cover_occ)=1
     union all
     select ' market_change_in_pure_rate_scc ' as col, count(distinct market_change_in_pure_rate_scc) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct market_change_in_pure_rate_scc)=1
     union all
     select ' line_change_in_pure_rate_scc ' as col, count(distinct line_change_in_pure_rate_scc) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct line_change_in_pure_rate_scc)=1
     union all
     select ' whole_change_in_pure_rate_scc ' as col, count(distinct whole_change_in_pure_rate_scc) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct whole_change_in_pure_rate_scc)=1
     union all
     select ' market_change_in_pure_rate_occ ' as col, count(distinct market_change_in_pure_rate_occ) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct market_change_in_pure_rate_occ)=1
     union all
     select ' line_change_in_pure_rate_occ ' as col, count(distinct line_change_in_pure_rate_occ) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct line_change_in_pure_rate_occ)=1
     union all
     select ' whole_change_in_pure_rate_occ ' as col, count(distinct whole_change_in_pure_rate_occ) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct whole_change_in_pure_rate_occ)=1
     union all
     select ' market_change_in_other_factors_scc ' as col, count(distinct market_change_in_other_factors_scc) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct market_change_in_other_factors_scc)=1
     union all
     select ' line_change_in_other_factors_scc ' as col, count(distinct line_change_in_other_factors_scc) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct line_change_in_other_factors_scc)=1
     union all
     select ' whole_change_in_other_factors_scc ' as col, count(distinct whole_change_in_other_factors_scc) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct whole_change_in_other_factors_scc)=1
     union all
     select ' market_change_in_other_factors_occ ' as col, count(distinct market_change_in_other_factors_occ) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct market_change_in_other_factors_occ)=1
     union all
     select ' line_change_in_other_factors_occ ' as col, count(distinct line_change_in_other_factors_occ) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct line_change_in_other_factors_occ)=1
     union all
     select ' whole_change_in_other_factors_occ ' as col, count(distinct whole_change_in_other_factors_occ) as cou
     from dwh.Fact_Pricing_Rating
     having count(distinct whole_change_in_other_factors_occ)=1)fct
where cou>1"""

pd_df_dstnct_pricing_rating = pd.read_sql_query(pd_dstnct_pricing_rating,connDw)

if pd_df_dstnct_pricing_rating.empty:
    print('Output : Distinct check is passed for Fact Pricing Rating !!')
else:
    pd.set_option('display.max_columns', None)    
    print('Output : Distinct check is failed for Fact Pricing Rating !! ')
    display(pd_df_dstnct_pricing_rating)

# COMMAND ----------

# DBTITLE 1,Distinct Check (Fact Signed Amount) for Phase A Fact - Should get no records in output
pd_distinct_signed_amt = """ Select *
from(select 'Fact_Signed_Amount_HBK' as col, count(distinct Fact_Signed_Amount_HBK) as cou
     from dwh.fact_signed_amount
     having count(distinct Fact_Signed_Amount_HBK)=1
     union all
     select 'Party_ID_Underwriter' as col, count(distinct Party_ID_Underwriter) as cou
     from dwh.fact_signed_amount
     having count(distinct Party_ID_Underwriter)=1
     union all
     select 'Party_original_id_Underwriter' as col, count(distinct Party_original_id_Underwriter) as cou
     from dwh.fact_signed_amount
     having count(distinct Party_original_id_Underwriter)=1
     union all
     select 'Party_ID_Broker' as col, count(distinct Party_ID_Broker) as cou
     from dwh.fact_signed_amount
     having count(distinct Party_ID_Broker)=1
     union all
     select 'Party_original_id_Broker' as col, count(distinct Party_original_id_Broker) as cou
     from dwh.fact_signed_amount
     having count(distinct Party_original_id_Broker)=1
     union all
     select 'Party_ID_Coverholder' as col, count(distinct Party_ID_Coverholder) as cou
     from dwh.fact_signed_amount
     having count(distinct Party_ID_Coverholder)=1
     union all
     select 'Party_original_id_Coverholder' as col, count(distinct Party_original_id_Coverholder) as cou
     from dwh.fact_signed_amount
     having count(distinct Party_original_id_Coverholder)=1
     union all
     select 'Party_ID_Insured' as col, count(distinct Party_ID_Insured) as cou
     from dwh.fact_signed_amount
     having count(distinct Party_ID_Insured)=1
     union all
     select 'Party_original_id_Insured' as col, count(distinct Party_original_id_Insured) as cou
     from dwh.fact_signed_amount
     having count(distinct Party_original_id_Insured)=1
     union all
     select 'Party_ID_Reinsured' as col, count(distinct Party_ID_Reinsured) as cou
     from dwh.fact_signed_amount
     having count(distinct Party_ID_Reinsured)=1
     union all
     select 'Party_original_id_Reinsured' as col, count(distinct Party_original_id_Reinsured) as cou
     from dwh.fact_signed_amount
     having count(distinct Party_original_id_Reinsured)=1
     union all
     select 'Location_ID_Domicile' as col, count(distinct Location_ID_Domicile) as cou
     from dwh.fact_signed_amount
     having count(distinct Location_ID_Domicile)=1
     union all
     select 'Location_original_id_Domicile' as col, count(distinct Location_original_id_Domicile) as cou
     from dwh.fact_signed_amount
     having count(distinct Location_original_id_Domicile)=1
     union all
     select 'Location_ID_Risk' as col, count(distinct Location_ID_Risk) as cou
     from dwh.fact_signed_amount
     having count(distinct Location_ID_Risk)=1
     union all
     select 'Location_original_id_Risk' as col, count(distinct Location_original_id_Risk) as cou
     from dwh.fact_signed_amount
     having count(distinct Location_original_id_Risk)=1
     union all
     select 'Location_ID_FIL_2' as col, count(distinct Location_ID_FIL_2) as cou
     from dwh.fact_signed_amount
     having count(distinct Location_ID_FIL_2)=1
     union all
     select 'Location_original_id_FIL_2' as col, count(distinct Location_original_id_FIL_2) as cou
     from dwh.fact_signed_amount
     having count(distinct Location_original_id_FIL_2)=1
     union all
     select 'Location_ID_FIL_4' as col, count(distinct Location_ID_FIL_4) as cou
     from dwh.fact_signed_amount
     having count(distinct Location_ID_FIL_4)=1
     union all
     select 'Location_original_id_FIL_4' as col, count(distinct Location_original_id_FIL_4) as cou
     from dwh.fact_signed_amount
     having count(distinct Location_original_id_FIL_4)=1
     union all
     select 'Office_ID_Fronting' as col, count(distinct Office_ID_Fronting) as cou
     from dwh.fact_signed_amount
     having count(distinct Office_ID_Fronting)=1
     union all
     select 'Office_original_id_Fronting' as col, count(distinct Office_original_id_Fronting) as cou
     from dwh.fact_signed_amount
     having count(distinct Office_original_id_Fronting)=1
     union all
     select 'Office_ID_Underwriting' as col, count(distinct Office_ID_Underwriting) as cou
     from dwh.fact_signed_amount
     having count(distinct Office_ID_Underwriting)=1
     union all
     select 'Office_original_id_Underwriting' as col, count(distinct Office_original_id_Underwriting) as cou
     from dwh.fact_signed_amount
     having count(distinct Office_original_id_Underwriting)=1
     union all
     select 'Business_Entity_ID' as col, count(distinct Business_Entity_ID) as cou
     from dwh.fact_signed_amount
     having count(distinct Business_Entity_ID)=1
     union all
     select 'Business_Entity_original_id' as col, count(distinct Business_Entity_original_id) as cou
     from dwh.fact_signed_amount
     having count(distinct Business_Entity_original_id)=1
     union all
     select 'Business_Entity_ID_Group' as col, count(distinct Business_Entity_ID_Group) as cou
     from dwh.fact_signed_amount
     having count(distinct Business_Entity_ID_Group)=1
     union all
     select 'Business_Entity_original_id_Group' as col, count(distinct Business_Entity_original_id_Group) as cou
     from dwh.fact_signed_amount
     having count(distinct Business_Entity_original_id_Group)=1
     union all
     select 'Business_Entity_ID_Finance_Group' as col, count(distinct Business_Entity_ID_Finance_Group) as cou
     from dwh.fact_signed_amount
     having count(distinct Business_Entity_ID_Finance_Group)=1
     union all
     select 'Business_Entity_original_id_Finance_Group' as col, count(distinct Business_Entity_original_id_Finance_Group) as cou
     from dwh.fact_signed_amount
     having count(distinct Business_Entity_original_id_Finance_Group)=1
     union all
     select 'Business_Entity_ID_Actuarial_Group' as col, count(distinct Business_Entity_ID_Actuarial_Group) as cou
     from dwh.fact_signed_amount
     having count(distinct Business_Entity_ID_Actuarial_Group)=1
     union all
     select 'Business_Entity_original_id_Actuarial_Group' as col, count(distinct Business_Entity_original_id_Actuarial_Group) as cou
     from dwh.fact_signed_amount
     having count(distinct Business_Entity_original_id_Actuarial_Group)=1
     union all
     select 'Currency_ID_Original' as col, count(distinct Currency_ID_Original) as cou
     from dwh.fact_signed_amount
     having count(distinct Currency_ID_Original)=1
     union all
     select 'Currency_original_id_Original' as col, count(distinct Currency_original_id_Original) as cou
     from dwh.fact_signed_amount
     having count(distinct Currency_original_id_Original)=1
     union all
     select 'Currency_ID_Settlement' as col, count(distinct Currency_ID_Settlement) as cou
     from dwh.fact_signed_amount
     having count(distinct Currency_ID_Settlement)=1
     union all
     select 'Currency_original_id_Settlement' as col, count(distinct Currency_original_id_Settlement) as cou
     from dwh.fact_signed_amount
     having count(distinct Currency_original_id_Settlement)=1
     union all
     select 'Policy_Header_ID' as col, count(distinct Policy_Header_ID) as cou
     from dwh.fact_signed_amount
     having count(distinct Policy_Header_ID)=1
     union all
     select 'Policy_Header_original_id' as col, count(distinct Policy_Header_original_id) as cou
     from dwh.fact_signed_amount
     having count(distinct Policy_Header_original_id)=1
     union all
     select 'Policy_Section_ID' as col, count(distinct Policy_Section_ID) as cou
     from dwh.fact_signed_amount
     having count(distinct Policy_Section_ID)=1
     union all
     select 'Policy_Section_original_id' as col, count(distinct Policy_Section_original_id) as cou
     from dwh.fact_signed_amount
     having count(distinct Policy_Section_original_id)=1
     union all
     select 'Signing_Transaction_ID' as col, count(distinct Signing_Transaction_ID) as cou
     from dwh.fact_signed_amount
     having count(distinct Signing_Transaction_ID)=1
     union all
     select 'Signing_Transaction_original_id' as col, count(distinct Signing_Transaction_original_id) as cou
     from dwh.fact_signed_amount
     having count(distinct Signing_Transaction_original_id)=1
     union all
     select 'Signing_Message_Detail_ID' as col, count(distinct Signing_Message_Detail_ID) as cou
     from dwh.fact_signed_amount
     having count(distinct Signing_Message_Detail_ID)=1
     union all
     select 'Signing_Message_Detail_original_id' as col, count(distinct Signing_Message_Detail_original_id) as cou
     from dwh.fact_signed_amount
     having count(distinct Signing_Message_Detail_original_id)=1
     union all
     select 'Transaction_Type_ID' as col, count(distinct Transaction_Type_ID) as cou
     from dwh.fact_signed_amount
     having count(distinct Transaction_Type_ID)=1
     union all
     select 'Transaction_Type_original_id' as col, count(distinct Transaction_Type_original_id) as cou
     from dwh.fact_signed_amount
     having count(distinct Transaction_Type_original_id)=1
     union all
     select 'Transaction_Line_ID' as col, count(distinct Transaction_Line_ID) as cou
     from dwh.fact_signed_amount
     having count(distinct Transaction_Line_ID)=1
     union all
     select 'Transaction_Line_original_id' as col, count(distinct Transaction_Line_original_id) as cou
     from dwh.fact_signed_amount
     having count(distinct Transaction_Line_original_id)=1
     union all
     select 'Trust_Fund_ID' as col, count(distinct Trust_Fund_ID) as cou
     from dwh.fact_signed_amount
     having count(distinct Trust_Fund_ID)=1
     union all
     select 'Trust_Fund_original_id' as col, count(distinct Trust_Fund_original_id) as cou
     from dwh.fact_signed_amount
     having count(distinct Trust_Fund_original_id)=1
     union all
     select 'Coverage_ID' as col, count(distinct Coverage_ID) as cou
     from dwh.fact_signed_amount
     having count(distinct Coverage_ID)=1
     union all
     select 'Coverage_original_id' as col, count(distinct Coverage_original_id) as cou
     from dwh.fact_signed_amount
     having count(distinct Coverage_original_id)=1
     union all
     select 'Item_ID' as col, count(distinct Item_ID) as cou
     from dwh.fact_signed_amount
     having count(distinct Item_ID)=1
     union all
     select 'Item_original_id' as col, count(distinct Item_original_id) as cou
     from dwh.fact_signed_amount
     having count(distinct Item_original_id)=1
     union all
     select 'Line_ID' as col, count(distinct Line_ID) as cou
     from dwh.fact_signed_amount
     having count(distinct Line_ID)=1
     union all
     select 'Line_original_id' as col, count(distinct Line_original_id) as cou
     from dwh.fact_signed_amount
     having count(distinct Line_original_id)=1
     union all
     select 'Endorsement_ID' as col, count(distinct Endorsement_ID) as cou
     from dwh.fact_signed_amount
     having count(distinct Endorsement_ID)=1
     union all
     select 'Endorsement_original_id' as col, count(distinct Endorsement_original_id) as cou
     from dwh.fact_signed_amount
     having count(distinct Endorsement_original_id)=1
     union all
     select 'Data_Source_ID' as col, count(distinct Data_Source_ID) as cou
     from dwh.fact_signed_amount
     having count(distinct Data_Source_ID)=1
     union all
     select 'Data_Source_original_id' as col, count(distinct Data_Source_original_id) as cou
     from dwh.fact_signed_amount
     having count(distinct Data_Source_original_id)=1
     union all
     select 'FIL_2_ID' as col, count(distinct FIL_2_ID) as cou
     from dwh.fact_signed_amount
     having count(distinct FIL_2_ID)=1
     union all
     select 'FIL_2_original_id' as col, count(distinct FIL_2_original_id) as cou
     from dwh.fact_signed_amount
     having count(distinct FIL_2_original_id)=1
     union all
     select 'FIL_4_ID' as col, count(distinct FIL_4_ID) as cou
     from dwh.fact_signed_amount
     having count(distinct FIL_4_ID)=1
     union all
     select 'FIL_4_original_id' as col, count(distinct FIL_4_original_id) as cou
     from dwh.fact_signed_amount
     having count(distinct FIL_4_original_id)=1
     union all
     select 'Lloyds_Risk_Code_ID' as col, count(distinct Lloyds_Risk_Code_ID) as cou
     from dwh.fact_signed_amount
     having count(distinct Lloyds_Risk_Code_ID)=1
     union all
     select 'Lloyds_Risk_Code_original_id' as col, count(distinct Lloyds_Risk_Code_original_id) as cou
     from dwh.fact_signed_amount
     having count(distinct Lloyds_Risk_Code_original_id)=1
     union all
     select 'Lead_Type_ID' as col, count(distinct Lead_Type_ID) as cou
     from dwh.fact_signed_amount
     having count(distinct Lead_Type_ID)=1
     union all
     select 'Business_Classification_ID' as col, count(distinct Business_Classification_ID) as cou
     from dwh.fact_signed_amount
     having count(distinct Business_Classification_ID)=1
     union all
     select 'Business_Classification_original_id' as col, count(distinct Business_Classification_original_id) as cou
     from dwh.fact_signed_amount
     having count(distinct Business_Classification_original_id)=1
     union all
     select 'Planning_Classification_ID' as col, count(distinct Planning_Classification_ID) as cou
     from dwh.fact_signed_amount
     having count(distinct Planning_Classification_ID)=1
     union all
     select 'Planning_Classification_original_id' as col, count(distinct Planning_Classification_original_id) as cou
     from dwh.fact_signed_amount
     having count(distinct Planning_Classification_original_id)=1
     union all
     select 'Date_ID_Inception' as col, count(distinct Date_ID_Inception) as cou
     from dwh.fact_signed_amount
     having count(distinct Date_ID_Inception)=1
     union all
     select 'Date_ID_Source_Transaction' as col, count(distinct Date_ID_Source_Transaction) as cou
     from dwh.fact_signed_amount
     having count(distinct Date_ID_Source_Transaction)=1
     union all
     select 'Date_ID_Expiry' as col, count(distinct Date_ID_Expiry) as cou
     from dwh.fact_signed_amount
     having count(distinct Date_ID_Expiry)=1
     union all
     select 'Date_ID_Settlement_Due' as col, count(distinct Date_ID_Settlement_Due) as cou
     from dwh.fact_signed_amount
     having count(distinct Date_ID_Settlement_Due)=1
     union all
     select 'Date_ID_Payment' as col, count(distinct Date_ID_Payment) as cou
     from dwh.fact_signed_amount
     having count(distinct Date_ID_Payment)=1
     union all
     select 'Date_ID_Accounting' as col, count(distinct Date_ID_Accounting) as cou
     from dwh.fact_signed_amount
     having count(distinct Date_ID_Accounting)=1
     union all
     select 'Accounting_Period_ID' as col, count(distinct Accounting_Period_ID) as cou
     from dwh.fact_signed_amount
     having count(distinct Accounting_Period_ID)=1
     union all
     select 'Accounting_Period_ID_Message' as col, count(distinct Accounting_Period_ID_Message) as cou
     from dwh.fact_signed_amount
     having count(distinct Accounting_Period_ID_Message)=1
     union all
     select 'Accounting_Period_ID_Source' as col, count(distinct Accounting_Period_ID_Source) as cou
     from dwh.fact_signed_amount
     having count(distinct Accounting_Period_ID_Source)=1
     union all
     select 'Development_Period_ID_Year_Of_Account' as col, count(distinct Development_Period_ID_Year_Of_Account) as cou
     from dwh.fact_signed_amount
     having count(distinct Development_Period_ID_Year_Of_Account)=1
     union all
     select 'Development_Period_ID_Inception_Date' as col, count(distinct Development_Period_ID_Inception_Date) as cou
     from dwh.fact_signed_amount
     having count(distinct Development_Period_ID_Inception_Date)=1
     union all
     select 'Development_Period_ID_Settlement_Due_Date' as col, count(distinct Development_Period_ID_Settlement_Due_Date) as cou
     from dwh.fact_signed_amount
     having count(distinct Development_Period_ID_Settlement_Due_Date)=1
     union all
     select 'Year_ID_Year_Of_Account' as col, count(distinct Year_ID_Year_Of_Account) as cou
     from dwh.fact_signed_amount
     having count(distinct Year_ID_Year_Of_Account)=1
     union all
     select 'Legacy_Business_Entity_Indicator' as col, count(distinct Legacy_Business_Entity_Indicator) as cou
     from dwh.fact_signed_amount
     having count(distinct Legacy_Business_Entity_Indicator)=1
     union all
     select 'Market_Gross_Gross_Signed_Premium_SCC' as col, count(distinct Market_Gross_Gross_Signed_Premium_SCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Market_Gross_Gross_Signed_Premium_SCC)=1
     union all
     select 'Line_Gross_Gross_Signed_Premium_SCC' as col, count(distinct Line_Gross_Gross_Signed_Premium_SCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Line_Gross_Gross_Signed_Premium_SCC)=1
     union all
     select 'Whole_Gross_Gross_Signed_Premium_SCC' as col, count(distinct Whole_Gross_Gross_Signed_Premium_SCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Whole_Gross_Gross_Signed_Premium_SCC)=1
     union all
     select 'Market_Gross_Gross_Signed_Premium_OCC' as col, count(distinct Market_Gross_Gross_Signed_Premium_OCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Market_Gross_Gross_Signed_Premium_OCC)=1
     union all
     select 'Line_Gross_Gross_Signed_Premium_OCC' as col, count(distinct Line_Gross_Gross_Signed_Premium_OCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Line_Gross_Gross_Signed_Premium_OCC)=1
     union all
     select 'Whole_Gross_Gross_Signed_Premium_OCC' as col, count(distinct Whole_Gross_Gross_Signed_Premium_OCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Whole_Gross_Gross_Signed_Premium_OCC)=1
     union all
     select 'Market_Gross_Net_Signed_Premium_SCC' as col, count(distinct Market_Gross_Net_Signed_Premium_SCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Market_Gross_Net_Signed_Premium_SCC)=1
     union all
     select 'Line_Gross_Net_Signed_Premium_SCC' as col, count(distinct Line_Gross_Net_Signed_Premium_SCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Line_Gross_Net_Signed_Premium_SCC)=1
     union all
     select 'Whole_Gross_Net_Signed_Premium_SCC' as col, count(distinct Whole_Gross_Net_Signed_Premium_SCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Whole_Gross_Net_Signed_Premium_SCC)=1
     union all
     select 'Market_Gross_Net_Signed_Premium_OCC' as col, count(distinct Market_Gross_Net_Signed_Premium_OCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Market_Gross_Net_Signed_Premium_OCC)=1
     union all
     select 'Line_Gross_Net_Signed_Premium_OCC' as col, count(distinct Line_Gross_Net_Signed_Premium_OCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Line_Gross_Net_Signed_Premium_OCC)=1
     union all
     select 'Whole_Gross_Net_Signed_Premium_OCC' as col, count(distinct Whole_Gross_Net_Signed_Premium_OCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Whole_Gross_Net_Signed_Premium_OCC)=1
     union all
     select 'Market_Gross_Signed_Acquisition_Cost_SCC' as col, count(distinct Market_Gross_Signed_Acquisition_Cost_SCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Market_Gross_Signed_Acquisition_Cost_SCC)=1
     union all
     select 'Line_Gross_Signed_Acquisition_Cost_SCC' as col, count(distinct Line_Gross_Signed_Acquisition_Cost_SCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Line_Gross_Signed_Acquisition_Cost_SCC)=1
     union all
     select 'Whole_Gross_Signed_Acquisition_Cost_SCC' as col, count(distinct Whole_Gross_Signed_Acquisition_Cost_SCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Whole_Gross_Signed_Acquisition_Cost_SCC)=1
     union all
     select 'Market_Gross_Signed_Acquisition_Cost_OCC' as col, count(distinct Market_Gross_Signed_Acquisition_Cost_OCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Market_Gross_Signed_Acquisition_Cost_OCC)=1
     union all
     select 'Line_Gross_Signed_Acquisition_Cost_OCC' as col, count(distinct Line_Gross_Signed_Acquisition_Cost_OCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Line_Gross_Signed_Acquisition_Cost_OCC)=1
     union all
     select 'Whole_Gross_Signed_Acquisition_Cost_OCC' as col, count(distinct Whole_Gross_Signed_Acquisition_Cost_OCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Whole_Gross_Signed_Acquisition_Cost_OCC)=1
     union all
     select 'Market_Gross_Signed_Profit_Commission_SCC' as col, count(distinct Market_Gross_Signed_Profit_Commission_SCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Market_Gross_Signed_Profit_Commission_SCC)=1
     union all
     select 'Line_Gross_Signed_Profit_Commission_SCC' as col, count(distinct Line_Gross_Signed_Profit_Commission_SCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Line_Gross_Signed_Profit_Commission_SCC)=1
     union all
     select 'Whole_Gross_Signed_Profit_Commission_SCC' as col, count(distinct Whole_Gross_Signed_Profit_Commission_SCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Whole_Gross_Signed_Profit_Commission_SCC)=1
     union all
     select 'Market_Gross_Signed_Profit_Commission_OCC' as col, count(distinct Market_Gross_Signed_Profit_Commission_OCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Market_Gross_Signed_Profit_Commission_OCC)=1
     union all
     select 'Line_Gross_Signed_Profit_Commission_OCC' as col, count(distinct Line_Gross_Signed_Profit_Commission_OCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Line_Gross_Signed_Profit_Commission_OCC)=1
     union all
     select 'Whole_Gross_Signed_Profit_Commission_OCC' as col, count(distinct Whole_Gross_Signed_Profit_Commission_OCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Whole_Gross_Signed_Profit_Commission_OCC)=1
     union all
     select 'Market_Gross_Signed_Reinstatement_Premium_SCC' as col, count(distinct Market_Gross_Signed_Reinstatement_Premium_SCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Market_Gross_Signed_Reinstatement_Premium_SCC)=1
     union all
     select 'Line_Gross_Signed_Reinstatement_Premium_SCC' as col, count(distinct Line_Gross_Signed_Reinstatement_Premium_SCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Line_Gross_Signed_Reinstatement_Premium_SCC)=1
     union all
     select 'Whole_Gross_Signed_Reinstatement_Premium_SCC' as col, count(distinct Whole_Gross_Signed_Reinstatement_Premium_SCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Whole_Gross_Signed_Reinstatement_Premium_SCC)=1
     union all
     select 'Market_Gross_Signed_Reinstatement_Premium_OCC' as col, count(distinct Market_Gross_Signed_Reinstatement_Premium_OCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Market_Gross_Signed_Reinstatement_Premium_OCC)=1
     union all
     select 'Line_Gross_Signed_Reinstatement_Premium_OCC' as col, count(distinct Line_Gross_Signed_Reinstatement_Premium_OCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Line_Gross_Signed_Reinstatement_Premium_OCC)=1
     union all
     select 'Whole_Gross_Signed_Reinstatement_Premium_OCC' as col, count(distinct Whole_Gross_Signed_Reinstatement_Premium_OCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Whole_Gross_Signed_Reinstatement_Premium_OCC)=1
     union all
     select 'Market_Gross_Paid_Claim_SCC' as col, count(distinct Market_Gross_Paid_Claim_SCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Market_Gross_Paid_Claim_SCC)=1
     union all
     select 'Line_Gross_Paid_Claim_SCC' as col, count(distinct Line_Gross_Paid_Claim_SCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Line_Gross_Paid_Claim_SCC)=1
     union all
     select 'Whole_Gross_Paid_Claim_SCC' as col, count(distinct Whole_Gross_Paid_Claim_SCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Whole_Gross_Paid_Claim_SCC)=1
     union all
     select 'Market_Gross_Paid_Claim_OCC' as col, count(distinct Market_Gross_Paid_Claim_OCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Market_Gross_Paid_Claim_OCC)=1
     union all
     select 'Line_Gross_Paid_Claim_OCC' as col, count(distinct Line_Gross_Paid_Claim_OCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Line_Gross_Paid_Claim_OCC)=1
     union all
     select 'Whole_Gross_Paid_Claim_OCC' as col, count(distinct Whole_Gross_Paid_Claim_OCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Whole_Gross_Paid_Claim_OCC)=1
     union all
     select 'Market_Gross_Paid_Claim_Loss_Fund_SCC' as col, count(distinct Market_Gross_Paid_Claim_Loss_Fund_SCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Market_Gross_Paid_Claim_Loss_Fund_SCC)=1
     union all
     select 'Line_Gross_Paid_Claim_Loss_Fund_SCC' as col, count(distinct Line_Gross_Paid_Claim_Loss_Fund_SCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Line_Gross_Paid_Claim_Loss_Fund_SCC)=1
     union all
     select 'Whole_Gross_Paid_Claim_Loss_Fund_SCC' as col, count(distinct Whole_Gross_Paid_Claim_Loss_Fund_SCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Whole_Gross_Paid_Claim_Loss_Fund_SCC)=1
     union all
     select 'Market_Gross_Paid_Claim_Loss_Fund_OCC' as col, count(distinct Market_Gross_Paid_Claim_Loss_Fund_OCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Market_Gross_Paid_Claim_Loss_Fund_OCC)=1
     union all
     select 'Line_Gross_Paid_Claim_Loss_Fund_OCC' as col, count(distinct Line_Gross_Paid_Claim_Loss_Fund_OCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Line_Gross_Paid_Claim_Loss_Fund_OCC)=1
     union all
     select 'Whole_Gross_Paid_Claim_Loss_Fund_OCC' as col, count(distinct Whole_Gross_Paid_Claim_Loss_Fund_OCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Whole_Gross_Paid_Claim_Loss_Fund_OCC)=1
     union all
     select 'Market_Gross_Signed_Overseas_Tax_SCC' as col, count(distinct Market_Gross_Signed_Overseas_Tax_SCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Market_Gross_Signed_Overseas_Tax_SCC)=1
     union all
     select 'Line_Gross_Signed_Overseas_Tax_SCC' as col, count(distinct Line_Gross_Signed_Overseas_Tax_SCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Line_Gross_Signed_Overseas_Tax_SCC)=1
     union all
     select 'Whole_Gross_Signed_Overseas_Tax_SCC' as col, count(distinct Whole_Gross_Signed_Overseas_Tax_SCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Whole_Gross_Signed_Overseas_Tax_SCC)=1
     union all
     select 'Market_Gross_Signed_Overseas_Tax_OCC' as col, count(distinct Market_Gross_Signed_Overseas_Tax_OCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Market_Gross_Signed_Overseas_Tax_OCC)=1
     union all
     select 'Line_Gross_Signed_Overseas_Tax_OCC' as col, count(distinct Line_Gross_Signed_Overseas_Tax_OCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Line_Gross_Signed_Overseas_Tax_OCC)=1
     union all
     select 'Whole_Gross_Signed_Overseas_Tax_OCC' as col, count(distinct Whole_Gross_Signed_Overseas_Tax_OCC) as cou
     from dwh.fact_signed_amount
     having count(distinct Whole_Gross_Signed_Overseas_Tax_OCC)=1)fct
where cou>1 """

pd_df_distinct_signed_amt = pd.read_sql_query(pd_distinct_signed_amt, connDw)

if pd_df_distinct_signed_amt.empty:
    print('Output : Distinct check is passed  For Fact Signed Amount!!')
else:
    pd.set_option('display.max_columns', None)    
    print('Output : Distinct check is failed for For Fact Signed Amount !! ')
    display(pd_df_distinct_signed_amt)

# COMMAND ----------

# DBTITLE 1,Distinct Check (Fact Estimated Premium) for Phase A Fact - Should get no records in output
pd_distinct_estimated_premium = """ Select *
from(SELECT 'Fact_Estimated_Premium_HBK' AS col, COUNT(DISTINCT fep.Fact_Estimated_Premium_HBK) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT fep.Fact_Estimated_Premium_HBK)=1
     UNION ALL
     SELECT 'Party_ID_Underwriter' AS col, COUNT(DISTINCT fep.Party_ID_Underwriter) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Party_ID_Underwriter)=1
     UNION ALL
     SELECT 'Party_Original_ID_Underwriter' AS col, COUNT(DISTINCT Party_Original_ID_Underwriter) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Party_Original_ID_Underwriter)=1
     UNION ALL
     SELECT 'Party_ID_Broker' AS col, COUNT(DISTINCT Party_ID_Broker) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Party_ID_Broker)=1
     UNION ALL
     SELECT 'Party_Original_ID_Broker' AS col, COUNT(DISTINCT Party_Original_ID_Broker) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Party_Original_ID_Broker)=1
     UNION ALL
     SELECT 'Party_ID_Coverholder' AS col, COUNT(DISTINCT Party_ID_Coverholder) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Party_ID_Coverholder)=1
     UNION ALL
     SELECT 'Party_Original_ID_Coverholder' AS col, COUNT(DISTINCT Party_Original_ID_Coverholder) AS cou
     FROM dwh.Fact_Signed_Amount
     HAVING COUNT(DISTINCT Party_Original_ID_Coverholder)=1
     UNION ALL
     SELECT 'Party_ID_Insured' AS col, COUNT(DISTINCT Party_ID_Insured) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Party_ID_Insured)=1
     UNION ALL
     SELECT 'Party_Original_ID_Insured' AS col, COUNT(DISTINCT Party_Original_ID_Insured) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Party_Original_ID_Insured)=1
     UNION ALL
     SELECT 'Party_ID_Reinsured' AS col, COUNT(DISTINCT Party_ID_Reinsured) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Party_ID_Reinsured)=1
     UNION ALL
     SELECT 'Party_Original_ID_Reinsured' AS col, COUNT(DISTINCT Party_Original_ID_Reinsured) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Party_Original_ID_Reinsured)=1
     UNION ALL
     SELECT 'Location_ID_Domicile' AS col, COUNT(DISTINCT Location_ID_Domicile) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Location_ID_Domicile)=1
     UNION ALL
     SELECT 'Location_Original_ID_Domicile' AS col, COUNT(DISTINCT Location_Original_ID_Domicile) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Location_Original_ID_Domicile)=1
     UNION ALL
     SELECT 'Location_ID_Risk' AS col, COUNT(DISTINCT Location_ID_Risk) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Location_ID_Risk)=1
     UNION ALL
     SELECT 'Location_Original_ID_Risk' AS col, COUNT(DISTINCT Location_Original_ID_Risk) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Location_Original_ID_Risk)=1
     UNION ALL
     SELECT 'Office_ID_Fronting' AS col, COUNT(DISTINCT fep.Office_ID_Fronting) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Office_ID_Fronting)=1
     UNION ALL
     SELECT 'Office_Original_ID_Fronting' AS col, COUNT(DISTINCT fep.Office_Original_ID_Fronting) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Office_Original_ID_Fronting)=1
     UNION ALL
     SELECT 'Office_ID_Underwriting' AS col, COUNT(DISTINCT fep.Office_ID_Underwriting) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT fep.Office_ID_Underwriting)=1
     UNION ALL
     SELECT 'Office_Original_ID_Underwriting' AS col, COUNT(DISTINCT fep.Office_Original_ID_Underwriting) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Office_Original_ID_Underwriting)=1
     UNION ALL
     SELECT 'Business_Entity_ID' AS col, COUNT(DISTINCT Business_Entity_ID) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Business_Entity_ID)=1
     UNION ALL
     SELECT 'Business_Entity_Original_ID' AS col, COUNT(DISTINCT Business_Entity_Original_ID) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Business_Entity_Original_ID)=1
     UNION ALL
     SELECT 'Business_Entity_ID_Group' AS col, COUNT(DISTINCT Business_Entity_ID_Group) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Business_Entity_ID_Group)=1
     UNION ALL
     SELECT 'Business_Entity_Original_ID_Group' AS col, COUNT(DISTINCT Business_Entity_Original_ID_Group) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Business_Entity_Original_ID_Group)=1
     UNION ALL
     SELECT 'Business_Entity_ID_Finance_Group' AS col, COUNT(DISTINCT Business_Entity_ID_Finance_Group) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Business_Entity_ID_Finance_Group)=1
     UNION ALL
     SELECT 'Business_Entity_Original_ID_Finance_Group' AS col, COUNT(DISTINCT Business_Entity_Original_ID_Finance_Group) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Business_Entity_Original_ID_Finance_Group)=1
     UNION ALL
     SELECT 'Business_Entity_ID_Actuarial_Group' AS col, COUNT(DISTINCT Business_Entity_ID_Actuarial_Group) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Business_Entity_ID_Actuarial_Group)=1
     UNION ALL
     SELECT 'Business_Entity_Original_ID_Actuarial_Group' AS col, COUNT(DISTINCT Business_Entity_Original_ID_Actuarial_Group) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Business_Entity_Original_ID_Actuarial_Group)=1
     UNION ALL
     SELECT 'Currency_ID_Original' AS col, COUNT(DISTINCT Currency_ID_Original) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Currency_ID_Original)=1
     UNION ALL
     SELECT 'Currency_Original_ID_Original' AS col, COUNT(DISTINCT Currency_Original_ID_Original) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Currency_Original_ID_Original)=1
     UNION ALL
     SELECT 'Currency_ID_Settlement' AS col, COUNT(DISTINCT Currency_ID_Settlement) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Currency_ID_Settlement)=1
     UNION ALL
     SELECT 'Currency_Original_ID_Settlement' AS col, COUNT(DISTINCT Currency_Original_ID_Settlement) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Currency_Original_ID_Settlement)=1
     UNION ALL
     SELECT 'Policy_Header_ID' AS col, COUNT(DISTINCT Policy_Header_ID) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Policy_Header_ID)=1
     UNION ALL
     SELECT 'Policy_Header_Original_ID' AS col, COUNT(DISTINCT Policy_Header_Original_ID) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Policy_Header_Original_ID)=1
     UNION ALL
     SELECT 'Policy_Section_ID' AS col, COUNT(DISTINCT Policy_Section_ID) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Policy_Section_ID)=1
     UNION ALL
     SELECT 'Policy_Section_Original_ID' AS col, COUNT(DISTINCT Policy_Section_Original_ID) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Policy_Section_Original_ID)=1
     UNION ALL
     SELECT 'Estimated_Premium_Transaction_ID' AS col, COUNT(DISTINCT fep.Estimated_Premium_Transaction_ID) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Estimated_Premium_Transaction_ID)=1
     UNION ALL
     SELECT 'Estimated_Premium_Transaction_Original_ID' AS col, COUNT(DISTINCT fep.Estimated_Premium_Transaction_Original_ID) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Estimated_Premium_Transaction_Original_ID)=1
     UNION ALL
     SELECT 'Transaction_Type_ID' AS col, COUNT(DISTINCT Transaction_Type_ID) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Transaction_Type_ID)=1
     UNION ALL
     SELECT 'Transaction_Type_Original_ID' AS col, COUNT(DISTINCT Transaction_Type_Original_ID) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Transaction_Type_Original_ID)=1
     UNION ALL
     SELECT 'Transaction_Line_ID' AS col, COUNT(DISTINCT Transaction_Line_ID) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Transaction_Line_ID)=1
     UNION ALL
     SELECT 'Transaction_Line_Original_ID' AS col, COUNT(DISTINCT Transaction_Line_Original_ID) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Transaction_Line_Original_ID)=1
     UNION ALL
     SELECT 'Trust_Fund_ID' AS col, COUNT(DISTINCT Trust_Fund_ID) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Trust_Fund_ID)=1
     UNION ALL
     SELECT 'Trust_Fund_Original_ID' AS col, COUNT(DISTINCT Trust_Fund_Original_ID) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Trust_Fund_Original_ID)=1
     UNION ALL
     SELECT 'Coverage_ID' AS col, COUNT(DISTINCT Coverage_ID) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Coverage_ID)=1
     UNION ALL
     SELECT 'Coverage_Original_ID' AS col, COUNT(DISTINCT Coverage_Original_ID) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Coverage_Original_ID)=1
     UNION ALL
     SELECT 'Item_ID' AS col, COUNT(DISTINCT Item_ID) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Item_ID)=1
     UNION ALL
     SELECT 'Item_Original_ID' AS col, COUNT(DISTINCT Item_Original_ID) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Item_Original_ID)=1
     UNION ALL
     SELECT 'Line_ID' AS col, COUNT(DISTINCT Line_ID) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Line_ID)=1
     UNION ALL
     SELECT 'Line_Original_ID' AS col, COUNT(DISTINCT Line_Original_ID) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Line_Original_ID)=1
     UNION ALL
     SELECT 'Endorsement_ID' AS col, COUNT(DISTINCT Endorsement_ID) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Endorsement_ID)=1
     UNION ALL
     SELECT 'Endorsement_Original_ID' AS col, COUNT(DISTINCT Endorsement_Original_ID) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Endorsement_Original_ID)=1
     UNION ALL
     SELECT 'Data_Source_ID' AS col, COUNT(DISTINCT Data_Source_ID) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Data_Source_ID)=1
     UNION ALL
     SELECT 'Data_Source_Original_ID' AS col, COUNT(DISTINCT Data_Source_Original_ID) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Data_Source_Original_ID)=1
     UNION ALL
     SELECT 'Lloyds_Risk_Code_ID' AS col, COUNT(DISTINCT Lloyds_Risk_Code_ID) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Lloyds_Risk_Code_ID)=1
     UNION ALL
     SELECT 'Lloyds_Risk_Code_Original_ID' AS col, COUNT(DISTINCT Lloyds_Risk_Code_Original_ID) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Lloyds_Risk_Code_Original_ID)=1
     UNION ALL
     SELECT 'Lead_Type_ID' AS col, COUNT(DISTINCT Lead_Type_ID) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Lead_Type_ID)=1
     UNION ALL
     SELECT 'Business_Classification_ID' AS col, COUNT(DISTINCT Business_Classification_ID) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Business_Classification_ID)=1
     UNION ALL
     SELECT 'Business_Classification_Original_ID' AS col, COUNT(DISTINCT Business_Classification_Original_ID) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Business_Classification_Original_ID)=1
     UNION ALL
     SELECT 'Planning_Classification_ID' AS col, COUNT(DISTINCT Planning_Classification_ID) AS cou
     FROM dwh.Fact_Signed_Amount
     HAVING COUNT(DISTINCT Planning_Classification_ID)=1
     UNION ALL
     SELECT 'Planning_Classification_Original_ID' AS col, COUNT(DISTINCT Planning_Classification_Original_ID) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Planning_Classification_Original_ID)=1
     UNION ALL
     SELECT 'Date_ID_Inception' AS col, COUNT(DISTINCT Date_ID_Inception) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Date_ID_Inception)=1
     UNION ALL
     SELECT 'Date_ID_Source_Transaction' AS col, COUNT(DISTINCT fep.Date_ID_Source_Transaction) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Date_ID_Source_Transaction)=1
     UNION ALL
     SELECT 'Date_ID_Expiry' AS col, COUNT(DISTINCT Date_ID_Expiry) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Date_ID_Expiry)=1
     UNION ALL
     SELECT 'Date_ID_Accounting' AS col, COUNT(DISTINCT fep.Date_ID_Accounting) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Date_ID_Accounting)=1
     UNION ALL
     SELECT 'Date_ID_Settlement_Due' AS col, COUNT(DISTINCT fep.Date_ID_Settlement_Due) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Date_ID_Settlement_Due)=1
     UNION ALL
     SELECT 'Accounting_Period_ID' AS col, COUNT(DISTINCT fep.Accounting_Period_ID) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Accounting_Period_ID)=1
     UNION ALL
     SELECT 'Accounting_Period_ID_Source' AS col, COUNT(DISTINCT fep.Accounting_Period_ID_Source) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Accounting_Period_ID_Source)=1
     UNION ALL
     SELECT 'Development_Period_ID_Year_Of_Account' AS col, COUNT(DISTINCT fep.Development_Period_ID_Year_Of_Account) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Development_Period_ID_Year_Of_Account)=1
     UNION ALL
     SELECT 'Development_Period_ID_Inception_Date' AS col, COUNT(DISTINCT fep.Development_Period_ID_Inception_Date) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Development_Period_ID_Inception_Date)=1
     UNION ALL
     SELECT 'Year_ID_Year_Of_Account' AS col, COUNT(DISTINCT fep.Year_ID_Year_Of_Account) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Year_ID_Year_Of_Account)=1
     UNION ALL
     SELECT 'Legacy_Business_Entity_Indicator' AS col, COUNT(DISTINCT Legacy_Business_Entity_Indicator) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Legacy_Business_Entity_Indicator)=1
     UNION ALL
     SELECT 'Market_Gross_Gross_Estimated_Premium_SCC' AS col, COUNT(DISTINCT fep.Market_Gross_Gross_Estimated_Premium_SCC) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Market_Gross_Gross_Estimated_Premium_SCC)=1
     UNION ALL
     SELECT 'Line_Gross_Gross_Estimated_Premium_SCC' AS col, COUNT(DISTINCT fep.Line_Gross_Gross_Estimated_Premium_SCC) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Line_Gross_Gross_Estimated_Premium_SCC)=1
     UNION ALL
     SELECT 'Whole_Gross_Gross_Estimated_Premium_SCC' AS col, COUNT(DISTINCT fep.Whole_Gross_Gross_Estimated_Premium_SCC) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Whole_Gross_Gross_Estimated_Premium_SCC)=1
     UNION ALL
     SELECT 'Market_Gross_Gross_Estimated_Premium_OCC' AS col, COUNT(DISTINCT fep.Market_Gross_Gross_Estimated_Premium_OCC) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Market_Gross_Gross_Estimated_Premium_OCC)=1
     UNION ALL
     SELECT 'Line_Gross_Gross_Estimated_Premium_OCC' AS col, COUNT(DISTINCT fep.Line_Gross_Gross_Estimated_Premium_OCC) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Line_Gross_Gross_Estimated_Premium_OCC)=1
     UNION ALL
     SELECT 'Whole_Gross_Gross_Estimated_Premium_OCC' AS col, COUNT(DISTINCT fep.Whole_Gross_Gross_Estimated_Premium_OCC) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Whole_Gross_Gross_Estimated_Premium_OCC)=1
     UNION ALL
     SELECT 'Market_Gross_Net_Estimated_Premium_SCC' AS col, COUNT(DISTINCT fep.Market_Gross_Net_Estimated_Premium_SCC) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Market_Gross_Net_Estimated_Premium_SCC)=1
     UNION ALL
     SELECT 'Line_Gross_Net_Estimated_Premium_SCC' AS col, COUNT(DISTINCT fep.Line_Gross_Net_Estimated_Premium_SCC) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Line_Gross_Net_Estimated_Premium_SCC)=1
     UNION ALL
     SELECT 'Whole_Gross_Net_Estimated_Premium_SCC' AS col, COUNT(DISTINCT fep.Whole_Gross_Net_Estimated_Premium_SCC) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Whole_Gross_Net_Estimated_Premium_SCC)=1
     UNION ALL
     SELECT 'Market_Gross_Net_Estimated_Premium_OCC' AS col, COUNT(DISTINCT fep.Market_Gross_Net_Estimated_Premium_OCC) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Market_Gross_Net_Estimated_Premium_OCC)=1
     UNION ALL
     SELECT 'Line_Gross_Net_Estimated_Premium_OCC' AS col, COUNT(DISTINCT fep.Line_Gross_Net_Estimated_Premium_OCC) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Line_Gross_Net_Estimated_Premium_OCC)=1
     UNION ALL
     SELECT 'Whole_Gross_Net_Estimated_Premium_OCC' AS col, COUNT(DISTINCT fep.Whole_Gross_Net_Estimated_Premium_OCC) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Whole_Gross_Net_Estimated_Premium_OCC)=1
     UNION ALL
     SELECT 'Market_Gross_Estimated_Acquisition_Cost_SCC' AS col, COUNT(DISTINCT fep.Market_Gross_Estimated_Acquisition_Cost_SCC) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Market_Gross_Estimated_Acquisition_Cost_SCC)=1
     UNION ALL
     SELECT 'Line_Gross_Estimated_Acquisition_Cost_SCC' AS col, COUNT(DISTINCT fep.Line_Gross_Estimated_Acquisition_Cost_SCC) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Line_Gross_Estimated_Acquisition_Cost_SCC)=1
     UNION ALL
     SELECT 'Whole_Gross_Estimated_Acquisition_Cost_SCC' AS col, COUNT(DISTINCT fep.Whole_Gross_Estimated_Acquisition_Cost_SCC) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Whole_Gross_Estimated_Acquisition_Cost_SCC)=1
     UNION ALL
     SELECT 'Market_Gross_Estimated_Acquisition_Cost_OCC' AS col, COUNT(DISTINCT fep.Market_Gross_Estimated_Acquisition_Cost_OCC) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Market_Gross_Estimated_Acquisition_Cost_OCC)=1
     UNION ALL
     SELECT 'Line_Gross_Estimated_Acquisition_Cost_OCC' AS col, COUNT(DISTINCT fep.Line_Gross_Estimated_Acquisition_Cost_OCC) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Line_Gross_Estimated_Acquisition_Cost_OCC)=1
     UNION ALL
     SELECT 'Whole_Gross_Estimated_Acquisition_Cost_OCC' AS col, COUNT(DISTINCT fep.Whole_Gross_Estimated_Acquisition_Cost_OCC) AS cou
     FROM dwh.Fact_Estimated_Premium fep
     HAVING COUNT(DISTINCT Whole_Gross_Estimated_Acquisition_Cost_OCC)=1)fct
where cou>1 """

pd_df_distinct_estimated_premium = pd.read_sql_query(pd_distinct_estimated_premium, connDw)

if pd_df_distinct_estimated_premium.empty:
    print('Output : Distinct check is passed For Fact Estimated Premium !!')
else:
    pd.set_option('display.max_columns', None)    
    print('Output : Distinct check is failed for For Fact Estimated Premium')
    display(pd_df_distinct_estimated_premium)

# COMMAND ----------

# DBTITLE 1,Distinct Check (Fact Exchange rate) for Phase A Fact - Should get no records in output
pd_distinct_exchange_rate = """ Select *
from(select 'Fact_Exchange_Rate_HBK' AS Col, count(distinct Fact_Exchange_Rate_HBK) as coldistinctCount
     from dwh.Fact_Exchange_Rate
     having count(distinct Fact_Exchange_Rate_HBK)=1
     union all
     select 'Exchange_Rate_Type_ID' AS Col, count(distinct Exchange_Rate_Type_ID) as coldistinctCount
     from dwh.Fact_Exchange_Rate
     having count(distinct Exchange_Rate_Type_ID)=1
     union all
     select 'Exchange_Rate_Type_Original_ID' AS Col, count(distinct Exchange_Rate_Type_Original_ID) as coldistinctCount
     from dwh.Fact_Exchange_Rate
     having count(distinct Exchange_Rate_Type_Original_ID)=1
     union all
     select 'Data_Source_ID' AS Col, count(distinct Data_Source_ID) as coldistinctCount
     from dwh.Fact_Exchange_Rate
     having count(distinct Data_Source_ID)=1
     union all
     select 'Data_Source_Original_ID' AS Col, count(distinct Data_Source_Original_ID) as coldistinctCount
     from dwh.Fact_Exchange_Rate
     having count(distinct Data_Source_Original_ID)=1
     union all
     select 'Currency_ID_Original' AS Col, count(distinct Currency_ID_Original) as coldistinctCount
     from dwh.Fact_Exchange_Rate
     having count(distinct Currency_ID_Original)=1
     union all
     select 'Currency_Original_ID_Original' AS Col, count(distinct Currency_Original_ID_Original) as coldistinctCount
     from dwh.Fact_Exchange_Rate
     having count(distinct Currency_Original_ID_Original)=1
     union all
     select 'Currency_ID_Settlement' AS Col, count(distinct Currency_ID_Settlement) as coldistinctCount
     from dwh.Fact_Exchange_Rate
     having count(distinct Currency_ID_Settlement)=1
     union all
     select 'Currency_Original_ID_Settlement' AS Col, count(distinct Currency_Original_ID_Settlement) as coldistinctCount
     from dwh.Fact_Exchange_Rate
     having count(distinct Currency_Original_ID_Settlement)=1
     union all
     select 'Date_ID_Accounting' AS Col, count(distinct Date_ID_Accounting) as coldistinctCount
     from dwh.Fact_Exchange_Rate
     having count(distinct Date_ID_Accounting)=1
     union all
     select 'Date_ID_Exchange_Rate' AS Col, count(distinct Date_ID_Exchange_Rate) as coldistinctCount
     from dwh.Fact_Exchange_Rate
     having count(distinct Date_ID_Exchange_Rate)=1
     union all
     select 'Accounting_Period_ID' AS Col, count(distinct Accounting_Period_ID) as coldistinctCount
     from dwh.Fact_Exchange_Rate
     having count(distinct Accounting_Period_ID)=1)fct
where coldistinctCount>1"""

pd_df_distinct_exchange_rate = pd.read_sql_query(pd_distinct_exchange_rate, connDw)

if pd_df_distinct_exchange_rate.empty:
    print('Output : Distinct check is passed For Fact Exchange Rate !!')
else:
    pd.set_option('display.max_columns', None)    
    print('Output : Distinct check is failed For Fact Exchange Rate !! ')
    display(pd_df_distinct_exchange_rate)


# COMMAND ----------

