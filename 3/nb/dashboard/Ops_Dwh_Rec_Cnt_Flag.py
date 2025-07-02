# Databricks notebook source
# MAGIC %md ## Notebook for record counts check based on flag and BED

# COMMAND ----------

# DBTITLE 1,Setup Parameter
dbutils.widgets.removeAll()
dbutils.widgets.text("datetime_required","2023-04-10 20:00:00","Enter the datetime [yyyy-mm-dd hh24:mi:ss] after which analysis is required : ")
datetime_required = dbutils.widgets.get("datetime_required")

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

# DBTITLE 1,Record Count Check on Flags
query_check = f"""
select
    case when main_tbl.table_name like 'dwh.Fact%' then 1
     when main_tbl.table_name like 'dwh.Bridge%' then 2
     else 3
end as table_type, main_tbl.*
from ( 
    select 'dwh.Bridge_Claim_Line' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Bridge_Claim_Line
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Bridge_Entity_Currency_Type' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Bridge_Entity_Currency_Type
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Bridge_Office_Line' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Bridge_Office_Line
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Bridge_Party_Agency_Rating' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Bridge_Party_Agency_Rating
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Bridge_Party_Claim_Activity_Role' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Bridge_Party_Claim_Activity_Role
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Bridge_Party_Claim_Role' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Bridge_Party_Claim_Role
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Bridge_Party_Group_Party' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Bridge_Party_Group_Party
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Bridge_Party_Policy_Header_Role' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Bridge_Party_Policy_Header_Role
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Bridge_Party_Policy_Section_Role' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Bridge_Party_Policy_Section_Role
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Bridge_Party_Signing_Transaction_Role' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Bridge_Party_Signing_Transaction_Role
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Control_Reporting_Usage' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Control_Reporting_Usage
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Agency' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Agency
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Bdx_Bordereaux' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Bdx_Bordereaux
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Bdx_Claim' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Bdx_Claim
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Bdx_Declaration' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Bdx_Declaration
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Bdx_Insurable_Interest' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Bdx_Insurable_Interest
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Bdx_Insurable_Interest_Cargo' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Bdx_Insurable_Interest_Cargo
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Bdx_Insurable_Interest_Characteristics' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Bdx_Insurable_Interest_Characteristics
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Bdx_Insurable_Interest_Classification' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Bdx_Insurable_Interest_Classification
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Bdx_Insurable_Interest_Liability' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Bdx_Insurable_Interest_Liability
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Bdx_Insurable_Interest_Limit' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Bdx_Insurable_Interest_Limit
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Bdx_Insurable_Interest_Motor' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Bdx_Insurable_Interest_Motor
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Bdx_Insurable_Interest_Property' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Bdx_Insurable_Interest_Property
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Bdx_Transaction_Type' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Bdx_Transaction_Type
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Business_Classification' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Business_Classification
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Business_Entity' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Business_Entity
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Claim' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Claim
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Claim_Activity' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Claim_Activity
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Claim_Characteristic' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Claim_Characteristic
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Claim_Line' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Claim_Line
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Claim_Loss_Narrative' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Claim_Loss_Narrative
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Claim_Message_Detail' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Claim_Message_Detail
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Claim_Note' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Claim_Note
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Claim_Transaction' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Claim_Transaction
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Clause' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Clause
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Coverage' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Coverage
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Currency' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Currency
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Data_Source' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Data_Source
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Deduction' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Deduction
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Endorsement' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Endorsement
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Estimated_Premium_Transaction' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Estimated_Premium_Transaction
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Exchange_Rate_Type' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Exchange_Rate_Type
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_FIL_2' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_FIL_2
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_FIL_4' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_FIL_4
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Item' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Item
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Limit' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Limit
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Line' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Line
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Lloyds_Risk_Code' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Lloyds_Risk_Code
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Location' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Location
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Loss_Event' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Loss_Event
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Note' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Note
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Office' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Office
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Party' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Party
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Party_Group' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Party_Group
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Peril' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Peril
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Planning_Classification' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Planning_Classification
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Policy_Activity' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Policy_Activity
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Policy_Characteristic' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Policy_Characteristic
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Policy_Header' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Policy_Header
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Policy_Section' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Policy_Section
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Pricing_Rating' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Pricing_Rating
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Rating_Type' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Rating_Type
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Reinstatement' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Reinstatement
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Role' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Role
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Signing_Message_Detail' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Signing_Message_Detail
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Signing_Message_Narrative' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Signing_Message_Narrative
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Signing_Message_Treaty_Detail' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Signing_Message_Treaty_Detail
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Signing_Transaction' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Signing_Transaction
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Transaction_Line' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Transaction_Line
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Fact_Bdx_Risk_Premium_Transaction' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Fact_Bdx_Risk_Premium_Transaction
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Fact_Bdx_Risk_Premium_Transaction_Movements' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Fact_Bdx_Risk_Premium_Transaction_Movements
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Fact_Claim' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Fact_Claim
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Fact_Estimated_Premium' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Fact_Estimated_Premium
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Fact_Exchange_Rate' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Fact_Exchange_Rate
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Fact_Limit' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Fact_Limit
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Fact_Pricing_Rating' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Fact_Pricing_Rating
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Fact_Signed_Amount' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Fact_Signed_Amount
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Transaction_Type' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Transaction_Type
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Dim_Trust_Fund' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Dim_Trust_Fund
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
UNION
    select 'dwh.Fact_Bdx_Claim_Transaction' as Table_Name, dwh_valid_from, dwh_action_type, count(*) as count
    from dwh.Fact_Bdx_Claim_Transaction
    where dwh_valid_from >= '{datetime_required}'
    group by dwh_valid_from,dwh_action_type
) main_tbl
order by 1,2,3 desc,4"""

pd_df_check = pd.read_sql_query(query_check,connDw)

if pd_df_check.empty:
    print('Empty Dataframe !!')
else:
    pd.set_option('display.max_columns', None)
    display(pd_df_check)

connDw.close()

# COMMAND ----------

# DBTITLE 1,Recon Query Check
# MAGIC %sql
# MAGIC SELECT Measure_Source
# MAGIC      , Measure_Group_Name
# MAGIC      , ROUND(SUM(ABS(RED_AMOUNT)), 2) AS RED_AMOUNT
# MAGIC      , ROUND(SUM(ABS(DP_AMOUNT)), 2) AS DP_AMOUNT
# MAGIC      , ROUND(SUM(ABS(VAR_AMOUNT)), 2) AS VAR_AMOUNT
# MAGIC      , EXPLANATION
# MAGIC   FROM red_dp_recon.variance_analysis
# MAGIC  WHERE var_amount <> 0
# MAGIC  GROUP
# MAGIC     BY Measure_Source, Measure_Group_Name, EXPLANATION
# MAGIC  ORDER
# MAGIC     BY EXPLANATION