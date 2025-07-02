# Databricks notebook source
# MAGIC %md ## Notebook for reconciliation of BDX Facts

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

# DBTITLE 1,Fact_Bdx_Claim_Transaction - Helper Query

try:
    qry = """SET NOCOUNT ON;
            DROP TABLE etl.LineTotals_claim ;
            DROP TABLE etl.LineFactor_tbl_claim ;
            select '' """
    pd.read_sql_query(qry,connDw)
except:
    print("Exception thrown , tables might not exists to delete")
LineTotals_claim_qry = """
SET NOCOUNT ON;
CREATE TABLE etl.LineTotals_claim
WITH (CLUSTERED COLUMNSTORE INDEX ,DISTRIBUTION = HASH ([policy_header_reference])) AS 					
SELECT					
sourceID					
,policy_header_reference					
,policy_section_reference					
,sum(Written_Line) AS Total_Written_Line					
FROM staging.line					
WHERE lakeisactive = 1 and lakedeletedtimestamp is null				
AND sourceID = '40'					
GROUP BY sourceID, policy_header_reference, policy_section_reference ;
select ''
"""

pd.read_sql_query(LineTotals_claim_qry,connDw)

LineFactor_tbl_claim_qry = """
SET NOCOUNT ON;
CREATE TABLE etl.LineFactor_tbl_claim
WITH (CLUSTERED COLUMNSTORE INDEX ,DISTRIBUTION = HASH ([policy_header_reference])) AS
select ln.sourceID
, ln.policy_header_reference
, ln.policy_section_reference
, ln.Business_Entity_Code
, ln.lakeDeletedTimestamp
, CASE WHEN lt.Total_Written_Line = 0
THEN 1
ELSE CAST(ln.Written_Line / lt.Total_Written_Line AS DECIMAL(6,3))
END AS Line_Factor
FROM staging.line ln
JOIN etl.LineTotals_claim  lt
ON ln.policy_header_reference = lt.policy_header_reference
and ln.policy_section_reference = lt.policy_section_reference
and ln.sourceID = lt.sourceID
WHERE lakeisactive = 1
AND ln.sourceID = '40';
select ''		
"""
pd.read_sql_query(LineFactor_tbl_claim_qry,connDw)




# COMMAND ----------

# DBTITLE 1,Fact_Bdx_Risk_Premium_Transaction_Movements - Helper Query

try:
    qry = """SET NOCOUNT ON;
            DROP TABLE etl.LineTotals_test_movements_rptm ;
            DROP TABLE etl.LineFactor_tbl_test_movements_rptm
            DROP TABLE etl.LineTotals_risk_test_rptm
            DROP TABLE etl.LineFactor_risk_test_rptm;
            select '' """
    pd.read_sql_query(qry,connDw)
except:
    print("Exception thrown , tables might not exists to delete")
LineTotals_test_movements_qry = """
SET NOCOUNT ON;
CREATE TABLE etl.LineTotals_test_movements_rptm
WITH (CLUSTERED COLUMNSTORE INDEX ,DISTRIBUTION = HASH ([policy_header_reference])) AS 						
SELECT
   sourceID
  ,policy_header_reference
  ,policy_section_reference
  ,sum(Written_Line) AS Total_Written_Line						
FROM staging.line
WHERE lakeisactive = 1 and lakedeletedtimestamp is null				
AND sourceID = 	40
GROUP BY sourceID, policy_header_reference, policy_section_reference ;
select ''
"""

pd.read_sql_query(LineTotals_test_movements_qry,connDw)


LineFactor_tbl_test_movements_qry = """
SET NOCOUNT ON;
CREATE TABLE etl.LineFactor_tbl_test_movements_rptm
WITH (CLUSTERED COLUMNSTORE INDEX ,DISTRIBUTION = HASH ([policy_header_reference])) AS 							
select ln.sourceID							
, ln.policy_header_reference							
, ln.policy_section_reference							
, ln.Business_Entity_Code							
, ln.lakeDeletedTimestamp							
, CASE WHEN lt.Total_Written_Line = 0							
    THEN 1 							
    ELSE CAST(ln.Written_Line / lt.Total_Written_Line AS DECIMAL (6,3)) 							
END AS Line_Factor							
FROM staging.line ln							
JOIN etl.LineTotals_test_movements  lt							
ON ln.policy_header_reference = lt.policy_header_reference							
and ln.policy_section_reference = lt.policy_section_reference							
and ln.sourceID = lt.sourceID							
WHERE  lakeisactive = 1 and lakedeletedtimestamp is null				
select ''		
"""
pd.read_sql_query(LineFactor_tbl_test_movements_qry,connDw)


LineTotals_risk_test_qry = """
SET NOCOUNT ON;
    CREATE TABLE etl.LineTotals_risk_test_rptm
            WITH (HEAP,DISTRIBUTION = HASH ([Policy_Header_Reference])) AS 			
                SELECT			
                   sourceID			
                  ,Policy_Header_Reference			
                  ,Policy_Section_Reference			
                  ,sum(Written_Line) AS Total_Written_Line			
                FROM Staging.Line			
                WHERE  lakeisactive = 1 and lakedeletedtimestamp is null
                AND sourceID = 40			
                GROUP BY sourceID, Policy_Header_Reference, Policy_Section_Reference			
select ''		
"""
pd.read_sql_query(LineTotals_risk_test_qry,connDw)


LineFactor_risk_test_qry = """
SET NOCOUNT ON;
         CREATE TABLE etl.LineFactor_risk_test_rptm
            WITH (HEAP,DISTRIBUTION = HASH ([Policy_Header_Reference])) AS 			
                select ln.sourceID			
                    , ln.Policy_Header_Reference			
                    , ln.Policy_Section_Reference			
                    , ln.Business_Entity_Code			
                    , CASE WHEN lt.Total_Written_Line = 0			
                        THEN 1 			
                        ELSE CAST(ln.Written_Line / lt.Total_Written_Line AS DECIMAL(6,3)) 			
                    END AS Line_Factor			
                    , ln.lakeDeletedTimestamp			
                    FROM Staging.Line ln			
                    JOIN etl.LineTotals_risk_test lt			
                    ON ln.Policy_Header_Reference = lt.Policy_Header_Reference			
                    and ln.Policy_Section_Reference = lt.Policy_Section_Reference			
                    and ln.sourceID = lt.SourceID			
                    WHERE  lakeisactive = 1 and lakedeletedtimestamp is null	
                    AND ln.sourceID = 40					
select ''		
"""
pd.read_sql_query(LineFactor_risk_test_qry,connDw)

# COMMAND ----------

# DBTITLE 1,Fact_Bdx_Risk_Premium_Transaction - Helper Query
try:
    qry = """SET NOCOUNT ON;
            DROP TABLE etl.LineTotals_risk_prem ;
            DROP TABLE etl.LineFactor_risk ;
            select '' """
    pd.read_sql_query(qry,connDw)
except:
    print("Exception thrown , tables might not exists to delete")
LineTotals_risk_prem_qry = """
SET NOCOUNT ON;
CREATE TABLE etl.LineTotals_risk_prem
WITH (CLUSTERED COLUMNSTORE INDEX ,DISTRIBUTION = HASH ([policy_header_reference])) AS
SELECT
sourceID
,policy_header_reference
,policy_section_reference
,sum(Written_Line) AS Total_Written_Line
FROM staging.line
WHERE   lakeisactive = 1 and lakedeletedtimestamp is null
AND sourceID = 	40
GROUP BY sourceID, policy_header_reference, policy_section_reference ;
select '';
"""

pd.read_sql_query(LineTotals_risk_prem_qry,connDw)

LineFactor_risk_qry = """
SET NOCOUNT ON;
CREATE TABLE etl.LineFactor_risk
WITH (CLUSTERED COLUMNSTORE INDEX ,DISTRIBUTION = HASH ([policy_header_reference])) AS
select ln.sourceID
, ln.policy_header_reference
, ln.policy_section_reference
, ln.Business_Entity_Code
, ln.lakeDeletedTimestamp
, CASE WHEN lt.Total_Written_Line = 0
THEN 1
ELSE CAST(ln.Written_Line / lt.Total_Written_Line AS DECIMAL(6,3))
END AS Line_Factor
FROM staging.line ln
JOIN etl.LineTotals_risk_prem  lt
ON ln.policy_header_reference = lt.policy_header_reference
and ln.policy_section_reference = lt.policy_section_reference
and ln.sourceID = lt.sourceID
WHERE lakeisactive = 1 and lakeDeletedTimestamp is null
AND ln.sourceID = 40;
select '';
"""
pd.read_sql_query(LineFactor_risk_qry,connDw)


# COMMAND ----------

# DBTITLE 1,Grain Check
fact_bdx_grain_query = """
SELECT
fct.Fact_Bdx_Claim_Transaction_HBK
, fct.Dwh_Valid_From
, COUNT(1)
FROM dwh.Fact_Bdx_Claim_Transaction fct
WHERE Dwh_Action_Type = 'I'
GROUP BY fct.Fact_Bdx_Claim_Transaction_HBK
, fct.Dwh_Valid_From
HAVING   COUNT(1) > 1

UNION ALL

SELECT
fct.Fact_Bdx_Risk_Premium_Transaction_HBK
, fct.Dwh_Valid_From
, COUNT(1)
FROM dwh.Fact_Bdx_Risk_Premium_Transaction fct
WHERE Dwh_Action_Type = 'I'
GROUP BY fct.Fact_Bdx_Risk_Premium_Transaction_HBK
, fct.Dwh_Valid_From
HAVING   COUNT(1) > 1

UNION ALL

SELECT
fct.Fact_Bdx_Risk_Premium_Transaction_Movements_HBK
, fct.Dwh_Valid_From
, COUNT(1)
FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements fct
WHERE Dwh_Action_Type = 'I'
GROUP BY fct.Fact_Bdx_Risk_Premium_Transaction_Movements_HBK
, fct.Dwh_Valid_From
HAVING   COUNT(1) > 1
"""

pd_df_Fact_bdx_grain_check = pd.read_sql_query(fact_bdx_grain_query,connDw)

if pd_df_Fact_bdx_grain_check.empty:
  print('Output : Grain check is passed !!')
else:
    pd.set_option('display.max_columns', None)
    print('Output : Grain check is failed for below fact Bdx Tables ')
    display(pd_df_Fact_bdx_grain_check)

# COMMAND ----------

# DBTITLE 1,Fact Bdx Count Check 
fact_bdx_count_check_qry = """
select Fact_Count.*,Fact_Stage_Count.Stage_Count,Fact_Count.Fact_Count- Fact_Stage_Count.Stage_Count as Difference_in_Count from 
(select 'fact_bdx_claim' as Fact , COUNT(*) as Stage_Count from
(
select * from [Staging].[Bordereaux_Claim_Movements]
where lakeisactive = 1 and lakedeletedtimestamp is null) a
left join
etl.LineFactor_tbl_test b
on a.policy_Section_reference = b.policy_Section_reference
and a.policy_Section_reference =b.policy_Section_reference

UNION ALL 

select 'fact_bdx_risk' as Fact, COUNT(*) as Stage_Count from
(
select * from [Staging].[Bordereaux_Risk_Premium]
where lakeisactive = 1 and lakedeletedtimestamp is null) a
left join
etl.LineFactor_tbl_test2 b
on a.policy_Section_reference = b.policy_Section_reference
and a.policy_Section_reference  =b.policy_Section_reference

UNION ALL 

select 'fact_bdx_risk_mov' as Fact, COUNT(*) as Stage_Count  from
(
select * from [Staging].[Bordereaux_Risk_Premium_Movements]
where lakeisactive = 1 and lakedeletedtimestamp is null) a
left join
etl.LineFactor_tbl_test_movements b
on a.policy_Section_reference = b.policy_Section_reference
and a.policy_Section_reference  =b.policy_Section_reference
) Fact_Stage_Count ,
 (SELECT 'fact_bdx_Claim' as Fact,COUNT(1) as Fact_Count from (
SELECT Fact_Bdx_Claim_Transaction_HBK, Hash_Value, Dwh_Action_Type, Dwh_Valid_From FROM
(SELECT Fact_Bdx_Claim_Transaction_HBK, Hash_Value ,Dwh_Action_Type,Dwh_Valid_From,
ROW_NUMBER() OVER(PARTITION BY Fact_Bdx_Claim_Transaction_HBK, Dwh_Action_Type ORDER BY Dwh_Valid_From DESC) AS rn
,DENSE_RANK() OVER(PARTITION BY Fact_Bdx_Claim_Transaction_HBK ORDER BY Dwh_Valid_From DESC) AS drn
FROM dwh.Fact_Bdx_Claim_Transaction) fl
WHERE fl.rn = 1 AND fl.drn = 1 AND Dwh_Action_Type = 'I' 
) fact

UNION ALL 

SELECT 'fact_bdx_risk' as Fact,COUNT(1) as Fact_Count from (
SELECT Fact_Bdx_Risk_Premium_Transaction_HBK,Hash_Value,Dwh_Action_Type,Dwh_Valid_From FROM
(SELECT Fact_Bdx_Risk_Premium_Transaction_HBK,Hash_Value,Dwh_Action_Type,Dwh_Valid_From,
ROW_NUMBER() OVER(PARTITION BY Fact_Bdx_Risk_Premium_Transaction_HBK,Dwh_Action_Type ORDER BY Dwh_Valid_From DESC) AS rn
,DENSE_RANK() OVER(PARTITION BY Fact_Bdx_Risk_Premium_Transaction_HBK ORDER BY Dwh_Valid_From DESC) AS drn
FROM dwh.Fact_Bdx_Risk_Premium_Transaction) fl
WHERE fl.rn = 1 AND fl.drn = 1 AND Dwh_Action_Type = 'I' ) fact

UNION ALL 

SELECT 'fact_bdx_risk_mov' as Fact,COUNT(1) as Fact_Count from (
SELECT Fact_Bdx_Risk_Premium_Transaction_Movements_HBK, Hash_Value, Dwh_Action_Type, Dwh_Valid_From
FROM
(SELECT Fact_Bdx_Risk_Premium_Transaction_Movements_HBK, Hash_Value, Dwh_Action_Type, Dwh_Valid_From,
ROW_NUMBER() OVER(PARTITION BY Fact_Bdx_Risk_Premium_Transaction_Movements_HBK, Dwh_Action_Type
ORDER BY Dwh_Valid_From DESC) AS rn
,DENSE_RANK() OVER(PARTITION BY Fact_Bdx_Risk_Premium_Transaction_Movements_HBK ORDER BY Dwh_Valid_From DESC) AS drn
FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements) fl
WHERE fl.rn = 1 AND fl.drn = 1 AND Dwh_Action_Type = 'I') fact
) Fact_Count
where Fact_Stage_Count.Fact=Fact_Count.Fact
and Fact_Stage_Count.Stage_Count <> Fact_Count.Fact_Count
"""

pd_df_fact_bdx_count_check = pd.read_sql_query(fact_bdx_count_check_qry,connDw)

if pd_df_fact_bdx_count_check.empty:
  print('Output : Count check is passed !!')
else:
    pd.set_option('display.max_columns', None)
    print('Output : Count check is failed for below facts')
    display(pd_df_fact_bdx_count_check)

# COMMAND ----------

# DBTITLE 1,Distinct Values Check
fact_bdx_distinct_values_check_qry = """
SELECT '[Bdx_Bordereaux_ID]' AS col , COUNT(DISTINCT  [Bdx_Bordereaux_ID] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Bdx_Bordereaux_ID]	) = 1
UNION ALL SELECT '[Bdx_Bordereaux_Original_ID]' AS col , COUNT(DISTINCT  [Bdx_Bordereaux_Original_ID] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction HAVING COUNT(DISTINCT 	[Bdx_Bordereaux_Original_ID]	) = 1
UNION ALL SELECT '[Bdx_Claim_ID]' AS col , COUNT(DISTINCT  [Bdx_Claim_ID] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Bdx_Claim_ID]	) = 1
UNION ALL SELECT '[Bdx_Claim_Original_ID]' AS col , COUNT(DISTINCT  [Bdx_Claim_Original_ID] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Bdx_Claim_Original_ID]	) = 1
UNION ALL SELECT '[Bdx_Declaration_ID]' AS col , COUNT(DISTINCT  [Bdx_Declaration_ID] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Bdx_Declaration_ID]	) = 1
UNION ALL SELECT '[Bdx_Declaration_Original_ID]' AS col , COUNT(DISTINCT  [Bdx_Declaration_Original_ID] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Bdx_Declaration_Original_ID]	) = 1
UNION ALL SELECT '[Policy_Section_ID]' AS col , COUNT(DISTINCT  [Policy_Section_ID] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Policy_Section_ID]	) = 1
UNION ALL SELECT '[Policy_Section_Original_ID]' AS col , COUNT(DISTINCT  [Policy_Section_Original_ID] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Policy_Section_Original_ID]	) = 1
UNION ALL SELECT '[Policy_Header_ID]' AS col , COUNT(DISTINCT  [Policy_Header_ID] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Policy_Header_ID]	) = 1
UNION ALL SELECT '[Policy_Header_Original_ID]' AS col , COUNT(DISTINCT  [Policy_Header_Original_ID] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Policy_Header_Original_ID]	) = 1
UNION ALL SELECT '[Lloyds_Risk_Code_ID]' AS col , COUNT(DISTINCT  [Lloyds_Risk_Code_ID] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Lloyds_Risk_Code_ID]	) = 1
UNION ALL SELECT '[Lloyds_Risk_Code_Original_ID]' AS col , COUNT(DISTINCT  [Lloyds_Risk_Code_Original_ID] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Lloyds_Risk_Code_Original_ID]	) = 1
UNION ALL SELECT '[Business_Classification_ID]' AS col , COUNT(DISTINCT  [Business_Classification_ID] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Business_Classification_ID]	) = 1
UNION ALL SELECT '[Business_Classification_Original_ID]' AS col , COUNT(DISTINCT  [Business_Classification_Original_ID] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Business_Classification_Original_ID]	) = 1
UNION ALL SELECT '[Line_ID]' AS col , COUNT(DISTINCT  [Line_ID] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Line_ID]	) = 1
UNION ALL SELECT '[Line_Original_ID]' AS col , COUNT(DISTINCT  [Line_Original_ID] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Line_Original_ID]	) = 1
UNION ALL SELECT '[Business_Entity_ID]' AS col , COUNT(DISTINCT  [Business_Entity_ID] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Business_Entity_ID]	) = 1
UNION ALL SELECT '[Business_Entity_Original_ID]' AS col , COUNT(DISTINCT  [Business_Entity_Original_ID] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Business_Entity_Original_ID]	) = 1
UNION ALL SELECT '[Business_Entity_ID_Group]' AS col , COUNT(DISTINCT  [Business_Entity_ID_Group] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Business_Entity_ID_Group]	) = 1
UNION ALL SELECT '[Business_Entity_Original_ID_Group]' AS col , COUNT(DISTINCT  [Business_Entity_Original_ID_Group] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Business_Entity_Original_ID_Group]	) = 1
UNION ALL SELECT '[Business_Entity_ID_Finance_Group]' AS col , COUNT(DISTINCT  [Business_Entity_ID_Finance_Group] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Business_Entity_ID_Finance_Group]	) = 1
UNION ALL SELECT '[Business_Entity_Original_ID_Finance_Group]' AS col , COUNT(DISTINCT  [Business_Entity_Original_ID_Finance_Group] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Business_Entity_Original_ID_Finance_Group]	) = 1
UNION ALL SELECT '[Business_Entity_ID_Actuarial_Group]' AS col , COUNT(DISTINCT  [Business_Entity_ID_Actuarial_Group] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Business_Entity_ID_Actuarial_Group]	) = 1
UNION ALL SELECT '[Business_Entity_Original_ID_Actuarial_Group]' AS col , COUNT(DISTINCT  [Business_Entity_Original_ID_Actuarial_Group] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Business_Entity_Original_ID_Actuarial_Group]	) = 1
UNION ALL SELECT '[Party_ID_Broker]' AS col , COUNT(DISTINCT  [Party_ID_Broker] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Party_ID_Broker]	) = 1
UNION ALL SELECT '[Party_Original_ID_Broker]' AS col , COUNT(DISTINCT  [Party_Original_ID_Broker] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Party_Original_ID_Broker]	) = 1
UNION ALL SELECT '[Party_ID_Insured]' AS col , COUNT(DISTINCT  [Party_ID_Insured] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Party_ID_Insured]	) = 1
UNION ALL SELECT '[Party_Original_ID_Insured]' AS col , COUNT(DISTINCT  [Party_Original_ID_Insured] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Party_Original_ID_Insured]	) = 1
UNION ALL SELECT '[Location_ID_Risk]' AS col , COUNT(DISTINCT  [Location_ID_Risk] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Location_ID_Risk]	) = 1
UNION ALL SELECT '[Location_Original_ID_Risk]' AS col , COUNT(DISTINCT  [Location_Original_ID_Risk] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Location_Original_ID_Risk]	) = 1
UNION ALL SELECT '[Location_ID_Loss]' AS col , COUNT(DISTINCT  [Location_ID_Loss] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Location_ID_Loss]	) = 1
UNION ALL SELECT '[Location_Original_ID_Loss]' AS col , COUNT(DISTINCT  [Location_Original_ID_Loss] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Location_Original_ID_Loss]	) = 1
UNION ALL SELECT '[Location_ID_Insured]' AS col , COUNT(DISTINCT  [Location_ID_Insured] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Location_ID_Insured]	) = 1
UNION ALL SELECT '[Location_Original_ID_Insured]' AS col , COUNT(DISTINCT  [Location_Original_ID_Insured] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Location_Original_ID_Insured]	) = 1
UNION ALL SELECT '[Location_ID_US_State_Of_Filing]' AS col , COUNT(DISTINCT  [Location_ID_US_State_Of_Filing] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Location_ID_US_State_Of_Filing]	) = 1
UNION ALL SELECT '[Location_Original_ID_US_State_Of_Filing]' AS col , COUNT(DISTINCT  [Location_Original_ID_US_State_Of_Filing] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Location_Original_ID_US_State_Of_Filing]	) = 1
UNION ALL SELECT '[Currency_ID_Settlement]' AS col , COUNT(DISTINCT  [Currency_ID_Settlement] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Currency_ID_Settlement]	) = 1
UNION ALL SELECT '[Currency_Original_ID_Settlement]' AS col , COUNT(DISTINCT  [Currency_Original_ID_Settlement] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Currency_Original_ID_Settlement]	) = 1
UNION ALL SELECT '[Currency_ID_Original]' AS col , COUNT(DISTINCT  [Currency_ID_Original] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Currency_ID_Original]	) = 1
UNION ALL SELECT '[Currency_Original_ID_Original]' AS col , COUNT(DISTINCT  [Currency_Original_ID_Original] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Currency_Original_ID_Original]	) = 1
UNION ALL SELECT '[Currency_ID_Sett_Amount_To_Market]' AS col , COUNT(DISTINCT  [Currency_ID_Sett_Amount_To_Market] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Currency_ID_Sett_Amount_To_Market]	) = 1
UNION ALL SELECT '[Currency_Original_ID_Sett_Amount_To_Market]' AS col , COUNT(DISTINCT  [Currency_Original_ID_Sett_Amount_To_Market] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Currency_Original_ID_Sett_Amount_To_Market]	) = 1
UNION ALL SELECT '[Accounting_Period_ID]' AS col , COUNT(DISTINCT  [Accounting_Period_ID] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Accounting_Period_ID]	) = 1
UNION ALL SELECT '[Accounting_Period_ID_Source]' AS col , COUNT(DISTINCT  [Accounting_Period_ID_Source] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Accounting_Period_ID_Source]	) = 1
UNION ALL SELECT '[Date_ID_Bordereaux_Period]' AS col , COUNT(DISTINCT  [Date_ID_Bordereaux_Period] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Date_ID_Bordereaux_Period]	) = 1
UNION ALL SELECT '[Date_ID_Bordereaux_Period_End]' AS col , COUNT(DISTINCT  [Date_ID_Bordereaux_Period_End] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Date_ID_Bordereaux_Period_End]	) = 1
UNION ALL SELECT '[Date_ID_Claim_Opened]' AS col , COUNT(DISTINCT  [Date_ID_Claim_Opened] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Date_ID_Claim_Opened]	) = 1
UNION ALL SELECT '[Date_ID_Claim_Closed]' AS col , COUNT(DISTINCT  [Date_ID_Claim_Closed] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Date_ID_Claim_Closed]	) = 1
UNION ALL SELECT '[Date_ID_Inception]' AS col , COUNT(DISTINCT  [Date_ID_Inception] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Date_ID_Inception]	) = 1
UNION ALL SELECT '[Date_ID_Claim_Paid]' AS col , COUNT(DISTINCT  [Date_ID_Claim_Paid] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Date_ID_Claim_Paid]	) = 1
UNION ALL SELECT '[Year_ID_Year_Of_Account]' AS col , COUNT(DISTINCT  [Year_ID_Year_Of_Account] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Year_ID_Year_Of_Account]	) = 1
UNION ALL SELECT '[Development_Period_ID_Inception_Date]' AS col , COUNT(DISTINCT  [Development_Period_ID_Inception_Date] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Development_Period_ID_Inception_Date]	) = 1
UNION ALL SELECT '[Development_Period_ID_Year_Of_Account]' AS col , COUNT(DISTINCT  [Development_Period_ID_Year_Of_Account] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Development_Period_ID_Year_Of_Account]	) = 1
UNION ALL SELECT '[Change_This_Month_Fees_OCC]' AS col , COUNT(DISTINCT  [Change_This_Month_Fees_OCC] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Change_This_Month_Fees_OCC]	) = 1
UNION ALL SELECT '[Change_This_Month_Indemity_OCC]' AS col , COUNT(DISTINCT  [Change_This_Month_Indemity_OCC] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Change_This_Month_Indemity_OCC]	) = 1
UNION ALL SELECT '[Paid_This_Month_Adjusters_Fees_OCC]' AS col , COUNT(DISTINCT  [Paid_This_Month_Adjusters_Fees_OCC] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Paid_This_Month_Adjusters_Fees_OCC]	) = 1
UNION ALL SELECT '[Paid_This_Month_Attorney_Coverage_Fees_OCC]' AS col , COUNT(DISTINCT  [Paid_This_Month_Attorney_Coverage_Fees_OCC] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Paid_This_Month_Attorney_Coverage_Fees_OCC]	) = 1
UNION ALL SELECT '[Paid_This_Month_Defence_Fees_OCC]' AS col , COUNT(DISTINCT  [Paid_This_Month_Defence_Fees_OCC] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Paid_This_Month_Defence_Fees_OCC]	) = 1
UNION ALL SELECT '[Paid_This_Month_Expenses_OCC]' AS col , COUNT(DISTINCT  [Paid_This_Month_Expenses_OCC] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Paid_This_Month_Expenses_OCC]	) = 1
UNION ALL SELECT '[Paid_This_Month_TPA_Fees_OCC]' AS col , COUNT(DISTINCT  [Paid_This_Month_TPA_Fees_OCC] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Paid_This_Month_TPA_Fees_OCC]	) = 1
UNION ALL SELECT '[Previously_Paid_Adjusters_Fees_OCC]' AS col , COUNT(DISTINCT  [Previously_Paid_Adjusters_Fees_OCC] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Previously_Paid_Adjusters_Fees_OCC]	) = 1
UNION ALL SELECT '[Previously_Paid_Attorney_Coverage_Fees_OCC]' AS col , COUNT(DISTINCT  [Previously_Paid_Attorney_Coverage_Fees_OCC] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Previously_Paid_Attorney_Coverage_Fees_OCC]	) = 1
UNION ALL SELECT '[Previously_Paid_Defence_Fees_OCC]' AS col , COUNT(DISTINCT  [Previously_Paid_Defence_Fees_OCC] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Previously_Paid_Defence_Fees_OCC]	) = 1
UNION ALL SELECT '[Previously_Paid_Expenses_OCC]' AS col , COUNT(DISTINCT  [Previously_Paid_Expenses_OCC] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Previously_Paid_Expenses_OCC]	) = 1
UNION ALL SELECT '[Previously_Paid_TPA_Fees_OCC]' AS col , COUNT(DISTINCT  [Previously_Paid_TPA_Fees_OCC] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Previously_Paid_TPA_Fees_OCC]	) = 1
UNION ALL SELECT '[Reserve_Adjusters_Fees_OCC]' AS col , COUNT(DISTINCT  [Reserve_Adjusters_Fees_OCC] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Reserve_Adjusters_Fees_OCC]	) = 1
UNION ALL SELECT '[Reserve_Attorney_Coverage_Fees_OCC]' AS col , COUNT(DISTINCT  [Reserve_Attorney_Coverage_Fees_OCC] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Reserve_Attorney_Coverage_Fees_OCC]	) = 1
UNION ALL SELECT '[Reserve_Defence_Fees_OCC]' AS col , COUNT(DISTINCT  [Reserve_Defence_Fees_OCC] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Reserve_Defence_Fees_OCC]	) = 1
UNION ALL SELECT '[Reserve_Expenses_OCC]' AS col , COUNT(DISTINCT  [Reserve_Expenses_OCC] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Reserve_Expenses_OCC]	) = 1
UNION ALL SELECT '[Reserve_TPA_Fees_OCC]' AS col , COUNT(DISTINCT  [Reserve_TPA_Fees_OCC] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Reserve_TPA_Fees_OCC]	) = 1
UNION ALL SELECT '[Total_Incurred_OCC]' AS col , COUNT(DISTINCT  [Total_Incurred_OCC] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Total_Incurred_OCC]	) = 1
UNION ALL SELECT '[Sums_Insured_Amount]' AS col , COUNT(DISTINCT  [Sums_Insured_Amount] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Sums_Insured_Amount]	) = 1
UNION ALL SELECT '[Amount_Claimed]' AS col , COUNT(DISTINCT  [Amount_Claimed] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Amount_Claimed]	) = 1
UNION ALL SELECT '[Rate_Of_Exchange]' AS col , COUNT(DISTINCT  [Rate_Of_Exchange] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Rate_Of_Exchange]	) = 1
UNION ALL SELECT '[Paid_This_Month_Fees_EU_VAT_Applied]' AS col , COUNT(DISTINCT  [Paid_This_Month_Fees_EU_VAT_Applied] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Paid_This_Month_Fees_EU_VAT_Applied]	) = 1
UNION ALL SELECT '[Paid_This_Month_Fees_EU_VAT_Amount]' AS col , COUNT(DISTINCT  [Paid_This_Month_Fees_EU_VAT_Amount] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Paid_This_Month_Fees_EU_VAT_Amount]	) = 1
UNION ALL SELECT '[Paid_This_Month_Fees_Exempt_Belgian_VAT]' AS col , COUNT(DISTINCT  [Paid_This_Month_Fees_Exempt_Belgian_VAT] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Paid_This_Month_Fees_Exempt_Belgian_VAT]	) = 1
UNION ALL SELECT '[Paid_This_Month_Fees_No_EU_VAT_Applied]' AS col , COUNT(DISTINCT  [Paid_This_Month_Fees_No_EU_VAT_Applied] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Paid_This_Month_Fees_No_EU_VAT_Applied]	) = 1
UNION ALL SELECT '[Heads_Of_Damage_Past_Economic_Loss]' AS col , COUNT(DISTINCT  [Heads_Of_Damage_Past_Economic_Loss] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Heads_Of_Damage_Past_Economic_Loss]	) = 1
UNION ALL SELECT '[Heads_Of_Damage_Future_Economic_Loss]' AS col , COUNT(DISTINCT  [Heads_Of_Damage_Future_Economic_Loss] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Heads_Of_Damage_Future_Economic_Loss]	) = 1
UNION ALL SELECT '[Heads_Of_Damage_Past_Medical_Hospital]' AS col , COUNT(DISTINCT  [Heads_Of_Damage_Past_Medical_Hospital] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Heads_Of_Damage_Past_Medical_Hospital]	) = 1
UNION ALL SELECT '[Heads_Of_Damage_Future_Medical_Hospital]' AS col , COUNT(DISTINCT  [Heads_Of_Damage_Future_Medical_Hospital] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Heads_Of_Damage_Future_Medical_Hospital]	) = 1
UNION ALL SELECT '[Heads_Of_Damage_Future_Caring_Services]' AS col , COUNT(DISTINCT  [Heads_Of_Damage_Future_Caring_Services] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Heads_Of_Damage_Future_Caring_Services]	) = 1
UNION ALL SELECT '[Heads_Of_Damage_General_Damages]' AS col , COUNT(DISTINCT  [Heads_Of_Damage_General_Damages] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Heads_Of_Damage_General_Damages]	) = 1
UNION ALL SELECT '[Heads_Of_Damage_Interest]' AS col , COUNT(DISTINCT  [Heads_Of_Damage_Interest] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Heads_Of_Damage_Interest]	) = 1
UNION ALL SELECT '[Heads_Of_Damage_Plaintiff_Legal_Costs]' AS col , COUNT(DISTINCT  [Heads_Of_Damage_Plaintiff_Legal_Costs] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Heads_Of_Damage_Plaintiff_Legal_Costs]	) = 1
UNION ALL SELECT '[Heads_Of_Damage_Defendant_Legal_Costs]' AS col , COUNT(DISTINCT  [Heads_Of_Damage_Defendant_Legal_Costs] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Heads_Of_Damage_Defendant_Legal_Costs]	) = 1
UNION ALL SELECT '[Heads_Of_Damage_Investigation_Costs]' AS col , COUNT(DISTINCT  [Heads_Of_Damage_Investigation_Costs] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Heads_Of_Damage_Investigation_Costs]	) = 1
UNION ALL SELECT '[Heads_Of_Damage_Other]' AS col , COUNT(DISTINCT  [Heads_Of_Damage_Other] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Heads_Of_Damage_Other]	) = 1
UNION ALL SELECT '[Number_Of_Vessels]' AS col , COUNT(DISTINCT  [Number_Of_Vessels] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Number_Of_Vessels]	) = 1
UNION ALL SELECT '[Management_Expense]' AS col , COUNT(DISTINCT  [Management_Expense] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Management_Expense]	) = 1
UNION ALL SELECT '[Received_This_Month_Subrogation_Monies]' AS col , COUNT(DISTINCT  [Received_This_Month_Subrogation_Monies] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Received_This_Month_Subrogation_Monies]	) = 1
UNION ALL SELECT '[Paid_This_Month_Subrogation_Fees]' AS col , COUNT(DISTINCT  [Paid_This_Month_Subrogation_Fees] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Paid_This_Month_Subrogation_Fees]	) = 1
UNION ALL SELECT '[Total_Collection_This_Month]' AS col , COUNT(DISTINCT  [Total_Collection_This_Month] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Total_Collection_This_Month]	) = 1
UNION ALL SELECT '[Settlement_Amt_To_Market]' AS col , COUNT(DISTINCT  [Settlement_Amt_To_Market] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Settlement_Amt_To_Market]	) = 1
UNION ALL SELECT '[Received_This_Month_Deductible]' AS col , COUNT(DISTINCT  [Received_This_Month_Deductible] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Received_This_Month_Deductible]	) = 1
UNION ALL SELECT '[GL_ISO_Class_Code]' AS col , COUNT(DISTINCT  [GL_ISO_Class_Code] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[GL_ISO_Class_Code]	) = 1
UNION ALL SELECT '[Paid_This_Month_Fees_OCC]' AS col , COUNT(DISTINCT  [Paid_This_Month_Fees_OCC] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Paid_This_Month_Fees_OCC]	) = 1
UNION ALL SELECT '[Paid_This_Month_Indemnity_OCC]' AS col , COUNT(DISTINCT  [Paid_This_Month_Indemnity_OCC] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Paid_This_Month_Indemnity_OCC]	) = 1
UNION ALL SELECT '[Previously_Paid_Fees_OCC]' AS col , COUNT(DISTINCT  [Previously_Paid_Fees_OCC] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Previously_Paid_Fees_OCC]	) = 1
UNION ALL SELECT '[Previously_Paid_Indemnity_OCC]' AS col , COUNT(DISTINCT  [Previously_Paid_Indemnity_OCC] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Previously_Paid_Indemnity_OCC]	) = 1
UNION ALL SELECT '[Reserve_Fees_OCC]' AS col , COUNT(DISTINCT  [Reserve_Fees_OCC] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Reserve_Fees_OCC]	) = 1
UNION ALL SELECT '[Reserve_Indemnity_OCC]' AS col , COUNT(DISTINCT  [Reserve_Indemnity_OCC] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Reserve_Indemnity_OCC]	) = 1
UNION ALL SELECT '[TotalIncurred_Fees_OCC]' AS col , COUNT(DISTINCT  [TotalIncurred_Fees_OCC] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[TotalIncurred_Fees_OCC]	) = 1
UNION ALL SELECT '[Total_Incurred_Indemnity_OCC]' AS col , COUNT(DISTINCT  [Total_Incurred_Indemnity_OCC] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Total_Incurred_Indemnity_OCC]	) = 1
UNION ALL SELECT '[Previously_Paid_Subrogation_Fees]' AS col , COUNT(DISTINCT  [Previously_Paid_Subrogation_Fees] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Previously_Paid_Subrogation_Fees]	) = 1
UNION ALL SELECT '[Previously_Recovered_Subrogation]' AS col , COUNT(DISTINCT  [Previously_Recovered_Subrogation] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Previously_Recovered_Subrogation]	) = 1
UNION ALL SELECT '[Total_Incurred_Adjusters_Fees]' AS col , COUNT(DISTINCT  [Total_Incurred_Adjusters_Fees] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Total_Incurred_Adjusters_Fees]	) = 1
UNION ALL SELECT '[Total_Incurred_Defence_Fees]' AS col , COUNT(DISTINCT  [Total_Incurred_Defence_Fees] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Total_Incurred_Defence_Fees]	) = 1
UNION ALL SELECT '[Total_Incurred_Recovered_Subrogation]' AS col , COUNT(DISTINCT  [Total_Incurred_Recovered_Subrogation] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Total_Incurred_Recovered_Subrogation]	) = 1
UNION ALL SELECT '[Total_Incurred_Subrogation_Fees]' AS col , COUNT(DISTINCT  [Total_Incurred_Subrogation_Fees] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Total_Incurred_Subrogation_Fees]	) = 1
UNION ALL SELECT '[Total_Incurred_TPA_Fees]' AS col , COUNT(DISTINCT  [Total_Incurred_TPA_Fees] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Total_Incurred_TPA_Fees]	) = 1
UNION ALL SELECT '[Total_Previous_Paid]' AS col , COUNT(DISTINCT  [Total_Previous_Paid] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Total_Previous_Paid]	) = 1
UNION ALL SELECT '[Total_Reserve]' AS col , COUNT(DISTINCT  [Total_Reserve] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Total_Reserve]	) = 1
UNION ALL SELECT '[Total_Subrogation_Incurred]' AS col , COUNT(DISTINCT  [Total_Subrogation_Incurred] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Total_Subrogation_Incurred]	) = 1
UNION ALL SELECT '[Retention]' AS col , COUNT(DISTINCT  [Retention] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Retention]	) = 1
UNION ALL SELECT '[Ultimate_Value]' AS col , COUNT(DISTINCT  [Ultimate_Value] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Ultimate_Value]	) = 1
UNION ALL SELECT '[Excess_Underlying]' AS col , COUNT(DISTINCT  [Excess_Underlying] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Excess_Underlying]	) = 1
UNION ALL SELECT '[Monthly_Benefit_Amount]' AS col , COUNT(DISTINCT  [Monthly_Benefit_Amount] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Monthly_Benefit_Amount]	) = 1
UNION ALL SELECT '[Sett_Amount_To_Market]' AS col , COUNT(DISTINCT  [Sett_Amount_To_Market] ) AS cou , 'Fact_Bdx_Claim_Transaction' AS tbl_name FROM   dwh.Fact_Bdx_Claim_Transaction 	 HAVING COUNT(DISTINCT 	[Sett_Amount_To_Market]	) = 1
UNION ALL 
SELECT 'Fact_Bdx_Risk_Premium_Transaction_HBK' AS col, count(DISTINCT Fact_Bdx_Risk_Premium_Transaction_HBK) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Fact_Bdx_Risk_Premium_Transaction_HBK) = 1 UNION ALL
SELECT 'Bdx_Transaction_Type_ID' AS col, count(DISTINCT Bdx_Transaction_Type_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Bdx_Transaction_Type_ID) = 1 UNION ALL
SELECT 'Bdx_Transaction_Type_Original_ID' AS col, count(DISTINCT Bdx_Transaction_Type_Original_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Bdx_Transaction_Type_Original_ID) = 1 UNION ALL
SELECT 'Bdx_Bordereaux_ID' AS col, count(DISTINCT Bdx_Bordereaux_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Bdx_Bordereaux_ID) = 1 UNION ALL
SELECT 'Bdx_Bordereaux_Original_ID' AS col, count(DISTINCT Bdx_Bordereaux_Original_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Bdx_Bordereaux_Original_ID) = 1 UNION ALL
SELECT 'Bdx_Insurable_Interest_ID' AS col, count(DISTINCT Bdx_Insurable_Interest_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Bdx_Insurable_Interest_ID) = 1 UNION ALL
SELECT 'Bdx_Insurable_Interest_Original_ID' AS col, count(DISTINCT Bdx_Insurable_Interest_Original_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Bdx_Insurable_Interest_Original_ID) = 1 UNION ALL
SELECT 'Bdx_Insurable_Interest_Cargo_ID' AS col, count(DISTINCT Bdx_Insurable_Interest_Cargo_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Bdx_Insurable_Interest_Cargo_ID) = 1 UNION ALL
SELECT 'Bdx_Insurable_Interest_Cargo_Original_ID' AS col, count(DISTINCT Bdx_Insurable_Interest_Cargo_Original_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Bdx_Insurable_Interest_Cargo_Original_ID) = 1 UNION ALL
SELECT 'Bdx_Insurable_Interest_Characteristics_ID' AS col, count(DISTINCT Bdx_Insurable_Interest_Characteristics_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Bdx_Insurable_Interest_Characteristics_ID) = 1 UNION ALL
SELECT 'Bdx_Insurable_Interest_Characteristics_Original_ID' AS col, count(DISTINCT Bdx_Insurable_Interest_Characteristics_Original_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Bdx_Insurable_Interest_Characteristics_Original_ID) = 1 UNION ALL
SELECT 'Bdx_Insurable_Interest_Classification_ID' AS col, count(DISTINCT Bdx_Insurable_Interest_Classification_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Bdx_Insurable_Interest_Classification_ID) = 1 UNION ALL
SELECT 'Bdx_Insurable_Interest_Classification_Original_ID' AS col, count(DISTINCT Bdx_Insurable_Interest_Classification_Original_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Bdx_Insurable_Interest_Classification_Original_ID) = 1 UNION ALL
SELECT 'Bdx_Insurable_Interest_Liability_ID' AS col, count(DISTINCT Bdx_Insurable_Interest_Liability_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Bdx_Insurable_Interest_Liability_ID) = 1 UNION ALL
SELECT 'Bdx_Insurable_Interest_Liability_Original_ID' AS col, count(DISTINCT Bdx_Insurable_Interest_Liability_Original_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Bdx_Insurable_Interest_Liability_Original_ID) = 1 UNION ALL
SELECT 'Bdx_Insurable_Interest_Limit_ID' AS col, count(DISTINCT Bdx_Insurable_Interest_Limit_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Bdx_Insurable_Interest_Limit_ID) = 1 UNION ALL
SELECT 'Bdx_Insurable_Interest_Limit_Original_ID' AS col, count(DISTINCT Bdx_Insurable_Interest_Limit_Original_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Bdx_Insurable_Interest_Limit_Original_ID) = 1 UNION ALL
SELECT 'Bdx_Insurable_Interest_Motor_ID' AS col, count(DISTINCT Bdx_Insurable_Interest_Motor_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Bdx_Insurable_Interest_Motor_ID) = 1 UNION ALL
SELECT 'Bdx_Insurable_Interest_Motor_Original_ID' AS col, count(DISTINCT Bdx_Insurable_Interest_Motor_Original_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Bdx_Insurable_Interest_Motor_Original_ID) = 1 UNION ALL
SELECT 'Bdx_Insurable_Interest_Property_ID' AS col, count(DISTINCT Bdx_Insurable_Interest_Property_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Bdx_Insurable_Interest_Property_ID) = 1 UNION ALL
SELECT 'Bdx_Insurable_Interest_Property_Original_ID' AS col, count(DISTINCT Bdx_Insurable_Interest_Property_Original_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Bdx_Insurable_Interest_Property_Original_ID) = 1 UNION ALL
SELECT 'Policy_Section_ID' AS col, count(DISTINCT Policy_Section_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Policy_Section_ID) = 1 UNION ALL
SELECT 'Policy_Section_Original_ID' AS col, count(DISTINCT Policy_Section_Original_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Policy_Section_Original_ID) = 1 UNION ALL
SELECT 'Policy_Header_ID' AS col, count(DISTINCT Policy_Header_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Policy_Header_ID) = 1 UNION ALL
SELECT 'Policy_Header_Original_ID' AS col, count(DISTINCT Policy_Header_Original_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Policy_Header_Original_ID) = 1 UNION ALL
SELECT 'Lloyds_Risk_Code_ID' AS col, count(DISTINCT Lloyds_Risk_Code_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Lloyds_Risk_Code_ID) = 1 UNION ALL
SELECT 'Lloyds_Risk_Code_Original_ID' AS col, count(DISTINCT Lloyds_Risk_Code_Original_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Lloyds_Risk_Code_Original_ID) = 1 UNION ALL
SELECT 'Business_Classification_ID' AS col, count(DISTINCT Business_Classification_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Business_Classification_ID) = 1 UNION ALL
SELECT 'Business_Classification_Original_ID' AS col, count(DISTINCT Business_Classification_Original_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Business_Classification_Original_ID) = 1 UNION ALL
SELECT 'Line_ID' AS col, count(DISTINCT Line_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Line_ID) = 1 UNION ALL
SELECT 'Line_Original_ID' AS col, count(DISTINCT Line_Original_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Line_Original_ID) = 1 UNION ALL
SELECT 'Business_Entity_ID' AS col, count(DISTINCT Business_Entity_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Business_Entity_ID) = 1 UNION ALL
SELECT 'Business_Entity_Original_ID' AS col, count(DISTINCT Business_Entity_Original_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Business_Entity_Original_ID) = 1 UNION ALL
SELECT 'Business_Entity_ID_Group' AS col, count(DISTINCT Business_Entity_ID_Group) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Business_Entity_ID_Group) = 1 UNION ALL
SELECT 'Business_Entity_Original_ID_Group' AS col, count(DISTINCT Business_Entity_Original_ID_Group) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Business_Entity_Original_ID_Group) = 1 UNION ALL
SELECT 'Business_Entity_ID_Finance_Group' AS col, count(DISTINCT Business_Entity_ID_Finance_Group) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Business_Entity_ID_Finance_Group) = 1 UNION ALL
SELECT 'Business_Entity_Original_ID_Finance_Group' AS col, count(DISTINCT Business_Entity_Original_ID_Finance_Group) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Business_Entity_Original_ID_Finance_Group) = 1 UNION ALL
SELECT 'Business_Entity_ID_Actuarial_Group' AS col, count(DISTINCT Business_Entity_ID_Actuarial_Group) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Business_Entity_ID_Actuarial_Group) = 1 UNION ALL
SELECT 'Business_Entity_Original_ID_Actuarial_Group' AS col, count(DISTINCT Business_Entity_Original_ID_Actuarial_Group) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Business_Entity_Original_ID_Actuarial_Group) = 1 UNION ALL
SELECT 'Party_ID_Broker' AS col, count(DISTINCT Party_ID_Broker) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Party_ID_Broker) = 1 UNION ALL
SELECT 'Party_Original_ID_Broker' AS col, count(DISTINCT Party_Original_ID_Broker) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Party_Original_ID_Broker) = 1 UNION ALL
SELECT 'Party_ID_Insured' AS col, count(DISTINCT Party_ID_Insured) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Party_ID_Insured) = 1 UNION ALL
SELECT 'Party_Original_ID_Insured' AS col, count(DISTINCT Party_Original_ID_Insured) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Party_Original_ID_Insured) = 1 UNION ALL
SELECT 'Location_ID_Risk' AS col, count(DISTINCT Location_ID_Risk) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Location_ID_Risk) = 1 UNION ALL
SELECT 'Location_Original_ID_Risk' AS col, count(DISTINCT Location_Original_ID_Risk) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Location_Original_ID_Risk) = 1 UNION ALL
SELECT 'Location_ID_Insured' AS col, count(DISTINCT Location_ID_Insured) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Location_ID_Insured) = 1 UNION ALL
SELECT 'Location_Original_ID_Insured' AS col, count(DISTINCT Location_Original_ID_Insured) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Location_Original_ID_Insured) = 1 UNION ALL
SELECT 'Location_ID_Intermediary_1' AS col, count(DISTINCT Location_ID_Intermediary_1) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Location_ID_Intermediary_1) = 1 UNION ALL
SELECT 'Location_Original_ID_Intermediary_1' AS col, count(DISTINCT Location_Original_ID_Intermediary_1) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Location_Original_ID_Intermediary_1) = 1 UNION ALL
SELECT 'Location_ID_Intermediary_2' AS col, count(DISTINCT Location_ID_Intermediary_2) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Location_ID_Intermediary_2) = 1 UNION ALL
SELECT 'Location_Original_ID_Intermediary_2' AS col, count(DISTINCT Location_Original_ID_Intermediary_2) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Location_Original_ID_Intermediary_2) = 1 UNION ALL
SELECT 'Location_ID_Surplus_Lines_Broker' AS col, count(DISTINCT Location_ID_Surplus_Lines_Broker) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Location_ID_Surplus_Lines_Broker) = 1 UNION ALL
SELECT 'Location_Original_ID_Surplus_Lines_Broker' AS col, count(DISTINCT Location_Original_ID_Surplus_Lines_Broker) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Location_Original_ID_Surplus_Lines_Broker) = 1 UNION ALL
SELECT 'Location_ID_Country_Of_Registration' AS col, count(DISTINCT Location_ID_Country_Of_Registration) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Location_ID_Country_Of_Registration) = 1 UNION ALL
SELECT 'Location_Original_ID_Country_Of_Registration' AS col, count(DISTINCT Location_Original_ID_Country_Of_Registration) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Location_Original_ID_Country_Of_Registration) = 1 UNION ALL
SELECT 'Location_ID_US_State_Of_Filing' AS col, count(DISTINCT Location_ID_US_State_Of_Filing) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Location_ID_US_State_Of_Filing) = 1 UNION ALL
SELECT 'Location_Original_ID_US_State_Of_Filing' AS col, count(DISTINCT Location_Original_ID_US_State_Of_Filing) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Location_Original_ID_US_State_Of_Filing) = 1 UNION ALL
SELECT 'Currency_ID_Settlement' AS col, count(DISTINCT Currency_ID_Settlement) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Currency_ID_Settlement) = 1 UNION ALL
SELECT 'Currency_Original_ID_Settlement' AS col, count(DISTINCT Currency_Original_ID_Settlement) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Currency_Original_ID_Settlement) = 1 UNION ALL
SELECT 'Currency_ID_Original' AS col, count(DISTINCT Currency_ID_Original) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Currency_ID_Original) = 1 UNION ALL
SELECT 'Currency_Original_ID_Original' AS col, count(DISTINCT Currency_Original_ID_Original) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Currency_Original_ID_Original) = 1 UNION ALL
SELECT 'Currency_ID_Sum_Insured' AS col, count(DISTINCT Currency_ID_Sum_Insured) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Currency_ID_Sum_Insured) = 1 UNION ALL
SELECT 'Currency_Original_ID_Sum_Insured' AS col, count(DISTINCT Currency_Original_ID_Sum_Insured) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Currency_Original_ID_Sum_Insured) = 1 UNION ALL
SELECT 'Currency_ID_Deductible' AS col, count(DISTINCT Currency_ID_Deductible) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Currency_ID_Deductible) = 1 UNION ALL
SELECT 'Currency_Original_ID_Deductible' AS col, count(DISTINCT Currency_Original_ID_Deductible) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Currency_Original_ID_Deductible) = 1 UNION ALL
SELECT 'Accounting_Period_ID' AS col, count(DISTINCT Accounting_Period_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Accounting_Period_ID) = 1 UNION ALL
SELECT 'Accounting_Period_ID_Source' AS col, count(DISTINCT Accounting_Period_ID_Source) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Accounting_Period_ID_Source) = 1 UNION ALL
SELECT 'Date_ID_Bordereaux_Period' AS col, count(DISTINCT Date_ID_Bordereaux_Period) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Date_ID_Bordereaux_Period) = 1 UNION ALL
SELECT 'Date_ID_Bordereaux_Period_End' AS col, count(DISTINCT Date_ID_Bordereaux_Period_End) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Date_ID_Bordereaux_Period_End) = 1 UNION ALL
SELECT 'Year_ID_Year_Of_Account' AS col, count(DISTINCT Year_ID_Year_Of_Account) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Year_ID_Year_Of_Account) = 1 UNION ALL
SELECT 'Development_Period_ID_Inception_Date' AS col, count(DISTINCT Development_Period_ID_Inception_Date) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Development_Period_ID_Inception_Date) = 1 UNION ALL
SELECT 'Development_Period_ID_Year_Of_Account' AS col, count(DISTINCT Development_Period_ID_Year_Of_Account) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Development_Period_ID_Year_Of_Account) = 1 UNION ALL
SELECT 'Brokerage_Percent_Of_Gross_Premium' AS col, count(DISTINCT Brokerage_Percent_Of_Gross_Premium) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Brokerage_Percent_Of_Gross_Premium) = 1 UNION ALL
SELECT 'Brokerage_Brokerage_Amount_Original_Currency' AS col, count(DISTINCT Brokerage_Brokerage_Amount_Original_Currency) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Brokerage_Brokerage_Amount_Original_Currency) = 1 UNION ALL
SELECT 'Deductible_Amount' AS col, count(DISTINCT Deductible_Amount) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Deductible_Amount) = 1 UNION ALL
SELECT 'Final_Net_Premium_Percent_For_Order' AS col, count(DISTINCT Final_Net_Premium_Percent_For_Order) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Final_Net_Premium_Percent_For_Order) = 1 UNION ALL
SELECT 'Final_Net_Premium_Brokerage_Amount_SCC' AS col, count(DISTINCT Final_Net_Premium_Brokerage_Amount_SCC) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Final_Net_Premium_Brokerage_Amount_SCC) = 1 UNION ALL
SELECT 'Final_Net_Premium_OCC' AS col, count(DISTINCT Final_Net_Premium_OCC) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Final_Net_Premium_OCC) = 1 UNION ALL
SELECT 'Final_Net_Premium_SCC' AS col, count(DISTINCT Final_Net_Premium_SCC) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Final_Net_Premium_SCC) = 1 UNION ALL
SELECT 'Final_Net_Premium_Rate_Of_Exchange' AS col, count(DISTINCT Final_Net_Premium_Rate_Of_Exchange) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Final_Net_Premium_Rate_Of_Exchange) = 1 UNION ALL
SELECT 'Other_Fees_Or_Deductions_Amount' AS col, count(DISTINCT Other_Fees_Or_Deductions_Amount) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Other_Fees_Or_Deductions_Amount) = 1 UNION ALL
SELECT 'Sum_Insured_Amount' AS col, count(DISTINCT Sum_Insured_Amount) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Sum_Insured_Amount) = 1 UNION ALL
SELECT 'Terrorism_Premium_OCC' AS col, count(DISTINCT Terrorism_Premium_OCC) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Terrorism_Premium_OCC) = 1 UNION ALL
SELECT 'Total_Taxes_And_Levies_OCC' AS col, count(DISTINCT Total_Taxes_And_Levies_OCC) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Total_Taxes_And_Levies_OCC) = 1 UNION ALL
SELECT 'Net_Premium_To_Market_SCC' AS col, count(DISTINCT Net_Premium_To_Market_SCC) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Net_Premium_To_Market_SCC) = 1 UNION ALL
SELECT 'Rate_Of_Exchange_SCC' AS col, count(DISTINCT Rate_Of_Exchange_SCC) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Rate_Of_Exchange_SCC) = 1 UNION ALL
SELECT 'Coverholder_Commission_Amount_For_Whole_Risk_Written_Premium' AS col, count(DISTINCT Coverholder_Commission_Amount_For_Whole_Risk_Written_Premium) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Coverholder_Commission_Amount_For_Whole_Risk_Written_Premium) = 1 UNION ALL
SELECT 'Tax_Amount_For_The_Whole_Risk_Written_Premium' AS col, count(DISTINCT Tax_Amount_For_The_Whole_Risk_Written_Premium) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Tax_Amount_For_The_Whole_Risk_Written_Premium) = 1 UNION ALL
SELECT 'IA_Levy_Total_Gross_Written_Premium_Amount' AS col, count(DISTINCT IA_Levy_Total_Gross_Written_Premium_Amount) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT IA_Levy_Total_Gross_Written_Premium_Amount) = 1 UNION ALL
SELECT 'Estimated_Premium_Income' AS col, count(DISTINCT Estimated_Premium_Income) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Estimated_Premium_Income) = 1 UNION ALL
SELECT 'Market_Brokerage_Amount_For_The_Risk' AS col, count(DISTINCT Market_Brokerage_Amount_For_The_Risk) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Market_Brokerage_Amount_For_The_Risk) = 1 UNION ALL
SELECT 'Management_Expense' AS col, count(DISTINCT Management_Expense) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Management_Expense) = 1 UNION ALL
SELECT 'Net_Premium_To_Underwriters_For_The_Risk' AS col, count(DISTINCT Net_Premium_To_Underwriters_For_The_Risk) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Net_Premium_To_Underwriters_For_The_Risk) = 1 UNION ALL
SELECT 'A_Buildings' AS col, count(DISTINCT A_Buildings) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT A_Buildings) = 1 UNION ALL
SELECT 'C_Contents' AS col, count(DISTINCT C_Contents) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT C_Contents) = 1 UNION ALL
SELECT 'D_Business_Interruption' AS col, count(DISTINCT D_Business_Interruption) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT D_Business_Interruption) = 1 UNION ALL
SELECT 'Insured_Assets' AS col, count(DISTINCT Insured_Assets) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Insured_Assets) = 1 UNION ALL
SELECT 'Rebuilding_Cost' AS col, count(DISTINCT Rebuilding_Cost) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Rebuilding_Cost) = 1 UNION ALL
SELECT 'Building_Excess_Amount' AS col, count(DISTINCT Building_Excess_Amount) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Building_Excess_Amount) = 1 UNION ALL
SELECT 'Building_New_Annual_Premium' AS col, count(DISTINCT Building_New_Annual_Premium) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Building_New_Annual_Premium) = 1 UNION ALL
SELECT 'Building_Transaction_Premium' AS col, count(DISTINCT Building_Transaction_Premium) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Building_Transaction_Premium) = 1 UNION ALL
SELECT 'Building_Alternative_Accommodation_Limit' AS col, count(DISTINCT Building_Alternative_Accommodation_Limit) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Building_Alternative_Accommodation_Limit) = 1 UNION ALL
SELECT 'High_Value_Art' AS col, count(DISTINCT High_Value_Art) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT High_Value_Art) = 1 UNION ALL
SELECT 'Contents_Blanket_Sum_Insured' AS col, count(DISTINCT Contents_Blanket_Sum_Insured) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Contents_Blanket_Sum_Insured) = 1 UNION ALL
SELECT 'Contents_Excess_Sum' AS col, count(DISTINCT Contents_Excess_Sum) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Contents_Excess_Sum) = 1 UNION ALL
SELECT 'Contents_New_Annual_Premium' AS col, count(DISTINCT Contents_New_Annual_Premium) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Contents_New_Annual_Premium) = 1 UNION ALL
SELECT 'Contents_Transaction_Premium' AS col, count(DISTINCT Contents_Transaction_Premium) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Contents_Transaction_Premium) = 1 UNION ALL
SELECT 'Contents_Alternative_Accommodation_Limit' AS col, count(DISTINCT Contents_Alternative_Accommodation_Limit) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Contents_Alternative_Accommodation_Limit) = 1 UNION ALL
SELECT 'Property_Total_Premium_Payable' AS col, count(DISTINCT Property_Total_Premium_Payable) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Property_Total_Premium_Payable) = 1 UNION ALL
SELECT 'Limit_Of_Indemnity' AS col, count(DISTINCT Limit_Of_Indemnity) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Limit_Of_Indemnity) = 1 UNION ALL
SELECT 'Insured_Professional_Fees' AS col, count(DISTINCT Insured_Professional_Fees) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Insured_Professional_Fees) = 1 UNION ALL
SELECT 'Other_Risk_Factor_Value' AS col, count(DISTINCT Other_Risk_Factor_Value) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Other_Risk_Factor_Value) = 1 UNION ALL
SELECT 'Weekly_Accident_And_Illness_Cover_Benefit' AS col, count(DISTINCT Weekly_Accident_And_Illness_Cover_Benefit) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Weekly_Accident_And_Illness_Cover_Benefit) = 1 UNION ALL
SELECT 'Weekly_Business_Expenses_Limit' AS col, count(DISTINCT Weekly_Business_Expenses_Limit) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Weekly_Business_Expenses_Limit) = 1 UNION ALL
SELECT 'Deductible_Or_Excess_Days' AS col, count(DISTINCT Deductible_Or_Excess_Days) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Deductible_Or_Excess_Days) = 1 UNION ALL
SELECT 'Total_Premium_Sum_Of_Instalments' AS col, count(DISTINCT Total_Premium_Sum_Of_Instalments) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Total_Premium_Sum_Of_Instalments) = 1 UNION ALL
SELECT 'Medical_Payment_Premium' AS col, count(DISTINCT Medical_Payment_Premium) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Medical_Payment_Premium) = 1 UNION ALL
SELECT 'Policy_Fee' AS col, count(DISTINCT Policy_Fee) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Policy_Fee) = 1 UNION ALL
SELECT 'B_Other_Structures_100_Percent' AS col, count(DISTINCT B_Other_Structures_100_Percent) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT B_Other_Structures_100_Percent) = 1 UNION ALL
SELECT 'Earthquake_Premium_Amount' AS col, count(DISTINCT Earthquake_Premium_Amount) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Earthquake_Premium_Amount) = 1 UNION ALL
SELECT 'Natural_Disaster_Premium_Amount' AS col, count(DISTINCT Natural_Disaster_Premium_Amount) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Natural_Disaster_Premium_Amount) = 1 UNION ALL
SELECT 'This_Year_As_If_Premium' AS col, count(DISTINCT This_Year_As_If_Premium) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT This_Year_As_If_Premium) = 1 UNION ALL
SELECT 'This_Year_As_If_Premium_New_Business' AS col, count(DISTINCT This_Year_As_If_Premium_New_Business) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT This_Year_As_If_Premium_New_Business) = 1 UNION ALL
SELECT 'This_Year_As_If_Premium_Renewal' AS col, count(DISTINCT This_Year_As_If_Premium_Renewal) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT This_Year_As_If_Premium_Renewal) = 1 UNION ALL
SELECT 'Total_Tax' AS col, count(DISTINCT Total_Tax) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Total_Tax) = 1 UNION ALL
SELECT 'A_Buildings_SI' AS col, count(DISTINCT A_Buildings_SI) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT A_Buildings_SI) = 1 UNION ALL
SELECT 'Total_Sum_Insured_DV' AS col, count(DISTINCT Total_Sum_Insured_DV) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Total_Sum_Insured_DV) = 1 UNION ALL
SELECT 'Fee_Amount' AS col, count(DISTINCT Fee_Amount) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Fee_Amount) = 1 UNION ALL
SELECT 'Coverholder_Reported_Total_Tax_And_Levies' AS col, count(DISTINCT Coverholder_Reported_Total_Tax_And_Levies) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Coverholder_Reported_Total_Tax_And_Levies) = 1 UNION ALL
SELECT 'Coverholder_Reported_Net_Due_To_Insurer' AS col, count(DISTINCT Coverholder_Reported_Net_Due_To_Insurer) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Coverholder_Reported_Net_Due_To_Insurer) = 1 UNION ALL
SELECT 'Total_Taxes_Paid_Locally' AS col, count(DISTINCT Total_Taxes_Paid_Locally) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Total_Taxes_Paid_Locally) = 1 UNION ALL
SELECT 'Market_Taxes' AS col, count(DISTINCT Market_Taxes) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Market_Taxes) = 1 UNION ALL
SELECT 'Total_Fee_Amount' AS col, count(DISTINCT Total_Fee_Amount) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Total_Fee_Amount) = 1 UNION ALL
SELECT 'Amount_Of_Taxable_Written_Premium' AS col, count(DISTINCT Amount_Of_Taxable_Written_Premium) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Amount_Of_Taxable_Written_Premium) = 1 UNION ALL
SELECT 'Local_Sub_Producers_Commission_Amount' AS col, count(DISTINCT Local_Sub_Producers_Commission_Amount) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Local_Sub_Producers_Commission_Amount) = 1 UNION ALL
SELECT 'Transaction_Original_Currency_Accessori_Italy' AS col, count(DISTINCT Transaction_Original_Currency_Accessori_Italy) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Transaction_Original_Currency_Accessori_Italy) = 1 UNION ALL
SELECT 'One_Hundred_Percent_Net_Written_Premium_In_USD' AS col, count(DISTINCT One_Hundred_Percent_Net_Written_Premium_In_USD) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT One_Hundred_Percent_Net_Written_Premium_In_USD) = 1 UNION ALL
SELECT 'Accessori_Written_Amount' AS col, count(DISTINCT Accessori_Written_Amount) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Accessori_Written_Amount) = 1 UNION ALL
SELECT 'Ultimate_Value' AS col, count(DISTINCT Ultimate_Value) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Ultimate_Value) = 1 UNION ALL
SELECT 'Comprehensive_Personal_Liability_Premium_100_Percent' AS col, count(DISTINCT Comprehensive_Personal_Liability_Premium_100_Percent) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Comprehensive_Personal_Liability_Premium_100_Percent) = 1 UNION ALL
SELECT 'Discount_Amount' AS col, count(DISTINCT Discount_Amount) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Discount_Amount) = 1 UNION ALL
SELECT 'Last_Year_As_If_Premium' AS col, count(DISTINCT Last_Year_As_If_Premium) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Last_Year_As_If_Premium) = 1 UNION ALL
SELECT 'Last_Year_As_If_Premium_Renewal' AS col, count(DISTINCT Last_Year_As_If_Premium_Renewal) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Last_Year_As_If_Premium_Renewal) = 1 UNION ALL
SELECT 'Expiring_Year_Net_Prem_Ex_Tax' AS col, count(DISTINCT Expiring_Year_Net_Prem_Ex_Tax) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Expiring_Year_Net_Prem_Ex_Tax) = 1 UNION ALL
SELECT 'Net_Premium_Risk_Code_B5_MD' AS col, count(DISTINCT Net_Premium_Risk_Code_B5_MD) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Net_Premium_Risk_Code_B5_MD) = 1 UNION ALL
SELECT 'Net_Premium_Risk_Code_NA_PL' AS col, count(DISTINCT Net_Premium_Risk_Code_NA_PL) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Net_Premium_Risk_Code_NA_PL) = 1 UNION ALL
SELECT 'Net_Premium_Risk_Code_TU_Terror' AS col, count(DISTINCT Net_Premium_Risk_Code_TU_Terror) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Net_Premium_Risk_Code_TU_Terror) = 1 UNION ALL
SELECT 'Net_Premium_Risk_Code_W3_EL' AS col, count(DISTINCT Net_Premium_Risk_Code_W3_EL) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Net_Premium_Risk_Code_W3_EL) = 1 UNION ALL
SELECT 'Gross_Premium_Risk_Code_B5_MD' AS col, count(DISTINCT Gross_Premium_Risk_Code_B5_MD) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Gross_Premium_Risk_Code_B5_MD) = 1 UNION ALL
SELECT 'Gross_Premium_Risk_Code_NA_PL' AS col, count(DISTINCT Gross_Premium_Risk_Code_NA_PL) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Gross_Premium_Risk_Code_NA_PL) = 1 UNION ALL
SELECT 'Gross_Premium_Risk_Code_TU_Terror' AS col, count(DISTINCT Gross_Premium_Risk_Code_TU_Terror) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Gross_Premium_Risk_Code_TU_Terror) = 1 UNION ALL
SELECT 'Gross_Premium_Risk_Code_W3_EL' AS col, count(DISTINCT Gross_Premium_Risk_Code_W3_EL) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Gross_Premium_Risk_Code_W3_EL) = 1  UNION ALL
SELECT 'Bdx_Declaration_ID' AS col, count(DISTINCT Bdx_Declaration_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Bdx_Declaration_ID) = 1  UNION ALL
SELECT 'Bdx_Declaration_Original_ID' AS col, count(DISTINCT Bdx_Declaration_Original_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction HAVING COUNT(DISTINCT Bdx_Declaration_Original_ID) = 1
UNION ALL
SELECT 'Fact_Bdx_Risk_Premium_Transaction_Movements_HBK' AS COL, COUNT(DISTINCT Fact_Bdx_Risk_Premium_Transaction_Movements_HBK) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Fact_Bdx_Risk_Premium_Transaction_Movements_HBK) = 1 	UNION ALL
SELECT 'Bdx_Transaction_Type_ID' AS COL, COUNT(DISTINCT Bdx_Transaction_Type_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Bdx_Transaction_Type_ID) = 1 	UNION ALL
SELECT 'Bdx_Transaction_Type_Original_ID' AS COL, COUNT(DISTINCT Bdx_Transaction_Type_Original_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Bdx_Transaction_Type_Original_ID) = 1 	UNION ALL
SELECT 'Bdx_Bordereaux_ID' AS COL, COUNT(DISTINCT Bdx_Bordereaux_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Bdx_Bordereaux_ID) = 1 	UNION ALL
SELECT 'Bdx_Bordereaux_Original_ID' AS COL, COUNT(DISTINCT Bdx_Bordereaux_Original_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Bdx_Bordereaux_Original_ID) = 1 	UNION ALL
SELECT 'Bdx_Insurable_Interest_ID' AS COL, COUNT(DISTINCT Bdx_Insurable_Interest_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Bdx_Insurable_Interest_ID) = 1 	UNION ALL
SELECT 'Bdx_Insurable_Interest_Original_ID' AS COL, COUNT(DISTINCT Bdx_Insurable_Interest_Original_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Bdx_Insurable_Interest_Original_ID) = 1 	UNION ALL
SELECT 'Bdx_Insurable_Interest_Cargo_ID' AS COL, COUNT(DISTINCT Bdx_Insurable_Interest_Cargo_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Bdx_Insurable_Interest_Cargo_ID) = 1 	UNION ALL
SELECT 'Bdx_Insurable_Interest_Cargo_Original_ID' AS COL, COUNT(DISTINCT Bdx_Insurable_Interest_Cargo_Original_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Bdx_Insurable_Interest_Cargo_Original_ID) = 1 	UNION ALL
SELECT 'Bdx_Insurable_Interest_Characteristics_ID' AS COL, COUNT(DISTINCT Bdx_Insurable_Interest_Characteristics_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Bdx_Insurable_Interest_Characteristics_ID) = 1 	UNION ALL
SELECT 'Bdx_Insurable_Interest_Characteristics_Original_ID' AS COL, COUNT(DISTINCT Bdx_Insurable_Interest_Characteristics_Original_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Bdx_Insurable_Interest_Characteristics_Original_ID) = 1 	UNION ALL
SELECT 'Bdx_Insurable_Interest_Classification_ID' AS COL, COUNT(DISTINCT Bdx_Insurable_Interest_Classification_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Bdx_Insurable_Interest_Classification_ID) = 1 	UNION ALL
SELECT 'Bdx_Insurable_Interest_Classification_Original_ID' AS COL, COUNT(DISTINCT Bdx_Insurable_Interest_Classification_Original_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Bdx_Insurable_Interest_Classification_Original_ID) = 1 	UNION ALL
SELECT 'Bdx_Insurable_Interest_Liability_ID' AS COL, COUNT(DISTINCT Bdx_Insurable_Interest_Liability_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Bdx_Insurable_Interest_Liability_ID) = 1 	UNION ALL
SELECT 'Bdx_Insurable_Interest_Liability_Original_ID' AS COL, COUNT(DISTINCT Bdx_Insurable_Interest_Liability_Original_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Bdx_Insurable_Interest_Liability_Original_ID) = 1 	UNION ALL
SELECT 'Bdx_Insurable_Interest_Limit_ID' AS COL, COUNT(DISTINCT Bdx_Insurable_Interest_Limit_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Bdx_Insurable_Interest_Limit_ID) = 1 	UNION ALL
SELECT 'Bdx_Insurable_Interest_Limit_Original_ID' AS COL, COUNT(DISTINCT Bdx_Insurable_Interest_Limit_Original_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Bdx_Insurable_Interest_Limit_Original_ID) = 1 	UNION ALL
SELECT 'Bdx_Insurable_Interest_Motor_ID' AS COL, COUNT(DISTINCT Bdx_Insurable_Interest_Motor_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Bdx_Insurable_Interest_Motor_ID) = 1 	UNION ALL
SELECT 'Bdx_Insurable_Interest_Motor_Original_ID' AS COL, COUNT(DISTINCT Bdx_Insurable_Interest_Motor_Original_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Bdx_Insurable_Interest_Motor_Original_ID) = 1 	UNION ALL
SELECT 'Bdx_Insurable_Interest_Property_ID' AS COL, COUNT(DISTINCT Bdx_Insurable_Interest_Property_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Bdx_Insurable_Interest_Property_ID) = 1 	UNION ALL
SELECT 'Bdx_Insurable_Interest_Property_Original_ID' AS COL, COUNT(DISTINCT Bdx_Insurable_Interest_Property_Original_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Bdx_Insurable_Interest_Property_Original_ID) = 1 	UNION ALL
SELECT 'Policy_Section_ID' AS COL, COUNT(DISTINCT Policy_Section_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Policy_Section_ID) = 1 	UNION ALL
SELECT 'Policy_Section_Original_ID' AS COL, COUNT(DISTINCT Policy_Section_Original_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Policy_Section_Original_ID) = 1 	UNION ALL
SELECT 'Policy_Header_ID' AS COL, COUNT(DISTINCT Policy_Header_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Policy_Header_ID) = 1 	UNION ALL
SELECT 'Policy_Header_Original_ID' AS COL, COUNT(DISTINCT Policy_Header_Original_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Policy_Header_Original_ID) = 1 	UNION ALL
SELECT 'Lloyds_Risk_Code_ID' AS COL, COUNT(DISTINCT Lloyds_Risk_Code_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Lloyds_Risk_Code_ID) = 1 	UNION ALL
SELECT 'Lloyds_Risk_Code_Original_ID' AS COL, COUNT(DISTINCT Lloyds_Risk_Code_Original_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Lloyds_Risk_Code_Original_ID) = 1 	UNION ALL
SELECT 'Business_Classification_ID' AS COL, COUNT(DISTINCT Business_Classification_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Business_Classification_ID) = 1 	UNION ALL
SELECT 'Business_Classification_Original_ID' AS COL, COUNT(DISTINCT Business_Classification_Original_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Business_Classification_Original_ID) = 1 	UNION ALL
SELECT 'Line_ID' AS COL, COUNT(DISTINCT Line_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Line_ID) = 1 	UNION ALL
SELECT 'Line_Original_ID' AS COL, COUNT(DISTINCT Line_Original_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Line_Original_ID) = 1 	UNION ALL
SELECT 'Business_Entity_ID' AS COL, COUNT(DISTINCT Business_Entity_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Business_Entity_ID) = 1 	UNION ALL
SELECT 'Business_Entity_Original_ID' AS COL, COUNT(DISTINCT Business_Entity_Original_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Business_Entity_Original_ID) = 1 	UNION ALL
SELECT 'Business_Entity_ID_Group' AS COL, COUNT(DISTINCT Business_Entity_ID_Group) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Business_Entity_ID_Group) = 1 	UNION ALL
SELECT 'Business_Entity_Original_ID_Group' AS COL, COUNT(DISTINCT Business_Entity_Original_ID_Group) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Business_Entity_Original_ID_Group) = 1 	UNION ALL
SELECT 'Business_Entity_ID_Finance_Group' AS COL, COUNT(DISTINCT Business_Entity_ID_Finance_Group) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Business_Entity_ID_Finance_Group) = 1 	UNION ALL
SELECT 'Business_Entity_Original_ID_Finance_Group' AS COL, COUNT(DISTINCT Business_Entity_Original_ID_Finance_Group) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Business_Entity_Original_ID_Finance_Group) = 1 	UNION ALL
SELECT 'Business_Entity_ID_Actuarial_Group' AS COL, COUNT(DISTINCT Business_Entity_ID_Actuarial_Group) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Business_Entity_ID_Actuarial_Group) = 1 	UNION ALL
SELECT 'Business_Entity_Original_ID_Actuarial_Group' AS COL, COUNT(DISTINCT Business_Entity_Original_ID_Actuarial_Group) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Business_Entity_Original_ID_Actuarial_Group) = 1 	UNION ALL
SELECT 'Party_ID_Broker' AS COL, COUNT(DISTINCT Party_ID_Broker) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Party_ID_Broker) = 1 	UNION ALL
SELECT 'Party_Original_ID_Broker' AS COL, COUNT(DISTINCT Party_Original_ID_Broker) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Party_Original_ID_Broker) = 1 	UNION ALL
SELECT 'Party_ID_Insured' AS COL, COUNT(DISTINCT Party_ID_Insured) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Party_ID_Insured) = 1 	UNION ALL
SELECT 'Party_Original_ID_Insured' AS COL, COUNT(DISTINCT Party_Original_ID_Insured) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Party_Original_ID_Insured) = 1 	UNION ALL
SELECT 'Location_ID_Risk' AS COL, COUNT(DISTINCT Location_ID_Risk) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Location_ID_Risk) = 1 	UNION ALL
SELECT 'Location_Original_ID_Risk' AS COL, COUNT(DISTINCT Location_Original_ID_Risk) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Location_Original_ID_Risk) = 1 	UNION ALL
SELECT 'Location_ID_Insured' AS COL, COUNT(DISTINCT Location_ID_Insured) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Location_ID_Insured) = 1 	UNION ALL
SELECT 'Location_Original_ID_Insured' AS COL, COUNT(DISTINCT Location_Original_ID_Insured) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Location_Original_ID_Insured) = 1 	UNION ALL
SELECT 'Location_ID_Intermediary_1' AS COL, COUNT(DISTINCT Location_ID_Intermediary_1) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Location_ID_Intermediary_1) = 1 	UNION ALL
SELECT 'Location_Original_ID_Intermediary_1' AS COL, COUNT(DISTINCT Location_Original_ID_Intermediary_1) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Location_Original_ID_Intermediary_1) = 1 	UNION ALL
SELECT 'Location_ID_Intermediary_2' AS COL, COUNT(DISTINCT Location_ID_Intermediary_2) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Location_ID_Intermediary_2) = 1 	UNION ALL
SELECT 'Location_Original_ID_Intermediary_2' AS COL, COUNT(DISTINCT Location_Original_ID_Intermediary_2) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Location_Original_ID_Intermediary_2) = 1 	UNION ALL
SELECT 'Location_ID_Surplus_Lines_Broker' AS COL, COUNT(DISTINCT Location_ID_Surplus_Lines_Broker) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Location_ID_Surplus_Lines_Broker) = 1 	UNION ALL
SELECT 'Location_Original_ID_Surplus_Lines_Broker' AS COL, COUNT(DISTINCT Location_Original_ID_Surplus_Lines_Broker) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Location_Original_ID_Surplus_Lines_Broker) = 1 	UNION ALL
SELECT 'Location_ID_Country_Of_Registration' AS COL, COUNT(DISTINCT Location_ID_Country_Of_Registration) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Location_ID_Country_Of_Registration) = 1 	UNION ALL
SELECT 'Location_Original_ID_Country_Of_Registration' AS COL, COUNT(DISTINCT Location_Original_ID_Country_Of_Registration) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Location_Original_ID_Country_Of_Registration) = 1 	UNION ALL
SELECT 'Location_ID_US_State_Of_Filing' AS COL, COUNT(DISTINCT Location_ID_US_State_Of_Filing) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Location_ID_US_State_Of_Filing) = 1 	UNION ALL
SELECT 'Location_Original_ID_US_State_Of_Filing' AS COL, COUNT(DISTINCT Location_Original_ID_US_State_Of_Filing) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Location_Original_ID_US_State_Of_Filing) = 1 	UNION ALL
SELECT 'Currency_ID_Settlement' AS COL, COUNT(DISTINCT Currency_ID_Settlement) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Currency_ID_Settlement) = 1 	UNION ALL
SELECT 'Currency_Original_ID_Settlement' AS COL, COUNT(DISTINCT Currency_Original_ID_Settlement) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Currency_Original_ID_Settlement) = 1 	UNION ALL
SELECT 'Currency_ID_Original' AS COL, COUNT(DISTINCT Currency_ID_Original) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Currency_ID_Original) = 1 	UNION ALL
SELECT 'Currency_Original_ID_Original' AS COL, COUNT(DISTINCT Currency_Original_ID_Original) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Currency_Original_ID_Original) = 1 	UNION ALL
SELECT 'Currency_ID_Sum_Insured' AS COL, COUNT(DISTINCT Currency_ID_Sum_Insured) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Currency_ID_Sum_Insured) = 1 	UNION ALL
SELECT 'Currency_Original_ID_Sum_Insured' AS COL, COUNT(DISTINCT Currency_Original_ID_Sum_Insured) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Currency_Original_ID_Sum_Insured) = 1 	UNION ALL
SELECT 'Currency_ID_Deductible' AS COL, COUNT(DISTINCT Currency_ID_Deductible) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Currency_ID_Deductible) = 1 	UNION ALL
SELECT 'Currency_Original_ID_Deductible' AS COL, COUNT(DISTINCT Currency_Original_ID_Deductible) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Currency_Original_ID_Deductible) = 1 	UNION ALL
SELECT 'Accounting_Period_ID' AS COL, COUNT(DISTINCT Accounting_Period_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Accounting_Period_ID) = 1 	UNION ALL
SELECT 'Accounting_Period_ID_Source' AS COL, COUNT(DISTINCT Accounting_Period_ID_Source) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Accounting_Period_ID_Source) = 1 	UNION ALL
SELECT 'Date_ID_Bordereaux_Period' AS COL, COUNT(DISTINCT Date_ID_Bordereaux_Period) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Date_ID_Bordereaux_Period) = 1 	UNION ALL
SELECT 'Date_ID_Bordereaux_Period_End' AS COL, COUNT(DISTINCT Date_ID_Bordereaux_Period_End) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Date_ID_Bordereaux_Period_End) = 1 	UNION ALL
SELECT 'Year_ID_Year_Of_Account' AS COL, COUNT(DISTINCT Year_ID_Year_Of_Account) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Year_ID_Year_Of_Account) = 1 	UNION ALL
SELECT 'Development_Period_ID_Inception_Date' AS COL, COUNT(DISTINCT Development_Period_ID_Inception_Date) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Development_Period_ID_Inception_Date) = 1 	UNION ALL
SELECT 'Development_Period_ID_Year_Of_Account' AS COL, COUNT(DISTINCT Development_Period_ID_Year_Of_Account) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements	 HAVING COUNT( DISTINCT Development_Period_ID_Year_Of_Account) = 1 	UNION ALL
SELECT 'Bdx_Declaration_ID' AS col, count(DISTINCT Bdx_Declaration_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements HAVING COUNT(DISTINCT Bdx_Declaration_ID) = 1  UNION ALL
SELECT 'Bdx_Declaration_Original_ID' AS col, count(DISTINCT Bdx_Declaration_Original_ID) AS cou , 'Fact_Bdx_Risk_Premium_Transaction_Movements' AS tbl_name FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements HAVING COUNT(DISTINCT Bdx_Declaration_Original_ID) = 1

"""


pd_df_fact_bdx_distinct_values_check = pd.read_sql_query(fact_bdx_distinct_values_check_qry,connDw)

if pd_df_fact_bdx_distinct_values_check.empty:
  print('Output : Count check is passed !!')
else:
    pd.set_option('display.max_columns', None)
    print('Output : Distinct Values check is failed for below facts')
    display(pd_df_fact_bdx_distinct_values_check)



# COMMAND ----------

# DBTITLE 1,Fact_Bdx_Claim_Transaction  -  Metric Check - Should not return any output

try:
    qry = """SET NOCOUNT ON;
            DROP TABLE etl.stg_claim_movements_Line_Factor_test ;
            select '' """
    pd.read_sql_query(qry,connDw)
except:
    print("Table etl.stg_claim_movements_Line_Factor_test not exists to delete")


stg_claim_transaction_qry = """
SET NOCOUNT ON;
CREATE TABLE etl.stg_claim_movements_Line_Factor_test
WITH (CLUSTERED COLUMNSTORE INDEX,DISTRIBUTION = HASH (policy_header_reference)) AS
SELECT
coalesce(TransactionDetailsOriginalCurrency_ChangeThisMonth_Fees,0.00) AS Change_This_Month_Fees_OCC
,coalesce(TransactionDetailsOriginalCurrency_ChangeThisMonth_Indemnity,0.00) AS Change_This_Month_Indemity_OCC
,coalesce(TransactionDetailsOriginalCurrency_PaidThisMonth_AdjustersFees,0.00) AS Paid_This_Month_Adjusters_Fees_OCC
,coalesce(TransactionDetailsOriginalCurrency_PaidThisMonth_AttorneyCoverageFees,0.00) AS Paid_This_Month_Attorney_Coverage_Fees_OCC
,coalesce(TransactionDetailsOriginalCurrency_PaidThisMonth_DefenceFees,0.00) AS Paid_This_Month_Defence_Fees_OCC
,coalesce(TransactionDetailsOriginalCurrency_PaidThisMonth_Expenses,0.00) AS Paid_This_Month_Expenses_OCC
,coalesce(TransactionDetailsOriginalCurrency_PaidThisMonth_TPAFees,0.00) AS Paid_This_Month_TPA_Fees_OCC
,coalesce(TransactionDetailsOriginalCurrency_PreviouslyPaid_AdjustersFees,0.00) AS Previously_Paid_Adjusters_Fees_OCC
,coalesce(TransactionDetailsOriginalCurrency_PreviouslyPaid_AttorneyCoverageFees,0.00) AS Previously_Paid_Attorney_Coverage_Fees_OCC
,coalesce(TransactionDetailsOriginalCurrency_PreviouslyPaid_DefenceFees,0.00) AS Previously_Paid_Defence_Fees_OCC
,coalesce(TransactionDetailsOriginalCurrency_PreviouslyPaid_Expenses,0.00) AS Previously_Paid_Expenses_OCC
,coalesce(TransactionDetailsOriginalCurrency_PreviouslyPaid_TPAFees,0.00) AS Previously_Paid_TPA_Fees_OCC
,coalesce(TransactionDetailsOriginalCurrency_Reserve_AdjustersFees,0.00) AS Reserve_Adjusters_Fees_OCC
,coalesce(TransactionDetailsOriginalCurrency_Reserve_AttorneyCoverageFees,0.00) AS Reserve_Attorney_Coverage_Fees_OCC
,coalesce(TransactionDetailsOriginalCurrency_Reserve_DefenceFees,0.00) AS Reserve_Defence_Fees_OCC
,coalesce(TransactionDetailsOriginalCurrency_Reserve_Expenses,0.00) AS Reserve_Expenses_OCC
,coalesce(TransactionDetailsOriginalCurrency_Reserve_TPAFees,0.00) AS Reserve_TPA_Fees_OCC
,coalesce(TransactionDetailsOriginalCurrency_TotalIncurred,0.00) AS Total_Incurred_OCC
,coalesce(RiskDetails_SumsInsuredAmount,0.00) AS Sums_Insured_Amount
,coalesce(FurtherDetails_AmountClaimed,0.00) AS Amount_Claimed
,coalesce(ContractDetails_RateOfExchange,0.00) AS Rate_Of_Exchange
,coalesce(PaidThisMonthFees_EUVATApplied,0.00) AS Paid_This_Month_Fees_EU_VAT_Applied
,coalesce(PaidThisMonthFees_EUVATAmount,0.00) AS Paid_This_Month_Fees_EU_VAT_Amount
,coalesce(PaidThisMonthFees_ExemptBelgianVAT,0.00) AS Paid_This_Month_Fees_Exempt_Belgian_VAT
,coalesce(PaidThisMonthFees_NoEUVATApplied,0.00) AS Paid_This_Month_Fees_No_EU_VAT_Applied
,coalesce(HeadsOfDamage_PastEconomicLoss,0.00) AS Heads_Of_Damage_Past_Economic_Loss
,coalesce(HeadsOfDamage_FutureEconomicLoss,0.00) AS Heads_Of_Damage_Future_Economic_Loss
,coalesce(HeadsOfDamage_PastMedicalHospital,0.00) AS Heads_Of_Damage_Past_Medical_Hospital
,coalesce(HeadsOfDamage_FutureMedicalHospital,0.00) AS Heads_Of_Damage_Future_Medical_Hospital
,coalesce(HeadsOfDamage_FutureCaringServices,0.00) AS Heads_Of_Damage_Future_Caring_Services
,coalesce(HeadsOfDamage_GeneralDamages,0.00) AS Heads_Of_Damage_General_Damages
,coalesce(HeadsOfDamage_Interest,0.00) AS Heads_Of_Damage_Interest
,coalesce(HeadsOfDamage_PlaintiffLegalCosts,0.00) AS Heads_Of_Damage_Plaintiff_Legal_Costs
,coalesce(HeadsOfDamage_DefendantLegalCosts,0.00) AS Heads_Of_Damage_Defendant_Legal_Costs
,coalesce(HeadsOfDamage_InvestigationCosts,0.00) AS Heads_Of_Damage_Investigation_Costs
,coalesce(HeadsOfDamage_Other,0.00) AS Heads_Of_Damage_Other
,coalesce(NumberOfVessels,0.00) AS Number_Of_Vessels
,coalesce(ManagementExpense,0.00) AS Management_Expense
,coalesce(SubrogationMoniesReceivedThisMonth,0.00) AS Received_This_Month_Subrogation_Monies
,coalesce(PaidThisMonth_SubrogationFees,0.00) AS Paid_This_Month_Subrogation_Fees
,coalesce(TotalCollectionThisMonth,0.00) AS Total_Collection_This_Month
,coalesce(SettlementAmtToMarket,0.00) AS Settlement_Amt_To_Market
,coalesce(DeductibleReceivedThisMonth,0.00) AS Received_This_Month_Deductible
,coalesce(GLISOClassCode,0.00) AS GL_ISO_Class_Code
,coalesce(TransactionDetailsOriginalCurrency_PaidThisMonth_Fees,0.00) AS Paid_This_Month_Fees_OCC
,coalesce(TransactionDetailsOriginalCurrency_PaidThisMonth_Indemnity,0.00) AS Paid_This_Month_Indemnity_OCC
,coalesce(TransactionDetailsOriginalCurrency_PreviouslyPaid_Fees,0.00) AS Previously_Paid_Fees_OCC
,coalesce(TransactionDetailsOriginalCurrency_PreviouslyPaid_Indemnity,0.00) AS Previously_Paid_Indemnity_OCC
,coalesce(TransactionDetailsOriginalCurrency_Reserve_Fees,0.00) AS Reserve_Fees_OCC
,coalesce(TransactionDetailsOriginalCurrency_Reserve_Indemnity,0.00) AS Reserve_Indemnity_OCC
,coalesce(TransactionDetailsOriginalCurrency_TotalIncurred_Fees,0.00) AS TotalIncurred_Fees_OCC
,coalesce(TransactionDetailsOriginalCurrency_TotalIncurred_Indemnity,0.00) AS Total_Incurred_Indemnity_OCC
,coalesce(PreviouslyPaid_SubrogationFees,0.00) AS Previously_Paid_Subrogation_Fees
,coalesce(PreviouslyRecovered_Subrogation,0.00) AS Previously_Recovered_Subrogation
,coalesce(TotalIncurred_AdjustersFees,0.00) AS Total_Incurred_Adjusters_Fees
,coalesce(TotalIncurred_DefenceFees,0.00) AS Total_Incurred_Defence_Fees
,coalesce(TotalIncurred_RecoveredSubrogation,0.00) AS Total_Incurred_Recovered_Subrogation
,coalesce(TotalIncurred_SubrogationFees,0.00) AS Total_Incurred_Subrogation_Fees
,coalesce(TotalIncurred_TPAFees,0.00) AS Total_Incurred_TPA_Fees
,coalesce(TotalPreviousPaid,0.00) AS Total_Previous_Paid
,coalesce(TotalReserve,0.00) AS Total_Reserve
,coalesce(TotalSubrogationIncurred,0.00) AS Total_Subrogation_Incurred
,coalesce(Retention,0.00) AS Retention
,coalesce(UltimateValue,0.00) AS Ultimate_Value
,coalesce(ExcessUnderlying,0.00) AS Excess_Underlying
,coalesce(MonthlyBenefitAmount,0.00) AS Monthly_Benefit_Amount
,coalesce(SettAmountToMarket,0.00) AS Sett_Amount_To_Market
,stg.policy_header_reference as policy_header_reference
,stg.policy_section_reference as policy_section_reference
,CASE
WHEN lf.lakeDeletedTimestamp IS NOT NULL
THEN NULL
ELSE coalesce(Business_Entity_Code,'-1')
END AS Business_Entity_Code
,CASE
WHEN lf.lakeDeletedTimestamp IS NOT NULL
THEN NULL
ELSE coalesce(Line_Factor,1)
END AS Line_Factor
FROM
( SELECT * FROM [Staging].[Bordereaux_Claim_Movements]
where lakeisactive = 1 and lakedeletedtimestamp is null
) stg
LEFT JOIN
(SELECT
sourceID
,policy_header_reference
,policy_section_reference
,Business_Entity_Code
,Line_Factor
,lakeDeletedTimestamp
FROM etl.LineFactor_tbl_claim) lf
ON stg.policy_header_reference = lf.policy_header_reference
AND stg.policy_section_reference = lf.policy_section_reference ;
select ''
"""

pd.read_sql_query(stg_claim_transaction_qry,connDw)

Fact_Bdx_Claim_Transaction_METRIC = """
SELECT 'Staging' as table_type,
SUM(cast(Change_This_Month_Fees_OCC* Line_Factor AS DECIMAL (28, 2))) AS Change_This_Month_Fees_OCC        
,SUM(cast(Change_This_Month_Indemity_OCC* Line_Factor AS DECIMAL (28, 2))) AS Change_This_Month_Indemity_OCC        
,SUM(cast(Paid_This_Month_Adjusters_Fees_OCC* Line_Factor AS DECIMAL (28, 2))) AS Paid_This_Month_Adjusters_Fees_OCC        
,SUM(cast(Paid_This_Month_Attorney_Coverage_Fees_OCC* Line_Factor AS DECIMAL (28, 2))) AS Paid_This_Month_Attorney_Coverage_Fees_OCC        
,SUM(cast(Paid_This_Month_Defence_Fees_OCC* Line_Factor AS DECIMAL (28, 2))) AS Paid_This_Month_Defence_Fees_OCC        
,SUM(cast(Paid_This_Month_Expenses_OCC* Line_Factor AS DECIMAL (28, 2))) AS Paid_This_Month_Expenses_OCC        
,SUM(cast(Paid_This_Month_TPA_Fees_OCC* Line_Factor AS DECIMAL (28, 2))) AS Paid_This_Month_TPA_Fees_OCC        
,SUM(cast(Previously_Paid_Adjusters_Fees_OCC* Line_Factor AS DECIMAL (28, 2))) AS Previously_Paid_Adjusters_Fees_OCC        
,SUM(cast(Previously_Paid_Attorney_Coverage_Fees_OCC* Line_Factor AS DECIMAL (28, 2))) AS Previously_Paid_Attorney_Coverage_Fees_OCC        
,SUM(cast(Previously_Paid_Defence_Fees_OCC* Line_Factor AS DECIMAL (28, 2))) AS Previously_Paid_Defence_Fees_OCC        
,SUM(cast(Previously_Paid_Expenses_OCC* Line_Factor AS DECIMAL (28, 2))) AS Previously_Paid_Expenses_OCC        
,SUM(cast(Previously_Paid_TPA_Fees_OCC* Line_Factor AS DECIMAL (28, 2))) AS Previously_Paid_TPA_Fees_OCC        
,SUM(cast(Reserve_Adjusters_Fees_OCC* Line_Factor AS DECIMAL (28, 2))) AS Reserve_Adjusters_Fees_OCC        
,SUM(cast(Reserve_Attorney_Coverage_Fees_OCC* Line_Factor AS DECIMAL (28, 2))) AS Reserve_Attorney_Coverage_Fees_OCC        
,SUM(cast(Reserve_Defence_Fees_OCC* Line_Factor AS DECIMAL (28, 2))) AS Reserve_Defence_Fees_OCC        
,SUM(cast(Reserve_Expenses_OCC* Line_Factor AS DECIMAL (28, 2))) AS Reserve_Expenses_OCC        
,SUM(cast(Reserve_TPA_Fees_OCC* Line_Factor AS DECIMAL (28, 2))) AS Reserve_TPA_Fees_OCC        
,SUM(cast(Total_Incurred_OCC* Line_Factor AS DECIMAL (28, 2))) AS Total_Incurred_OCC        
,SUM(cast(Sums_Insured_Amount* Line_Factor AS DECIMAL (28, 2))) AS Sums_Insured_Amount        
,SUM(cast(Amount_Claimed* Line_Factor AS DECIMAL (28, 2))) AS Amount_Claimed        
,SUM(cast(Rate_Of_Exchange* Line_Factor AS DECIMAL (28, 2))) AS Rate_Of_Exchange        
,SUM(cast(Paid_This_Month_Fees_EU_VAT_Applied* Line_Factor AS DECIMAL (28, 2))) AS Paid_This_Month_Fees_EU_VAT_Applied        
,SUM(cast(Paid_This_Month_Fees_EU_VAT_Amount* Line_Factor AS DECIMAL (28, 2))) AS Paid_This_Month_Fees_EU_VAT_Amount        
,SUM(cast(Paid_This_Month_Fees_Exempt_Belgian_VAT* Line_Factor AS DECIMAL (28, 2))) AS Paid_This_Month_Fees_Exempt_Belgian_VAT        
,SUM(cast(Paid_This_Month_Fees_No_EU_VAT_Applied* Line_Factor AS DECIMAL (28, 2))) AS Paid_This_Month_Fees_No_EU_VAT_Applied        
,SUM(cast(Heads_Of_Damage_Past_Economic_Loss* Line_Factor AS DECIMAL (28, 2))) AS Heads_Of_Damage_Past_Economic_Loss        
,SUM(cast(Heads_Of_Damage_Future_Economic_Loss* Line_Factor AS DECIMAL (28, 2))) AS Heads_Of_Damage_Future_Economic_Loss        
,SUM(cast(Heads_Of_Damage_Past_Medical_Hospital* Line_Factor AS DECIMAL (28, 2))) AS Heads_Of_Damage_Past_Medical_Hospital        
,SUM(cast(Heads_Of_Damage_Future_Medical_Hospital* Line_Factor AS DECIMAL (28, 2))) AS Heads_Of_Damage_Future_Medical_Hospital        
,SUM(cast(Heads_Of_Damage_Future_Caring_Services* Line_Factor AS DECIMAL (28, 2))) AS Heads_Of_Damage_Future_Caring_Services        
,SUM(cast(Heads_Of_Damage_General_Damages* Line_Factor AS DECIMAL (28, 2))) AS Heads_Of_Damage_General_Damages        
,SUM(cast(Heads_Of_Damage_Interest* Line_Factor AS DECIMAL (28, 2))) AS Heads_Of_Damage_Interest        
,SUM(cast(Heads_Of_Damage_Plaintiff_Legal_Costs* Line_Factor AS DECIMAL (28, 2))) AS Heads_Of_Damage_Plaintiff_Legal_Costs        
,SUM(cast(Heads_Of_Damage_Defendant_Legal_Costs* Line_Factor AS DECIMAL (28, 2))) AS Heads_Of_Damage_Defendant_Legal_Costs        
,SUM(cast(Heads_Of_Damage_Investigation_Costs* Line_Factor AS DECIMAL (28, 2))) AS Heads_Of_Damage_Investigation_Costs        
,SUM(cast(Heads_Of_Damage_Other* Line_Factor AS DECIMAL (28, 2))) AS Heads_Of_Damage_Other        
,SUM(cast(Number_Of_Vessels* Line_Factor AS DECIMAL (28, 2))) AS Number_Of_Vessels        
,SUM(cast(Management_Expense* Line_Factor AS DECIMAL (28, 2))) AS Management_Expense        
,SUM(cast(Received_This_Month_Subrogation_Monies* Line_Factor AS DECIMAL (28, 2)))  AS Received_This_Month_Subrogation_Monies        
,SUM(cast(Paid_This_Month_Subrogation_Fees* Line_Factor AS DECIMAL (28, 2))) AS Paid_This_Month_Subrogation_Fees        
,SUM(cast(Total_Collection_This_Month* Line_Factor AS DECIMAL (28, 2))) AS Total_Collection_This_Month        
,SUM(cast(Settlement_Amt_To_Market* Line_Factor AS DECIMAL (28, 2))) AS Settlement_Amt_To_Market        
,SUM(cast(Received_This_Month_Deductible* Line_Factor AS DECIMAL (28, 2))) AS Received_This_Month_Deductible        
,SUM(cast(GL_ISO_Class_Code* Line_Factor AS DECIMAL (28, 2))) AS GL_ISO_Class_Code        
,SUM(cast(Paid_This_Month_Fees_OCC* Line_Factor AS DECIMAL (28, 2))) AS Paid_This_Month_Fees_OCC        
,SUM(cast(Paid_This_Month_Indemnity_OCC* Line_Factor AS DECIMAL (28, 2))) AS Paid_This_Month_Indemnity_OCC        
,SUM(cast(Previously_Paid_Fees_OCC* Line_Factor AS DECIMAL (28, 2))) AS Previously_Paid_Fees_OCC        
,SUM(cast(Previously_Paid_Indemnity_OCC* Line_Factor AS DECIMAL (28, 2))) AS Previously_Paid_Indemnity_OCC        
,SUM(cast(Reserve_Fees_OCC* Line_Factor AS DECIMAL (28, 2))) AS Reserve_Fees_OCC        
,SUM(cast(Reserve_Indemnity_OCC* Line_Factor AS DECIMAL (28, 2))) AS Reserve_Indemnity_OCC        
,SUM(cast(TotalIncurred_Fees_OCC* Line_Factor AS DECIMAL (28, 2))) AS TotalIncurred_Fees_OCC        
,SUM(cast(Total_Incurred_Indemnity_OCC* Line_Factor AS DECIMAL (28, 2))) AS Total_Incurred_Indemnity_OCC        
,SUM(cast(Previously_Paid_Subrogation_Fees* Line_Factor AS DECIMAL (28, 2))) AS Previously_Paid_Subrogation_Fees        
,SUM(cast(Previously_Recovered_Subrogation* Line_Factor AS DECIMAL (28, 2))) AS Previously_Recovered_Subrogation        
,SUM(cast(Total_Incurred_Adjusters_Fees* Line_Factor AS DECIMAL (28, 2))) AS Total_Incurred_Adjusters_Fees        
,SUM(cast(Total_Incurred_Defence_Fees* Line_Factor AS DECIMAL (28, 2))) AS Total_Incurred_Defence_Fees        
,SUM(cast(Total_Incurred_Recovered_Subrogation* Line_Factor AS DECIMAL (28, 2))) AS Total_Incurred_Recovered_Subrogation        
,SUM(cast(Total_Incurred_Subrogation_Fees* Line_Factor AS DECIMAL (28, 2))) AS Total_Incurred_Subrogation_Fees        
,SUM(cast(Total_Incurred_TPA_Fees* Line_Factor AS DECIMAL (28, 2))) AS Total_Incurred_TPA_Fees        
,SUM(cast(Total_Previous_Paid* Line_Factor AS DECIMAL (28, 2))) AS Total_Previous_Paid        
,SUM(cast(Total_Reserve* Line_Factor AS DECIMAL (28, 2))) AS Total_Reserve        
,SUM(cast(Total_Subrogation_Incurred* Line_Factor AS DECIMAL (28, 2))) AS Total_Subrogation_Incurred        
,SUM(cast(Retention* Line_Factor AS DECIMAL (28, 2))) AS Retention        
,SUM(cast(Ultimate_Value* Line_Factor AS DECIMAL (28, 2))) AS Ultimate_Value        
,SUM(cast(Excess_Underlying* Line_Factor AS DECIMAL (28, 2))) AS Excess_Underlying        
,SUM(cast(Monthly_Benefit_Amount* Line_Factor AS DECIMAL (28, 2))) AS Monthly_Benefit_Amount        
,SUM(cast(Sett_Amount_To_Market* Line_Factor AS DECIMAL (28, 2))) AS Sett_Amount_To_Market         
FROM etl.stg_claim_movements_Line_Factor_test   
     
union all

SELECT 'Fact' as table_type,
SUM(cast(Change_This_Month_Fees_OCC AS DECIMAL (28, 2))) as Change_This_Month_Fees_OCC        
,SUM(cast(Change_This_Month_Indemity_OCC AS DECIMAL (28, 2))) as Change_This_Month_Indemity_OCC        
,SUM(cast(Paid_This_Month_Adjusters_Fees_OCC AS DECIMAL (28, 2))) as Paid_This_Month_Adjusters_Fees_OCC        
,SUM(cast(Paid_This_Month_Attorney_Coverage_Fees_OCC AS DECIMAL (28, 2))) as Paid_This_Month_Attorney_Coverage_Fees_OCC        
,SUM(cast(Paid_This_Month_Defence_Fees_OCC AS DECIMAL (28, 2))) as Paid_This_Month_Defence_Fees_OCC        
,SUM(cast(Paid_This_Month_Expenses_OCC AS DECIMAL (28, 2))) as Paid_This_Month_Expenses_OCC        
,SUM(cast(Paid_This_Month_TPA_Fees_OCC AS DECIMAL (28, 2))) as Paid_This_Month_TPA_Fees_OCC        
,SUM(cast(Previously_Paid_Adjusters_Fees_OCC AS DECIMAL (28, 2))) as Previously_Paid_Adjusters_Fees_OCC        
,SUM(cast(Previously_Paid_Attorney_Coverage_Fees_OCC AS DECIMAL (28, 2))) as Previously_Paid_Attorney_Coverage_Fees_OCC        
,SUM(cast(Previously_Paid_Defence_Fees_OCC AS DECIMAL (28, 2))) as Previously_Paid_Defence_Fees_OCC        
,SUM(cast(Previously_Paid_Expenses_OCC AS DECIMAL (28, 2))) as Previously_Paid_Expenses_OCC        
,SUM(cast(Previously_Paid_TPA_Fees_OCC AS DECIMAL (28, 2))) as Previously_Paid_TPA_Fees_OCC        
,SUM(cast(Reserve_Adjusters_Fees_OCC AS DECIMAL (28, 2))) as Reserve_Adjusters_Fees_OCC        
,SUM(cast(Reserve_Attorney_Coverage_Fees_OCC AS DECIMAL (28, 2))) as Reserve_Attorney_Coverage_Fees_OCC        
,SUM(cast(Reserve_Defence_Fees_OCC AS DECIMAL (28, 2))) as Reserve_Defence_Fees_OCC        
,SUM(cast(Reserve_Expenses_OCC AS DECIMAL (28, 2))) as Reserve_Expenses_OCC        
,SUM(cast(Reserve_TPA_Fees_OCC AS DECIMAL (28, 2))) as Reserve_TPA_Fees_OCC        
,SUM(cast(Total_Incurred_OCC AS DECIMAL (28, 2))) as Total_Incurred_OCC        
,SUM(cast(Sums_Insured_Amount AS DECIMAL (28, 2))) as Sums_Insured_Amount        
,SUM(cast(Amount_Claimed AS DECIMAL (28, 2))) as Amount_Claimed        
,SUM(cast(Rate_Of_Exchange AS DECIMAL (28, 2))) as Rate_Of_Exchange        
,SUM(cast(Paid_This_Month_Fees_EU_VAT_Applied AS DECIMAL (28, 2))) as Paid_This_Month_Fees_EU_VAT_Applied        
,SUM(cast(Paid_This_Month_Fees_EU_VAT_Amount AS DECIMAL (28, 2))) as Paid_This_Month_Fees_EU_VAT_Amount        
,SUM(cast(Paid_This_Month_Fees_Exempt_Belgian_VAT AS DECIMAL (28, 2))) as Paid_This_Month_Fees_Exempt_Belgian_VAT        
,SUM(cast(Paid_This_Month_Fees_No_EU_VAT_Applied AS DECIMAL (28, 2))) as Paid_This_Month_Fees_No_EU_VAT_Applied        
,SUM(cast(Heads_Of_Damage_Past_Economic_Loss AS DECIMAL (28, 2))) as Heads_Of_Damage_Past_Economic_Loss        
,SUM(cast(Heads_Of_Damage_Future_Economic_Loss AS DECIMAL (28, 2))) as Heads_Of_Damage_Future_Economic_Loss        
,SUM(cast(Heads_Of_Damage_Past_Medical_Hospital AS DECIMAL (28, 2))) as Heads_Of_Damage_Past_Medical_Hospital        
,SUM(cast(Heads_Of_Damage_Future_Medical_Hospital AS DECIMAL (28, 2))) as Heads_Of_Damage_Future_Medical_Hospital        
,SUM(cast(Heads_Of_Damage_Future_Caring_Services AS DECIMAL (28, 2))) as Heads_Of_Damage_Future_Caring_Services        
,SUM(cast(Heads_Of_Damage_General_Damages AS DECIMAL (28, 2))) as Heads_Of_Damage_General_Damages        
,SUM(cast(Heads_Of_Damage_Interest AS DECIMAL (28, 2))) as Heads_Of_Damage_Interest        
,SUM(cast(Heads_Of_Damage_Plaintiff_Legal_Costs AS DECIMAL (28, 2))) as Heads_Of_Damage_Plaintiff_Legal_Costs        
,SUM(cast(Heads_Of_Damage_Defendant_Legal_Costs AS DECIMAL (28, 2))) as Heads_Of_Damage_Defendant_Legal_Costs        
,SUM(cast(Heads_Of_Damage_Investigation_Costs AS DECIMAL (28, 2))) as Heads_Of_Damage_Investigation_Costs        
,SUM(cast(Heads_Of_Damage_Other AS DECIMAL (28, 2))) as Heads_Of_Damage_Other        
,SUM(cast(Number_Of_Vessels AS DECIMAL (28, 2))) as Number_Of_Vessels        
,SUM(cast(Management_Expense AS DECIMAL (28, 2))) as Management_Expense        
,SUM(cast(Received_This_Month_Subrogation_Monies AS DECIMAL (28, 2))) as Received_This_Month_Subrogation_Monies        
,SUM(cast(Paid_This_Month_Subrogation_Fees AS DECIMAL (28, 2))) as Paid_This_Month_Subrogation_Fees        
,SUM(cast(Total_Collection_This_Month AS DECIMAL (28, 2))) as Total_Collection_This_Month        
,SUM(cast(Settlement_Amt_To_Market AS DECIMAL (28, 2))) as Settlement_Amt_To_Market        
,SUM(cast(Received_This_Month_Deductible AS DECIMAL (28, 2))) as Received_This_Month_Deductible        
,SUM(cast(GL_ISO_Class_Code AS DECIMAL (28, 2))) as GL_ISO_Class_Code        
,SUM(cast(Paid_This_Month_Fees_OCC AS DECIMAL (28, 2))) as Paid_This_Month_Fees_OCC        
,SUM(cast(Paid_This_Month_Indemnity_OCC AS DECIMAL (28, 2))) as Paid_This_Month_Indemnity_OCC        
,SUM(cast(Previously_Paid_Fees_OCC AS DECIMAL (28, 2))) as Previously_Paid_Fees_OCC        
,SUM(cast(Previously_Paid_Indemnity_OCC AS DECIMAL (28, 2))) as Previously_Paid_Indemnity_OCC        
,SUM(cast(Reserve_Fees_OCC AS DECIMAL (28, 2))) as Reserve_Fees_OCC        
,SUM(cast(Reserve_Indemnity_OCC AS DECIMAL (28, 2))) as Reserve_Indemnity_OCC        
,SUM(cast(TotalIncurred_Fees_OCC AS DECIMAL (28, 2))) as TotalIncurred_Fees_OCC        
,SUM(cast(Total_Incurred_Indemnity_OCC AS DECIMAL (28, 2))) as Total_Incurred_Indemnity_OCC        
,SUM(cast(Previously_Paid_Subrogation_Fees AS DECIMAL (28, 2))) as Previously_Paid_Subrogation_Fees        
,SUM(cast(Previously_Recovered_Subrogation AS DECIMAL (28, 2))) as Previously_Recovered_Subrogation        
,SUM(cast(Total_Incurred_Adjusters_Fees AS DECIMAL (28, 2))) as Total_Incurred_Adjusters_Fees        
,SUM(cast(Total_Incurred_Defence_Fees AS DECIMAL (28, 2))) as Total_Incurred_Defence_Fees        
,SUM(cast(Total_Incurred_Recovered_Subrogation AS DECIMAL (28, 2))) as Total_Incurred_Recovered_Subrogation        
,SUM(cast(Total_Incurred_Subrogation_Fees AS DECIMAL (28, 2))) as Total_Incurred_Subrogation_Fees        
,SUM(cast(Total_Incurred_TPA_Fees AS DECIMAL (28, 2))) as Total_Incurred_TPA_Fees        
,SUM(cast(Total_Previous_Paid AS DECIMAL (28, 2))) as Total_Previous_Paid        
,SUM(cast(Total_Reserve AS DECIMAL (28, 2))) as Total_Reserve        
,SUM(cast(Total_Subrogation_Incurred AS DECIMAL (28, 2))) as Total_Subrogation_Incurred        
,SUM(cast(Retention AS DECIMAL (28, 2))) as Retention        
,SUM(cast(Ultimate_Value AS DECIMAL (28, 2))) as Ultimate_Value        
,SUM(cast(Excess_Underlying AS DECIMAL (28, 2))) as Excess_Underlying        
,SUM(cast(Monthly_Benefit_Amount AS DECIMAL (28, 2))) as Monthly_Benefit_Amount        
,SUM(cast(Sett_Amount_To_Market AS DECIMAL (28, 2))) as Sett_Amount_To_Market        
FROM dwh.Fact_Bdx_Claim_Transaction 							
"""

pd_dataframe_Fact_Bdx_Claim_Transaction = pd.read_sql_query(Fact_Bdx_Claim_Transaction_METRIC, connDw)

df_Bdx_Claim_Transaction = spark.createDataFrame(pd_dataframe_Fact_Bdx_Claim_Transaction)
df_Bdx_Claim_Transaction_count = df_Bdx_Claim_Transaction.drop('table_type').dropDuplicates().count()
if(df_Bdx_Claim_Transaction_count > 1):
    print('Output : Metric check is failed for Fact Bdx_Claim_Transaction ')
    display(df_Bdx_Claim_Transaction)
else:
    print('Output : Metric check is passed for Fact Bdx_Claim_Transaction !!')


# COMMAND ----------

# DBTITLE 1,Fact_Bdx_Risk_Premium_Transaction - Metric Check - Should not return any records 

try:
    qry = """SET NOCOUNT ON;
            DROP TABLE etl.stg_Bordereaux_Risk_Premium_test ;
            DROP TABLE etl.stg_Bordereaux_Risk_Premium_LineFactor_test;
            select '' """
    pd.read_sql_query(qry,connDw)
except:
    print("Tables etl.stg_Bordereaux_Risk_Premium_test, etl.stg_Bordereaux_Risk_Premium_LineFactor_test not exists to delete")


stg_risk_premium_qry = """
SET NOCOUNT ON;
CREATE TABLE etl.stg_Bordereaux_Risk_Premium_test
WITH (CLUSTERED COLUMNSTORE INDEX,DISTRIBUTION = HASH (Policy_Header_Reference)) AS
SELECT CAST(COALESCE(Brokerage_BrokeragePercentOfGrossPremium,0.00) AS DECIMAL(19,8)) AS 	Brokerage_Percent_Of_Gross_Premium
,CAST(COALESCE(Brokerage_BrokerageAmountOriginalCurrency, 0.00) AS DECIMAL(28,2)) AS 	Brokerage_Brokerage_Amount_Original_Currency
,CAST(COALESCE(Deductible_Amount, 0.00) AS DECIMAL(28,2)) AS	Deductible_Amount
,CAST(COALESCE(FinalNetPremium_PercentForOrder, 0.00) AS DECIMAL(19,8)) AS	Final_Net_Premium_Percent_For_Order
,CAST(COALESCE(FinalNetPremium_BrokerageAmountSettlementCurrency, 0.00) AS DECIMAL(28,2)) AS	Final_Net_Premium_Brokerage_Amount_SCC
,CAST(COALESCE(FinalNetPremium_FinalNetPremiumOriginalCurrency, 0.00) AS DECIMAL(28,2)) AS	Final_Net_Premium_OCC
,CAST(COALESCE(FinalNetPremium_FinalNetPremiumSettlementCurrency, 0.00) AS DECIMAL(28,2)) AS	Final_Net_Premium_SCC
,CAST(COALESCE(FinalNetPremium_RateOfExchange, 0.00) AS DECIMAL(28,18)) AS	Final_Net_Premium_Rate_Of_Exchange
,CAST(COALESCE(OtherFeesOrDeductions_Amount, 0.00) AS DECIMAL(28,2)) AS	Other_Fees_Or_Deductions_Amount
,CAST(COALESCE(SumInsured_Amount, 0.00) AS DECIMAL(28,2)) AS	Sum_Insured_Amount
,CAST(COALESCE(Transaction_OriginalCurrency_TerrorismPremium, 0.00) AS DECIMAL(28,2)) AS	Terrorism_Premium_OCC
,CAST(COALESCE(Transaction_OriginalCurrency_TotalTaxesAndLevies, 0.00) AS DECIMAL(28,2)) AS	Total_Taxes_And_Levies_OCC
,CAST(COALESCE(Transaction_SettlementCurrency_NetPremiumToMarket, 0.00) AS DECIMAL(28,2)) AS	Net_Premium_To_Market_SCC
,CAST(COALESCE(Transaction_SettlementCurrency_RateOfExchange, 0.00) AS DECIMAL(28,18)) AS	Rate_Of_Exchange_SCC
,CAST(COALESCE(CoverholderCommissionAmountForWholeRiskWrittenPremium, 0.00) AS DECIMAL(28,2)) AS	Coverholder_Commission_Amount_For_Whole_Risk_Written_Premium
,CAST(COALESCE(TaxAmountForTheWholeRiskWrittenPremium, 0.00) AS DECIMAL(28,2)) AS	Tax_Amount_For_The_Whole_Risk_Written_Premium
,CAST(COALESCE(IALevy_TotalGrossWrittenPremiumAmount, 0.00) AS DECIMAL(28,2)) AS	IA_Levy_Total_Gross_Written_Premium_Amount
,CAST(COALESCE(EstimatedPremiumIncome, 0.00) AS DECIMAL(28,2)) AS	Estimated_Premium_Income
,CAST(COALESCE(MarketBrokerageAmountForTheRisk, 0.00) AS DECIMAL(28,2)) AS	Market_Brokerage_Amount_For_The_Risk
,CAST(COALESCE(ManagementExpense, 0.00) AS DECIMAL(28,2)) AS	Management_Expense
,CAST(COALESCE(NetPremiumToUnderwritersForTheRisk, 0.00) AS DECIMAL(28,2)) AS	Net_Premium_To_Underwriters_For_The_Risk
,CAST(COALESCE(A_Buildings, 0.00) AS DECIMAL(28,2)) AS	A_Buildings
,CAST(COALESCE(C_Contents, 0.00) AS DECIMAL(28,2)) AS	C_Contents
,CAST(COALESCE(D_BusinessInterruption, 0.00) AS DECIMAL(28,2)) AS	D_Business_Interruption
,CAST(COALESCE(InsuredAssets, 0.00) AS DECIMAL(28,2)) AS	Insured_Assets
,CAST(COALESCE(RebuildingCost, 0.00) AS DECIMAL(28,2)) AS	Rebuilding_Cost
,CAST(COALESCE(BuildingExcessAmount, 0.00) AS DECIMAL(28,2)) AS	Building_Excess_Amount
,CAST(COALESCE(BuildingNewAnnualPremium, 0.00) AS DECIMAL(28,2)) AS	Building_New_Annual_Premium
,CAST(COALESCE(BuildingTransactionPremium, 0.00) AS DECIMAL(28,2)) AS	Building_Transaction_Premium
,CAST(COALESCE(BuildingAlternativeAccommodationLimit, 0.00) AS DECIMAL(28,2)) AS	Building_Alternative_Accommodation_Limit
,CAST(COALESCE(HighValueArt, 0.00) AS DECIMAL(28,2)) AS	High_Value_Art
,CAST(COALESCE(ContentsBlanketSumInsured, 0.00) AS DECIMAL(28,2)) AS	Contents_Blanket_Sum_Insured
,CAST(COALESCE(ContentsExcessSum, 0.00) AS DECIMAL(28,2)) AS	Contents_Excess_Sum
,CAST(COALESCE(ContentsNewAnnualPremium, 0.00) AS DECIMAL(28,2)) AS	Contents_New_Annual_Premium
,CAST(COALESCE(ContentsTransactionPremium, 0.00) AS DECIMAL(28,2)) AS	Contents_Transaction_Premium
,CAST(COALESCE(ContentsAlternativeAccommodationLimit, 0.00) AS DECIMAL(28,2)) AS	Contents_Alternative_Accommodation_Limit
,CAST(COALESCE(PropertyTotalPremiumPayable, 0.00) AS DECIMAL(28,2)) AS	Property_Total_Premium_Payable
,CAST(COALESCE(LimitOfIndemnity, 0.00) AS DECIMAL(28,2)) AS	Limit_Of_Indemnity
,CAST(COALESCE(InsuredProfessionalFees, 0.00) AS DECIMAL(28,2)) AS	Insured_Professional_Fees
,CAST(COALESCE(OtherRiskFactorValue, 0.00) AS DECIMAL(28,2)) AS	Other_Risk_Factor_Value
,CAST(COALESCE(WeeklyAccidentAndIllnessCoverBenefit, 0.00) AS DECIMAL(28,2)) AS	Weekly_Accident_And_Illness_Cover_Benefit
,CAST(COALESCE(WeeklyBusinessExpensesLimit, 0.00) AS DECIMAL(28,2)) AS	Weekly_Business_Expenses_Limit
,CAST(COALESCE(DeductibleOrExcessDays, 0.00) AS DECIMAL(28,2)) AS	Deductible_Or_Excess_Days
,CAST(COALESCE(TotalPremium_TotalPremiumSumOfInstalments, 0.00) AS DECIMAL(28,2)) AS	Total_Premium_Sum_Of_Instalments
,CAST(COALESCE(MedicalPaymentPremium, 0.00) AS DECIMAL(28,2)) AS	Medical_Payment_Premium
,CAST(COALESCE(PolicyFee, 0.00) AS DECIMAL(28,2)) AS	Policy_Fee
,CAST(COALESCE(B_OtherStructures100Percent, 0.00) AS DECIMAL(19, 8)) AS	B_Other_Structures_100_Percent
,CAST(COALESCE(EarthquakePremiumAmount, 0.00) AS DECIMAL(28,2)) AS	Earthquake_Premium_Amount
,CAST(COALESCE(NaturalDisasterPremiumAmount, 0.00) AS DECIMAL(28,2)) AS	Natural_Disaster_Premium_Amount
,CAST(COALESCE(ThisYearAsIfPremium, 0.00) AS DECIMAL(28,2)) AS	This_Year_As_If_Premium
,CAST(COALESCE(ThisYearAsIfPremiumNewBusiness, 0.00) AS DECIMAL(28,2)) AS	This_Year_As_If_Premium_New_Business
,CAST(COALESCE(ThisYearAsIfPremiumRenewal, 0.00) AS DECIMAL(28,2)) AS	This_Year_As_If_Premium_Renewal
,CAST(COALESCE(TotalTax, 0.00) AS DECIMAL(28,2)) AS	Total_Tax
,CAST(COALESCE(A_BuildingsSI, 0.00) AS DECIMAL(28,2)) AS	A_Buildings_SI
,CAST(COALESCE(TotalSumInsuredDV, 0.00) AS DECIMAL(28,2)) AS	Total_Sum_Insured_DV
,CAST(COALESCE(FeeAmount, 0.00) AS DECIMAL(28,2)) AS	Fee_Amount
,CAST(COALESCE(CoverholderReported_TotalTaxAndLevies, 0.00) AS DECIMAL(28,2)) AS	Coverholder_Reported_Total_Tax_And_Levies
,CAST(COALESCE(CoverholderReported_NetDueToInsurer, 0.00) AS DECIMAL(28,2)) AS	Coverholder_Reported_Net_Due_To_Insurer
,CAST(COALESCE(TotalTaxesPaidLocally, 0.00) AS DECIMAL(28,2)) AS	Total_Taxes_Paid_Locally
,CAST(COALESCE(MarketTaxes, 0.00) AS DECIMAL(28,2)) AS	Market_Taxes
,CAST(COALESCE(TotalFeeAmount, 0.00) AS DECIMAL(28,2)) AS	Total_Fee_Amount
,CAST(COALESCE(AmountOfTaxableWrittenPremium, 0.00) AS DECIMAL(28,2)) AS	Amount_Of_Taxable_Written_Premium
,CAST(COALESCE(LocalSubProducersCommissionAmount, 0.00) AS DECIMAL(28,2)) AS	Local_Sub_Producers_Commission_Amount
,CAST(COALESCE(Transaction_OriginalCurrency_AccessoriItaly, 0.00) AS DECIMAL(28,2)) AS	Transaction_Original_Currency_Accessori_Italy
,CAST(COALESCE([100PercentNetWrittenPremiumInUSD], 0.00) AS DECIMAL(28,2)) AS	One_Hundred_Percent_Net_Written_Premium_In_USD
,CAST(COALESCE(AccessoriWrittenAmount, 0.00) AS DECIMAL(28,2)) AS	Accessori_Written_Amount
,CAST(COALESCE(UltimateValue, 0.00) AS DECIMAL(28,2)) AS	Ultimate_Value
,CAST(COALESCE(ComprehensivePersonalLiabilityPremium100Percent, 0.00) AS DECIMAL(19,8)) AS	Comprehensive_Personal_Liability_Premium_100_Percent
,CAST(COALESCE(DiscountAmount, 0.00) AS DECIMAL(28,2)) AS	Discount_Amount
,CAST(COALESCE(LastYearAsIfPremium, 0.00) AS DECIMAL(28,2)) AS	Last_Year_As_If_Premium
,CAST(COALESCE(LastYearAsIfPremiumRenewal, 0.00) AS DECIMAL(28,2)) AS	Last_Year_As_If_Premium_Renewal
,CAST(COALESCE(ExpiringYearNetPremExTax, 0.00) AS DECIMAL(28,2)) AS	Expiring_Year_Net_Prem_Ex_Tax
,CAST(COALESCE(NetPremium_RiskCode_B5MD, 0.00) AS DECIMAL(28,2)) AS	Net_Premium_Risk_Code_B5_MD
,CAST(COALESCE(NetPremium_RiskCode_NA_PL, 0.00) AS DECIMAL(28,2)) AS	Net_Premium_Risk_Code_NA_PL
,CAST(COALESCE(NetPremium_RiskCode_TUTerror, 0.00) AS DECIMAL(28,2)) AS	Net_Premium_Risk_Code_TU_Terror
,CAST(COALESCE(NetPremium_RiskCode_W3EL, 0.00) AS DECIMAL(28,2)) AS	Net_Premium_Risk_Code_W3_EL
,CAST(COALESCE(GrossPremium_RiskCode_B5MD, 0.00) AS DECIMAL(28,2)) AS	Gross_Premium_Risk_Code_B5_MD
,CAST(COALESCE(GrossPremium_RiskCode_NA_PL, 0.00) AS DECIMAL(28,2)) AS	Gross_Premium_Risk_Code_NA_PL
,CAST(COALESCE(GrossPremium_RiskCode_TUTerror, 0.00) AS DECIMAL(28,2)) AS	Gross_Premium_Risk_Code_TU_Terror
,CAST(COALESCE(GrossPremium_RiskCode_W3EL, 0.00) AS DECIMAL(28,2)) AS	Gross_Premium_Risk_Code_W3_EL
,SourceID
,Policy_Header_Reference
,Policy_Section_Reference
FROM [Staging].[Bordereaux_Risk_Premium]
WHERE  lakeisactive = 1 and lakedeletedtimestamp is null ;

CREATE TABLE etl.stg_Bordereaux_Risk_Premium_LineFactor_test
WITH (CLUSTERED COLUMNSTORE INDEX,DISTRIBUTION = HASH (Policy_Section_Reference)) AS
SELECT
CASE
WHEN lakeDeletedTimestamp IS NOT NULL
THEN NULL
ELSE coalesce(Business_Entity_Code,'-1')
END AS Business_Entity_Code
,CASE
WHEN lakeDeletedTimestamp IS NOT NULL
THEN NULL
ELSE coalesce(Line_Factor,1)
END AS Line_Factor
, Brokerage_Percent_Of_Gross_Premium
, Brokerage_Brokerage_Amount_Original_Currency
, Deductible_Amount
, Final_Net_Premium_Percent_For_Order
, Final_Net_Premium_Brokerage_Amount_SCC
, Final_Net_Premium_OCC
, Final_Net_Premium_SCC
, Final_Net_Premium_Rate_Of_Exchange
, Other_Fees_Or_Deductions_Amount
, Sum_Insured_Amount
, Terrorism_Premium_OCC
, Total_Taxes_And_Levies_OCC
, Net_Premium_To_Market_SCC
, Rate_Of_Exchange_SCC
, Coverholder_Commission_Amount_For_Whole_Risk_Written_Premium
, Tax_Amount_For_The_Whole_Risk_Written_Premium
, IA_Levy_Total_Gross_Written_Premium_Amount
, Estimated_Premium_Income
, Market_Brokerage_Amount_For_The_Risk
, Management_Expense
, Net_Premium_To_Underwriters_For_The_Risk
, A_Buildings
, C_Contents
, D_Business_Interruption
, Insured_Assets
, Rebuilding_Cost
, Building_Excess_Amount
, Building_New_Annual_Premium
, Building_Transaction_Premium
, Building_Alternative_Accommodation_Limit
, High_Value_Art
, Contents_Blanket_Sum_Insured
, Contents_Excess_Sum
, Contents_New_Annual_Premium
, Contents_Transaction_Premium
, Contents_Alternative_Accommodation_Limit
, Property_Total_Premium_Payable
, Limit_Of_Indemnity
, Insured_Professional_Fees
, Other_Risk_Factor_Value
, Weekly_Accident_And_Illness_Cover_Benefit
, Weekly_Business_Expenses_Limit
, Deductible_Or_Excess_Days
, Total_Premium_Sum_Of_Instalments
, Medical_Payment_Premium
, Policy_Fee
, B_Other_Structures_100_Percent
, Earthquake_Premium_Amount
, Natural_Disaster_Premium_Amount
, This_Year_As_If_Premium
, This_Year_As_If_Premium_New_Business
, This_Year_As_If_Premium_Renewal
, Total_Tax
, A_Buildings_SI
, Total_Sum_Insured_DV
, Fee_Amount
, Coverholder_Reported_Total_Tax_And_Levies
, Coverholder_Reported_Net_Due_To_Insurer
, Total_Taxes_Paid_Locally
, Market_Taxes
, Total_Fee_Amount
, Amount_Of_Taxable_Written_Premium
, Local_Sub_Producers_Commission_Amount
, Transaction_Original_Currency_Accessori_Italy
, One_Hundred_Percent_Net_Written_Premium_In_USD
, Accessori_Written_Amount
, Ultimate_Value
, Comprehensive_Personal_Liability_Premium_100_Percent
, Discount_Amount
, Last_Year_As_If_Premium
, Last_Year_As_If_Premium_Renewal
, Expiring_Year_Net_Prem_Ex_Tax
, Net_Premium_Risk_Code_B5_MD
, Net_Premium_Risk_Code_NA_PL
, Net_Premium_Risk_Code_TU_Terror
, Net_Premium_Risk_Code_W3_EL
, Gross_Premium_Risk_Code_B5_MD
, Gross_Premium_Risk_Code_NA_PL
, Gross_Premium_Risk_Code_TU_Terror
, Gross_Premium_Risk_Code_W3_EL
, stg.Policy_Header_Reference AS Policy_Header_Reference
, stg.Policy_Section_Reference AS Policy_Section_Reference
, stg.SourceID AS SourceID
FROM (
(SELECT Brokerage_Percent_Of_Gross_Premium
, Brokerage_Brokerage_Amount_Original_Currency
, Deductible_Amount
, Final_Net_Premium_Percent_For_Order
, Final_Net_Premium_Brokerage_Amount_SCC
, Final_Net_Premium_OCC
, Final_Net_Premium_SCC
, Final_Net_Premium_Rate_Of_Exchange
, Other_Fees_Or_Deductions_Amount
, Sum_Insured_Amount
, Terrorism_Premium_OCC
, Total_Taxes_And_Levies_OCC
, Net_Premium_To_Market_SCC
, Rate_Of_Exchange_SCC
, Coverholder_Commission_Amount_For_Whole_Risk_Written_Premium
, Tax_Amount_For_The_Whole_Risk_Written_Premium
, IA_Levy_Total_Gross_Written_Premium_Amount
, Estimated_Premium_Income
, Market_Brokerage_Amount_For_The_Risk
, Management_Expense
, Net_Premium_To_Underwriters_For_The_Risk
, A_Buildings
, C_Contents
, D_Business_Interruption
, Insured_Assets
, Rebuilding_Cost
, Building_Excess_Amount
, Building_New_Annual_Premium
, Building_Transaction_Premium
, Building_Alternative_Accommodation_Limit
, High_Value_Art
, Contents_Blanket_Sum_Insured
, Contents_Excess_Sum
, Contents_New_Annual_Premium
, Contents_Transaction_Premium
, Contents_Alternative_Accommodation_Limit
, Property_Total_Premium_Payable
, Limit_Of_Indemnity
, Insured_Professional_Fees
, Other_Risk_Factor_Value
, Weekly_Accident_And_Illness_Cover_Benefit
, Weekly_Business_Expenses_Limit
, Deductible_Or_Excess_Days
, Total_Premium_Sum_Of_Instalments
, Medical_Payment_Premium
, Policy_Fee
, B_Other_Structures_100_Percent
, Earthquake_Premium_Amount
, Natural_Disaster_Premium_Amount
, This_Year_As_If_Premium
, This_Year_As_If_Premium_New_Business
, This_Year_As_If_Premium_Renewal
, Total_Tax
, A_Buildings_SI
, Total_Sum_Insured_DV
, Fee_Amount
, Coverholder_Reported_Total_Tax_And_Levies
, Coverholder_Reported_Net_Due_To_Insurer
, Total_Taxes_Paid_Locally
, Market_Taxes
, Total_Fee_Amount
, Amount_Of_Taxable_Written_Premium
, Local_Sub_Producers_Commission_Amount
, Transaction_Original_Currency_Accessori_Italy
, One_Hundred_Percent_Net_Written_Premium_In_USD
, Accessori_Written_Amount
, Ultimate_Value
, Comprehensive_Personal_Liability_Premium_100_Percent
, Discount_Amount
, Last_Year_As_If_Premium
, Last_Year_As_If_Premium_Renewal
, Expiring_Year_Net_Prem_Ex_Tax
, Net_Premium_Risk_Code_B5_MD
, Net_Premium_Risk_Code_NA_PL
, Net_Premium_Risk_Code_TU_Terror
, Net_Premium_Risk_Code_W3_EL
, Gross_Premium_Risk_Code_B5_MD
, Gross_Premium_Risk_Code_NA_PL
, Gross_Premium_Risk_Code_TU_Terror
, Gross_Premium_Risk_Code_W3_EL
, Policy_Header_Reference
, Policy_Section_Reference
, SourceID
FROM etl.stg_Bordereaux_Risk_Premium_test) stg
LEFT JOIN
(SELECT
SourceID
,Policy_Header_Reference
,Policy_Section_Reference
,Business_Entity_Code
,Line_Factor
,lakeDeletedTimestamp
FROM etl.LineFactor_risk ) lf
ON stg.Policy_Header_Reference = lf.Policy_Header_Reference
AND stg.Policy_Section_Reference = lf.Policy_Section_Reference
AND 40 = lf.SourceID
);
select ''

"""

pd.read_sql_query(stg_risk_premium_qry,connDw)

Fact_Bdx_Risk_Premium_METRIC = """
select           
SUM(CAST( (Brokerage_Percent_Of_Gross_Premium * Line_Factor) AS DECIMAL(19, 8))) AS Brokerage_Percent_Of_Gross_Premium         
, SUM(CAST( Brokerage_Brokerage_Amount_Original_Currency * Line_Factor AS DECIMAL(28,2))) AS  Brokerage_Brokerage_Amount_Original_Currency        
, SUM(CAST( Deductible_Amount * Line_Factor AS DECIMAL(28,2))) AS  Deductible_Amount        
, SUM(CAST( Final_Net_Premium_Percent_For_Order * Line_Factor AS DECIMAL(19,8))) AS  Final_Net_Premium_Percent_For_Order        
, SUM(CAST( Final_Net_Premium_Brokerage_Amount_SCC * Line_Factor AS DECIMAL(28,2))) AS  Final_Net_Premium_Brokerage_Amount_SCC        
, SUM(CAST( Final_Net_Premium_OCC * Line_Factor AS DECIMAL(28,2))) AS  Final_Net_Premium_OCC        
, SUM(CAST( Final_Net_Premium_SCC * Line_Factor AS DECIMAL(28,2))) AS  Final_Net_Premium_SCC        
, SUM(CAST( Final_Net_Premium_Rate_Of_Exchange * Line_Factor AS DECIMAL(28,18))) AS  Final_Net_Premium_Rate_Of_Exchange        
, SUM(CAST( Other_Fees_Or_Deductions_Amount * Line_Factor AS DECIMAL(28,2))) AS  Other_Fees_Or_Deductions_Amount        
, SUM(CAST( Sum_Insured_Amount * Line_Factor AS DECIMAL(28,2))) AS  Sum_Insured_Amount        
, SUM(CAST( Terrorism_Premium_OCC * Line_Factor AS DECIMAL(28,2))) AS  Terrorism_Premium_OCC        
, SUM(CAST( Total_Taxes_And_Levies_OCC * Line_Factor AS DECIMAL(28,2))) AS  Total_Taxes_And_Levies_OCC        
, SUM(CAST( Net_Premium_To_Market_SCC * Line_Factor AS DECIMAL(28,2))) AS  Net_Premium_To_Market_SCC        
, SUM(CAST( Rate_Of_Exchange_SCC * Line_Factor AS DECIMAL(28,18))) AS  Rate_Of_Exchange_SCC        
, SUM(CAST( Coverholder_Commission_Amount_For_Whole_Risk_Written_Premium * Line_Factor AS DECIMAL(28,2))) AS  Coverholder_Commission_Amount_For_Whole_Risk_Written_Premium        
, SUM(CAST( Tax_Amount_For_The_Whole_Risk_Written_Premium * Line_Factor AS DECIMAL(28,2))) AS  Tax_Amount_For_The_Whole_Risk_Written_Premium        
, SUM(CAST( IA_Levy_Total_Gross_Written_Premium_Amount * Line_Factor AS DECIMAL(28,2))) AS  IA_Levy_Total_Gross_Written_Premium_Amount        
, SUM(CAST( Estimated_Premium_Income * Line_Factor AS DECIMAL(28,2))) AS  Estimated_Premium_Income        
, SUM(CAST( Market_Brokerage_Amount_For_The_Risk * Line_Factor AS DECIMAL(28,2))) AS  Market_Brokerage_Amount_For_The_Risk        
, SUM(CAST( Management_Expense * Line_Factor AS DECIMAL(28,2))) AS  Management_Expense        
, SUM(CAST( Net_Premium_To_Underwriters_For_The_Risk * Line_Factor AS DECIMAL(28,2))) AS  Net_Premium_To_Underwriters_For_The_Risk        
, SUM(CAST( A_Buildings * Line_Factor AS DECIMAL(28,2))) AS  A_Buildings        
, SUM(CAST( C_Contents * Line_Factor AS DECIMAL(28,2))) AS  C_Contents        
, SUM(CAST( D_Business_Interruption * Line_Factor AS DECIMAL(28,2))) AS  D_Business_Interruption        
, SUM(CAST( Insured_Assets * Line_Factor AS DECIMAL(28,2))) AS  Insured_Assets        
, SUM(CAST( Rebuilding_Cost * Line_Factor AS DECIMAL(28,2))) AS  Rebuilding_Cost        
, SUM(CAST( Building_Excess_Amount * Line_Factor AS DECIMAL(28,2))) AS  Building_Excess_Amount        
, SUM(CAST( Building_New_Annual_Premium * Line_Factor AS DECIMAL(28,2))) AS  Building_New_Annual_Premium        
, SUM(CAST( Building_Transaction_Premium * Line_Factor AS DECIMAL(28,2))) AS  Building_Transaction_Premium        
, SUM(CAST( Building_Alternative_Accommodation_Limit * Line_Factor AS DECIMAL(28,2))) AS  Building_Alternative_Accommodation_Limit        
, SUM(CAST( High_Value_Art * Line_Factor AS DECIMAL(28,2))) AS  High_Value_Art        
, SUM(CAST( Contents_Blanket_Sum_Insured * Line_Factor AS DECIMAL(28,2))) AS  Contents_Blanket_Sum_Insured        
, SUM(CAST( Contents_Excess_Sum * Line_Factor AS DECIMAL(28,2))) AS  Contents_Excess_Sum        
, SUM(CAST( Contents_New_Annual_Premium * Line_Factor AS DECIMAL(28,2))) AS  Contents_New_Annual_Premium        
, SUM(CAST( Contents_Transaction_Premium * Line_Factor AS DECIMAL(28,2))) AS  Contents_Transaction_Premium        
, SUM(CAST( Contents_Alternative_Accommodation_Limit * Line_Factor AS DECIMAL(28,2))) AS  Contents_Alternative_Accommodation_Limit        
, SUM(CAST( Property_Total_Premium_Payable * Line_Factor AS DECIMAL(28,2))) AS  Property_Total_Premium_Payable        
, SUM(CAST( Limit_Of_Indemnity * Line_Factor AS DECIMAL(28,2))) AS  Limit_Of_Indemnity        
, SUM(CAST( Insured_Professional_Fees * Line_Factor AS DECIMAL(28,2))) AS  Insured_Professional_Fees        
, SUM(CAST( Other_Risk_Factor_Value * Line_Factor AS DECIMAL(28,2))) AS  Other_Risk_Factor_Value        
, SUM(CAST( Weekly_Accident_And_Illness_Cover_Benefit * Line_Factor AS DECIMAL(28,2))) AS  Weekly_Accident_And_Illness_Cover_Benefit        
, SUM(CAST( Weekly_Business_Expenses_Limit * Line_Factor AS DECIMAL(28,2))) AS  Weekly_Business_Expenses_Limit        
, SUM(CAST( Deductible_Or_Excess_Days * Line_Factor AS DECIMAL(28,2))) AS  Deductible_Or_Excess_Days        
, SUM(CAST( Total_Premium_Sum_Of_Instalments * Line_Factor AS DECIMAL(28,2))) AS  Total_Premium_Sum_Of_Instalments        
, SUM(CAST( Medical_Payment_Premium * Line_Factor AS DECIMAL(28,2))) AS  Medical_Payment_Premium        
, SUM(CAST( Policy_Fee * Line_Factor AS DECIMAL(28,2))) AS  Policy_Fee        
, SUM(CAST( B_Other_Structures_100_Percent * Line_Factor AS DECIMAL(19,8))) AS  B_Other_Structures_100_Percent        
, SUM(CAST( Earthquake_Premium_Amount * Line_Factor AS DECIMAL(28,2))) AS  Earthquake_Premium_Amount        
, SUM(CAST( Natural_Disaster_Premium_Amount * Line_Factor AS DECIMAL(28,2))) AS  Natural_Disaster_Premium_Amount        
, SUM(CAST( This_Year_As_If_Premium * Line_Factor AS DECIMAL(28,2))) AS  This_Year_As_If_Premium        
, SUM(CAST( This_Year_As_If_Premium_New_Business * Line_Factor AS DECIMAL(28,2))) AS  This_Year_As_If_Premium_New_Business        
, SUM(CAST( This_Year_As_If_Premium_Renewal * Line_Factor AS DECIMAL(28,2))) AS  This_Year_As_If_Premium_Renewal        
, SUM(CAST( Total_Tax * Line_Factor AS DECIMAL(28,2))) AS  Total_Tax        
, SUM(CAST( A_Buildings_SI * Line_Factor AS DECIMAL(28,2))) AS  A_Buildings_SI        
, SUM(CAST( Total_Sum_Insured_DV * Line_Factor AS DECIMAL(28,2))) AS  Total_Sum_Insured_DV        
, SUM(CAST( Fee_Amount * Line_Factor AS DECIMAL(28,2))) AS  Fee_Amount        
, SUM(CAST( Coverholder_Reported_Total_Tax_And_Levies * Line_Factor AS DECIMAL(28,2))) AS  Coverholder_Reported_Total_Tax_And_Levies        
, SUM(CAST( Coverholder_Reported_Net_Due_To_Insurer * Line_Factor AS DECIMAL(28,2))) AS  Coverholder_Reported_Net_Due_To_Insurer        
, SUM(CAST( Total_Taxes_Paid_Locally * Line_Factor AS DECIMAL(28,2))) AS  Total_Taxes_Paid_Locally        
, SUM(CAST( Market_Taxes * Line_Factor AS DECIMAL(28,2))) AS  Market_Taxes        
, SUM(CAST( Total_Fee_Amount * Line_Factor AS DECIMAL(28,2))) AS  Total_Fee_Amount        
, SUM(CAST( Amount_Of_Taxable_Written_Premium * Line_Factor AS DECIMAL(28,2))) AS  Amount_Of_Taxable_Written_Premium        
, SUM(CAST( Local_Sub_Producers_Commission_Amount * Line_Factor AS DECIMAL(28,2))) AS  Local_Sub_Producers_Commission_Amount        
, SUM(CAST( Transaction_Original_Currency_Accessori_Italy * Line_Factor AS DECIMAL(28,2))) AS  Transaction_Original_Currency_Accessori_Italy        
, SUM(CAST( One_Hundred_Percent_Net_Written_Premium_In_USD * Line_Factor AS DECIMAL(28,2))) AS  One_Hundred_Percent_Net_Written_Premium_In_USD        
, SUM(CAST( Accessori_Written_Amount * Line_Factor AS DECIMAL(28,2))) AS  Accessori_Written_Amount        
, SUM(CAST( Ultimate_Value * Line_Factor AS DECIMAL(28,2))) AS  Ultimate_Value        
, SUM(CAST( Comprehensive_Personal_Liability_Premium_100_Percent * Line_Factor AS DECIMAL(19,8))) AS  Comprehensive_Personal_Liability_Premium_100_Percent        
, SUM(CAST( Discount_Amount * Line_Factor AS DECIMAL(28,2))) AS  Discount_Amount        
, SUM(CAST( Last_Year_As_If_Premium * Line_Factor AS DECIMAL(28,2))) AS  Last_Year_As_If_Premium        
, SUM(CAST( Last_Year_As_If_Premium_Renewal * Line_Factor AS DECIMAL(28,2))) AS  Last_Year_As_If_Premium_Renewal        
, SUM(CAST( Expiring_Year_Net_Prem_Ex_Tax * Line_Factor AS DECIMAL(28,2))) AS  Expiring_Year_Net_Prem_Ex_Tax        
, SUM(CAST( Net_Premium_Risk_Code_B5_MD * Line_Factor AS DECIMAL(28,2))) AS  Net_Premium_Risk_Code_B5_MD        
, SUM(CAST( Net_Premium_Risk_Code_NA_PL * Line_Factor AS DECIMAL(28,2))) AS  Net_Premium_Risk_Code_NA_PL        
, SUM(CAST( Net_Premium_Risk_Code_TU_Terror * Line_Factor AS DECIMAL(28,2))) AS  Net_Premium_Risk_Code_TU_Terror        
, SUM(CAST( Net_Premium_Risk_Code_W3_EL * Line_Factor AS DECIMAL(28,2))) AS  Net_Premium_Risk_Code_W3_EL        
, SUM(CAST( Gross_Premium_Risk_Code_B5_MD * Line_Factor AS DECIMAL(28,2))) AS  Gross_Premium_Risk_Code_B5_MD        
, SUM(CAST( Gross_Premium_Risk_Code_NA_PL * Line_Factor AS DECIMAL(28,2))) AS  Gross_Premium_Risk_Code_NA_PL        
, SUM(CAST( Gross_Premium_Risk_Code_TU_Terror * Line_Factor AS DECIMAL(28,2))) AS  Gross_Premium_Risk_Code_TU_Terror        
, SUM(CAST( Gross_Premium_Risk_Code_W3_EL * Line_Factor AS DECIMAL(28,2))) AS  Gross_Premium_Risk_Code_W3_EL        
FROM etl.stg_Bordereaux_Risk_Premium_LineFactor_test           
           
UNION ALL 

SELECT           
SUM(cast( Brokerage_Percent_Of_Gross_Premium AS DECIMAL (19,8))) as Brokerage_Percent_Of_Gross_Premium           
, SUM(cast( Brokerage_Brokerage_Amount_Original_Currency AS DECIMAL (28,2))) as Brokerage_Brokerage_Amount_Original_Currency           
, SUM(cast( Deductible_Amount AS DECIMAL (28,2))) as Deductible_Amount           
, SUM(cast( Final_Net_Premium_Percent_For_Order AS DECIMAL (19,8))) as Final_Net_Premium_Percent_For_Order           
, SUM(cast( Final_Net_Premium_Brokerage_Amount_SCC AS DECIMAL (28,2))) as Final_Net_Premium_Brokerage_Amount_SCC           
, SUM(cast( Final_Net_Premium_OCC AS DECIMAL (28,2))) as Final_Net_Premium_OCC           
, SUM(cast( Final_Net_Premium_SCC AS DECIMAL (28,2))) as Final_Net_Premium_SCC           
, SUM(cast( Final_Net_Premium_Rate_Of_Exchange AS DECIMAL (28,18))) as Final_Net_Premium_Rate_Of_Exchange           
, SUM(cast( Other_Fees_Or_Deductions_Amount AS DECIMAL (28,2))) as Other_Fees_Or_Deductions_Amount           
, SUM(cast( Sum_Insured_Amount AS DECIMAL (28,2))) as Sum_Insured_Amount           
, SUM(cast( Terrorism_Premium_OCC AS DECIMAL (28,2))) as Terrorism_Premium_OCC           
, SUM(cast( Total_Taxes_And_Levies_OCC AS DECIMAL (28,2))) as Total_Taxes_And_Levies_OCC           
, SUM(cast( Net_Premium_To_Market_SCC AS DECIMAL (28,2))) as Net_Premium_To_Market_SCC           
, SUM(cast( Rate_Of_Exchange_SCC AS DECIMAL (28,18))) as Rate_Of_Exchange_SCC           
, SUM(cast( Coverholder_Commission_Amount_For_Whole_Risk_Written_Premium AS DECIMAL (28,2))) as Coverholder_Commission_Amount_For_Whole_Risk_Written_Premium           
, SUM(cast( Tax_Amount_For_The_Whole_Risk_Written_Premium AS DECIMAL (28,2))) as Tax_Amount_For_The_Whole_Risk_Written_Premium           
, SUM(cast( IA_Levy_Total_Gross_Written_Premium_Amount AS DECIMAL (28,2))) as IA_Levy_Total_Gross_Written_Premium_Amount           
, SUM(cast( Estimated_Premium_Income AS DECIMAL (28,2))) as Estimated_Premium_Income           
, SUM(cast( Market_Brokerage_Amount_For_The_Risk AS DECIMAL (28,2))) as Market_Brokerage_Amount_For_The_Risk           
, SUM(cast( Management_Expense AS DECIMAL (28,2))) as Management_Expense           
, SUM(cast( Net_Premium_To_Underwriters_For_The_Risk AS DECIMAL (28,2))) as Net_Premium_To_Underwriters_For_The_Risk           
, SUM(cast( A_Buildings AS DECIMAL (28,2))) as A_Buildings           
, SUM(cast( C_Contents AS DECIMAL (28,2))) as C_Contents           
, SUM(cast( D_Business_Interruption AS DECIMAL (28,2))) as D_Business_Interruption           
, SUM(cast( Insured_Assets AS DECIMAL (28,2))) as Insured_Assets           
, SUM(cast( Rebuilding_Cost AS DECIMAL (28,2))) as Rebuilding_Cost           
, SUM(cast( Building_Excess_Amount AS DECIMAL (28,2))) as Building_Excess_Amount           
, SUM(cast( Building_New_Annual_Premium AS DECIMAL (28,2))) as Building_New_Annual_Premium           
, SUM(cast( Building_Transaction_Premium AS DECIMAL (28,2))) as Building_Transaction_Premium           
, SUM(cast( Building_Alternative_Accommodation_Limit AS DECIMAL (28,2))) as Building_Alternative_Accommodation_Limit           
, SUM(cast( High_Value_Art AS DECIMAL (28,2))) as High_Value_Art           
, SUM(cast( Contents_Blanket_Sum_Insured AS DECIMAL (28,2))) as Contents_Blanket_Sum_Insured           
, SUM(cast( Contents_Excess_Sum AS DECIMAL (28,2))) as Contents_Excess_Sum           
, SUM(cast( Contents_New_Annual_Premium AS DECIMAL (28,2))) as Contents_New_Annual_Premium           
, SUM(cast( Contents_Transaction_Premium AS DECIMAL (28,2))) as Contents_Transaction_Premium           
, SUM(cast( Contents_Alternative_Accommodation_Limit AS DECIMAL (28,2))) as Contents_Alternative_Accommodation_Limit           
, SUM(cast( Property_Total_Premium_Payable AS DECIMAL (28,2))) as Property_Total_Premium_Payable           
, SUM(cast( Limit_Of_Indemnity AS DECIMAL (28,2))) as Limit_Of_Indemnity           
, SUM(cast( Insured_Professional_Fees AS DECIMAL (28,2))) as Insured_Professional_Fees           
, SUM(cast( Other_Risk_Factor_Value AS DECIMAL (28,2))) as Other_Risk_Factor_Value           
, SUM(cast( Weekly_Accident_And_Illness_Cover_Benefit AS DECIMAL (28,2))) as Weekly_Accident_And_Illness_Cover_Benefit           
, SUM(cast( Weekly_Business_Expenses_Limit AS DECIMAL (28,2))) as Weekly_Business_Expenses_Limit           
, SUM(cast( Deductible_Or_Excess_Days AS DECIMAL (28,2))) as Deductible_Or_Excess_Days           
, SUM(cast( Total_Premium_Sum_Of_Instalments AS DECIMAL (28,2))) as Total_Premium_Sum_Of_Instalments           
, SUM(cast( Medical_Payment_Premium AS DECIMAL (28,2))) as Medical_Payment_Premium           
, SUM(cast( Policy_Fee AS DECIMAL (28,2))) as Policy_Fee           
, SUM(cast( B_Other_Structures_100_Percent AS DECIMAL (19,8))) as B_Other_Structures_100_Percent           
, SUM(cast( Earthquake_Premium_Amount AS DECIMAL (28,2))) as Earthquake_Premium_Amount           
, SUM(cast( Natural_Disaster_Premium_Amount AS DECIMAL (28,2))) as Natural_Disaster_Premium_Amount           
, SUM(cast( This_Year_As_If_Premium AS DECIMAL (28,2))) as This_Year_As_If_Premium           
, SUM(cast( This_Year_As_If_Premium_New_Business AS DECIMAL (28,2))) as This_Year_As_If_Premium_New_Business           
, SUM(cast( This_Year_As_If_Premium_Renewal AS DECIMAL (28,2))) as This_Year_As_If_Premium_Renewal           
, SUM(cast( Total_Tax AS DECIMAL (28,2))) as Total_Tax           
, SUM(cast( A_Buildings_SI AS DECIMAL (28,2))) as A_Buildings_SI           
, SUM(cast( Total_Sum_Insured_DV AS DECIMAL (28,2))) as Total_Sum_Insured_DV           
, SUM(cast( Fee_Amount AS DECIMAL (28,2))) as Fee_Amount           
, SUM(cast( Coverholder_Reported_Total_Tax_And_Levies AS DECIMAL (28,2))) as Coverholder_Reported_Total_Tax_And_Levies           
, SUM(cast( Coverholder_Reported_Net_Due_To_Insurer AS DECIMAL (28,2))) as Coverholder_Reported_Net_Due_To_Insurer           
, SUM(cast( Total_Taxes_Paid_Locally AS DECIMAL (28,2))) as Total_Taxes_Paid_Locally           
, SUM(cast( Market_Taxes AS DECIMAL (28,2))) as Market_Taxes           
, SUM(cast( Total_Fee_Amount AS DECIMAL (28,2))) as Total_Fee_Amount           
,SUM(cast(Amount_Of_Taxable_Written_Premium AS DECIMAL (28,2))) AS Amount_Of_Taxable_Written_Premium          
,SUM(cast(Local_Sub_Producers_Commission_Amount AS DECIMAL (28,2))) AS Local_Sub_Producers_Commission_Amount          
,SUM(cast(Transaction_Original_Currency_Accessori_Italy AS DECIMAL (28,2))) AS Transaction_Original_Currency_Accessori_Italy          
,SUM(cast(One_Hundred_Percent_Net_Written_Premium_In_USD AS DECIMAL (28,2))) AS One_Hundred_Percent_Net_Written_Premium_In_USD          
,SUM(cast(Accessori_Written_Amount AS DECIMAL (28,2))) AS Accessori_Written_Amount          
,SUM(cast(Ultimate_Value AS DECIMAL (28,2))) AS Ultimate_Value          
,SUM(cast(Comprehensive_Personal_Liability_Premium_100_Percent AS DECIMAL (19,8))) AS Comprehensive_Personal_Liability_Premium_100_Percent          
,SUM(cast(Discount_Amount AS DECIMAL (28,2))) AS Discount_Amount          
,SUM(cast(Last_Year_As_If_Premium AS DECIMAL (28,2))) AS Last_Year_As_If_Premium          
,SUM(cast(Last_Year_As_If_Premium_Renewal AS DECIMAL (28,2))) AS Last_Year_As_If_Premium_Renewal          
,SUM(cast(Expiring_Year_Net_Prem_Ex_Tax AS DECIMAL (28,2))) AS Expiring_Year_Net_Prem_Ex_Tax          
,SUM(cast(Net_Premium_Risk_Code_B5_MD AS DECIMAL (28,2))) AS Net_Premium_Risk_Code_B5_MD          
,SUM(cast(Net_Premium_Risk_Code_NA_PL AS DECIMAL (28,2))) AS Net_Premium_Risk_Code_NA_PL          
,SUM(cast(Net_Premium_Risk_Code_TU_Terror AS DECIMAL (28,2))) AS Net_Premium_Risk_Code_TU_Terror          
,SUM(cast(Net_Premium_Risk_Code_W3_EL AS DECIMAL (28,2))) AS Net_Premium_Risk_Code_W3_EL          
,SUM(cast(Gross_Premium_Risk_Code_B5_MD AS DECIMAL (28,2))) AS Gross_Premium_Risk_Code_B5_MD          
,SUM(cast(Gross_Premium_Risk_Code_NA_PL AS DECIMAL (28,2))) AS Gross_Premium_Risk_Code_NA_PL          
,SUM(cast(Gross_Premium_Risk_Code_TU_Terror AS DECIMAL (28,2))) AS Gross_Premium_Risk_Code_TU_Terror          
,SUM(cast(Gross_Premium_Risk_Code_W3_EL AS DECIMAL (28,2))) AS Gross_Premium_Risk_Code_W3_EL          
FROM dwh.Fact_Bdx_Risk_Premium_Transaction 										
"""

pd_dataframe_Fact_Bdx_Risk_Premium = pd.read_sql_query(Fact_Bdx_Risk_Premium_METRIC, connDw)

df_Bdx_Fact_Bdx_Risk_Premium = spark.createDataFrame(pd_dataframe_Fact_Bdx_Risk_Premium)
df_Bdx_Fact_Bdx_Risk_Premium_count = df_Bdx_Fact_Bdx_Risk_Premium.drop('table_type').dropDuplicates().count()
if(df_Bdx_Fact_Bdx_Risk_Premium_count > 1):
    print('Output : Metric check is failed for Fact Bdx_Risk_Premium ')
    display(df_Bdx_Fact_Bdx_Risk_Premium)
else:
    print('Output : Metric check is passed for Fact Bdx_Risk_Premium !!')



# COMMAND ----------

# DBTITLE 1,Fact_Bdx_Risk_Premium_Transaction_Movements - Metric - Should not return any result
try:
    qry = """SET NOCOUNT ON;
            DROP TABLE etl.stg_Bordereaux_Risk_Premium_Movements_test ;
            select '' """
    pd.read_sql_query(qry,connDw)
except:
    print("Table etl.stg_Bordereaux_Risk_Premium_Movements_test not exists to delete")


stg_risk_premium_mvmts_qry = """
SET NOCOUNT ON;
CREATE TABLE etl.stg_Bordereaux_Risk_Premium_Movements_test
WITH (CLUSTERED COLUMNSTORE INDEX,DISTRIBUTION = HASH (policy_header_reference)) AS
SELECT
coalesce(Brokerage_BrokerageAmountOriginalCurrency ,0.00) AS 	Brokerage_Brokerage_Amount_Original_Currency
,coalesce(FinalNetPremium_BrokerageAmountSettlementCurrency ,0.00) AS 	Final_Net_Premium_Brokerage_Amount_SCC
,coalesce(FinalNetPremium_FinalNetPremiumOriginalCurrency ,0.00) AS 	Final_Net_Premium_OCC
,coalesce(FinalNetPremium_FinalNetPremiumSettlementCurrency ,0.00) AS 	Final_Net_Premium_SCC
,coalesce(OtherFeesOrDeductions_Amount ,0.00) AS 	Other_Fees_Or_Deductions_Amount
,coalesce(SumInsured_Amount ,0.00) AS 	Sum_Insured_Amount
,coalesce(Transaction_OriginalCurrency_TerrorismPremium ,0.00) AS 	Terrorism_Premium_OCC
,coalesce(Transaction_OriginalCurrency_TotalTaxesAndLevies ,0.00) AS 	Total_Taxes_And_Levies_OCC
,coalesce(Transaction_SettlementCurrency_NetPremiumToMarket ,0.00) AS 	Net_Premium_To_Market_SCC
,coalesce(CoverholderCommissionAmountForWholeRiskWrittenPremium ,0.00) AS 	Coverholder_Commission_Amount_For_Whole_Risk_Written_Premium
,coalesce(TaxAmountForTheWholeRiskWrittenPremium ,0.00) AS 	Tax_Amount_For_The_Whole_Risk_Written_Premium
,coalesce(IALevy_TotalGrossWrittenPremiumAmount ,0.00) AS 	IA_Levy_Total_Gross_Written_Premium_Amount
,coalesce(EstimatedPremiumIncome ,0.00) AS 	Estimated_Premium_Income
,coalesce(MarketBrokerageAmountForTheRisk ,0.00) AS 	Market_Brokerage_Amount_For_The_Risk
,coalesce(ManagementExpense ,0.00) AS 	Management_Expense
,coalesce(NetPremiumToUnderwritersForTheRisk ,0.00) AS 	Net_Premium_To_Underwriters_For_The_Risk
,coalesce(A_Buildings ,0.00) AS 	A_Buildings
,coalesce(C_Contents ,0.00) AS 	C_Contents
,coalesce(D_BusinessInterruption ,0.00) AS 	D_Business_Interruption
,coalesce(InsuredAssets ,0.00) AS 	Insured_Assets
,coalesce(RebuildingCost ,0.00) AS 	Rebuilding_Cost
,coalesce(BuildingExcessAmount ,0.00) AS 	Building_Excess_Amount
,coalesce(BuildingNewAnnualPremium ,0.00) AS 	Building_New_Annual_Premium
,coalesce(BuildingTransactionPremium ,0.00) AS 	Building_Transaction_Premium
,coalesce(BuildingAlternativeAccommodationLimit ,0.00) AS 	Building_Alternative_Accommodation_Limit
,coalesce(HighValueArt ,0.00) AS 	High_Value_Art
,coalesce(ContentsBlanketSumInsured ,0.00) AS 	Contents_Blanket_Sum_Insured
,coalesce(ContentsExcessSum ,0.00) AS 	Contents_Excess_Sum
,coalesce(ContentsNewAnnualPremium ,0.00) AS 	Contents_New_Annual_Premium
,coalesce(ContentsTransactionPremium ,0.00) AS 	Contents_Transaction_Premium
,coalesce(ContentsAlternativeAccommodationLimit ,0.00) AS 	Contents_Alternative_Accommodation_Limit
,coalesce(PropertyTotalPremiumPayable ,0.00) AS 	Property_Total_Premium_Payable
,coalesce(LimitOfIndemnity ,0.00) AS 	Limit_Of_Indemnity
,coalesce(InsuredProfessionalFees ,0.00) AS 	Insured_Professional_Fees
,coalesce(OtherRiskFactorValue ,0.00) AS 	Other_Risk_Factor_Value
,coalesce(TotalPremium_TotalPremiumSumOfInstalments ,0.00) AS 	Total_Premium_Sum_Of_Instalments
,coalesce(Transaction_OriginalCurrency_CommissionAmount ,0.00) AS 	Commission_Amount_OCC
,coalesce(Transaction_OriginalCurrency_GrossPremiumPaidThisTime ,0.00) AS 	Gross_Premium_Paid_This_Time_OCC
,coalesce(Transaction_OriginalCurrency_NetPremiumToMarket ,0.00) AS 	Net_Premium_To_Market_OCC
,coalesce(MedicalPaymentPremium ,0.00) AS 	Medical_Payment_Premium
,coalesce(PolicyFee ,0.00) AS 	Policy_Fee
,coalesce(B_OtherStructures100Percent ,0.00) AS 	B_Other_Structures_100_Percent
,coalesce(EarthquakePremiumAmount ,0.00) AS 	Earthquake_Premium_Amount
,coalesce(NaturalDisasterPremiumAmount ,0.00) AS 	Natural_Disaster_Premium_Amount
,coalesce(ThisYearAsIfPremium ,0.00) AS 	This_Year_As_If_Premium
,coalesce(ThisYearAsIfPremiumNewBusiness ,0.00) AS 	This_Year_As_If_Premium_New_Business
,coalesce(ThisYearAsIfPremiumRenewal ,0.00) AS 	This_Year_As_If_Premium_Renewal
,coalesce(TotalTax ,0.00) AS 	Total_Tax
,coalesce(A_BuildingsSI ,0.00) AS 	A_Buildings_SI
,coalesce(TotalSumInsuredDV ,0.00) AS 	Total_Sum_Insured_DV
,coalesce(FeeAmount ,0.00) AS 	Fee_Amount
,coalesce(CoverholderReported_TotalTaxAndLevies ,0.00) AS 	Coverholder_Reported_Total_Tax_And_Levies
,coalesce(CoverholderReported_NetDueToInsurer ,0.00) AS 	Coverholder_Reported_Net_Due_To_Insurer
,coalesce(TotalTaxesPaidLocally ,0.00) AS 	Total_Taxes_Paid_Locally
,coalesce(MarketTaxes ,0.00) AS 	Market_Taxes
,coalesce(TotalFeeAmount ,0.00) AS 	Total_Fee_Amount
,coalesce(AmountOfTaxableWrittenPremium ,0.00) AS 	Amount_Of_Taxable_Written_Premium
,coalesce(LocalSubProducersCommissionAmount ,0.00) AS 	Local_Sub_Producers_Commission_Amount
,coalesce(Transaction_OriginalCurrency_AccessoriItaly ,0.00) AS 	Transaction_Original_Currency_Accessori_Italy
,coalesce([100PercentNetWrittenPremiumInUSD] ,0.00) AS 	Net_Written_Premium_100_Percent_USD
,coalesce(AccessoriWrittenAmount ,0.00) AS 	Accessori_Written_Amount
,coalesce(UltimateValue ,0.00) AS 	Ultimate_Value
,stg.policy_header_reference as policy_header_reference
,stg.policy_section_reference as policy_section_reference
,CASE
WHEN lf.lakeDeletedTimestamp IS NOT NULL
THEN NULL
ELSE coalesce(Business_Entity_Code,'-1')
END AS Business_Entity_Code
,CASE
WHEN lf.lakeDeletedTimestamp IS NOT NULL
THEN NULL
ELSE coalesce(Line_Factor,1)
END AS Line_Factor
FROM
( SELECT * FROM [Staging].[Bordereaux_Risk_Premium_Movements]
where lakeisactive = 1 and lakedeletedtimestamp is null
) stg
LEFT JOIN
(SELECT
sourceID
,policy_header_reference
,policy_section_reference
,Business_Entity_Code
,Line_Factor
,lakeDeletedTimestamp
FROM etl.LineFactor_tbl_test_movements ) lf
ON stg.policy_header_reference = lf.policy_header_reference
AND stg.policy_section_reference = lf.policy_section_reference ;
select ''
"""

pd.read_sql_query(stg_risk_premium_mvmts_qry,connDw)

Fact_Bdx_Risk_Premium_Movmnts_METRIC = """
select 'Staging' as table_type,
SUM(cast(Brokerage_Brokerage_Amount_Original_Currency * Line_Factor AS DECIMAL (28,2))) AS  Brokerage_Brokerage_Amount_Original_Currency        
,SUM(cast(Final_Net_Premium_Brokerage_Amount_SCC * Line_Factor AS DECIMAL (28,2))) AS  Final_Net_Premium_Brokerage_Amount_SCC        
,SUM(cast(Final_Net_Premium_OCC * Line_Factor AS DECIMAL (28,2))) AS  Final_Net_Premium_OCC        
,SUM(cast(Final_Net_Premium_SCC * Line_Factor AS DECIMAL (28,2))) AS  Final_Net_Premium_SCC        
,SUM(cast(Other_Fees_Or_Deductions_Amount * Line_Factor AS DECIMAL (28,2))) AS  Other_Fees_Or_Deductions_Amount        
,SUM(cast(Sum_Insured_Amount * Line_Factor AS DECIMAL (28,2))) AS  Sum_Insured_Amount        
,SUM(cast(Terrorism_Premium_OCC * Line_Factor AS DECIMAL (28,2))) AS  Terrorism_Premium_OCC        
,SUM(cast(Total_Taxes_And_Levies_OCC * Line_Factor AS DECIMAL (28,2))) AS  Total_Taxes_And_Levies_OCC        
,SUM(cast(Net_Premium_To_Market_SCC * Line_Factor AS DECIMAL (28,2))) AS  Net_Premium_To_Market_SCC        
,SUM(cast(Coverholder_Commission_Amount_For_Whole_Risk_Written_Premium * Line_Factor AS DECIMAL (28,2))) AS  Coverholder_Commission_Amount_For_Whole_Risk_Written_Premium        
,SUM(cast(Tax_Amount_For_The_Whole_Risk_Written_Premium * Line_Factor AS DECIMAL (28,2))) AS  Tax_Amount_For_The_Whole_Risk_Written_Premium        
,SUM(cast(IA_Levy_Total_Gross_Written_Premium_Amount * Line_Factor AS DECIMAL (28,2))) AS  IA_Levy_Total_Gross_Written_Premium_Amount        
,SUM(cast(Estimated_Premium_Income * Line_Factor AS DECIMAL (28,2))) AS  Estimated_Premium_Income        
,SUM(cast(Market_Brokerage_Amount_For_The_Risk * Line_Factor AS DECIMAL (28,2))) AS  Market_Brokerage_Amount_For_The_Risk        
,SUM(cast(Management_Expense * Line_Factor AS DECIMAL (28,2))) AS  Management_Expense        
,SUM(cast(Net_Premium_To_Underwriters_For_The_Risk * Line_Factor AS DECIMAL (28,2))) AS  Net_Premium_To_Underwriters_For_The_Risk        
,SUM(cast(A_Buildings * Line_Factor AS DECIMAL (28,2))) AS  A_Buildings        
,SUM(cast(C_Contents * Line_Factor AS DECIMAL (28,2))) AS  C_Contents        
,SUM(cast(D_Business_Interruption * Line_Factor AS DECIMAL (28,2))) AS  D_Business_Interruption        
,SUM(cast(Insured_Assets * Line_Factor AS DECIMAL (28,2))) AS  Insured_Assets        
,SUM(cast(Rebuilding_Cost * Line_Factor AS DECIMAL (28,2))) AS  Rebuilding_Cost        
,SUM(cast(Building_Excess_Amount * Line_Factor AS DECIMAL (28,2))) AS  Building_Excess_Amount        
,SUM(cast(Building_New_Annual_Premium * Line_Factor AS DECIMAL (28,2))) AS  Building_New_Annual_Premium        
,SUM(cast(Building_Transaction_Premium * Line_Factor AS DECIMAL (28,2))) AS  Building_Transaction_Premium        
,SUM(cast(Building_Alternative_Accommodation_Limit * Line_Factor AS DECIMAL (28,2))) AS  Building_Alternative_Accommodation_Limit        
,SUM(cast(High_Value_Art * Line_Factor AS DECIMAL (28,2))) AS  High_Value_Art        
,SUM(cast(Contents_Blanket_Sum_Insured * Line_Factor AS DECIMAL (28,2))) AS  Contents_Blanket_Sum_Insured        
,SUM(cast(Contents_Excess_Sum * Line_Factor AS DECIMAL (28,2))) AS  Contents_Excess_Sum        
,SUM(cast(Contents_New_Annual_Premium * Line_Factor AS DECIMAL (28,2))) AS  Contents_New_Annual_Premium        
,SUM(cast(Contents_Transaction_Premium * Line_Factor AS DECIMAL (28,2))) AS  Contents_Transaction_Premium        
,SUM(cast(Contents_Alternative_Accommodation_Limit * Line_Factor AS DECIMAL (28,2))) AS  Contents_Alternative_Accommodation_Limit        
,SUM(cast(Property_Total_Premium_Payable * Line_Factor AS DECIMAL (28,2))) AS  Property_Total_Premium_Payable        
,SUM(cast(Limit_Of_Indemnity * Line_Factor AS DECIMAL (28,2))) AS  Limit_Of_Indemnity        
,SUM(cast(Insured_Professional_Fees * Line_Factor AS DECIMAL (28,2))) AS  Insured_Professional_Fees        
,SUM(cast(Other_Risk_Factor_Value * Line_Factor AS DECIMAL (28,2))) AS  Other_Risk_Factor_Value        
,SUM(cast(Total_Premium_Sum_Of_Instalments * Line_Factor AS DECIMAL (28,2))) AS  Total_Premium_Sum_Of_Instalments        
,SUM(cast(Commission_Amount_OCC * Line_Factor AS DECIMAL (28,2))) AS  Commission_Amount_OCC        
,SUM(cast(Gross_Premium_Paid_This_Time_OCC * Line_Factor AS DECIMAL (28,2))) AS  Gross_Premium_Paid_This_Time_OCC        
,SUM(cast(Net_Premium_To_Market_OCC * Line_Factor AS DECIMAL (28,2))) AS  Net_Premium_To_Market_OCC        
,SUM(cast(Medical_Payment_Premium * Line_Factor AS DECIMAL (28,2))) AS  Medical_Payment_Premium        
,SUM(cast(Policy_Fee * Line_Factor AS DECIMAL (28,2))) AS  Policy_Fee        
,SUM(cast(B_Other_Structures_100_Percent * Line_Factor AS DECIMAL (28,2))) AS  B_Other_Structures_100_Percent        
,SUM(cast(Earthquake_Premium_Amount * Line_Factor AS DECIMAL (28,2))) AS  Earthquake_Premium_Amount        
,SUM(cast(Natural_Disaster_Premium_Amount * Line_Factor AS DECIMAL (28,2))) AS  Natural_Disaster_Premium_Amount        
,SUM(cast(This_Year_As_If_Premium * Line_Factor AS DECIMAL (28,2))) AS  This_Year_As_If_Premium        
,SUM(cast(This_Year_As_If_Premium_New_Business * Line_Factor AS DECIMAL (28,2))) AS  This_Year_As_If_Premium_New_Business        
,SUM(cast(This_Year_As_If_Premium_Renewal * Line_Factor AS DECIMAL (28,2))) AS  This_Year_As_If_Premium_Renewal        
,SUM(cast(Total_Tax * Line_Factor AS DECIMAL (28,2))) AS  Total_Tax        
,SUM(cast(A_Buildings_SI * Line_Factor AS DECIMAL (28,2))) AS  A_Buildings_SI        
,SUM(cast(Total_Sum_Insured_DV * Line_Factor AS DECIMAL (28,2))) AS  Total_Sum_Insured_DV        
,SUM(cast(Fee_Amount * Line_Factor AS DECIMAL (28,2))) AS  Fee_Amount        
,SUM(cast(Coverholder_Reported_Total_Tax_And_Levies * Line_Factor AS DECIMAL (28,2))) AS  Coverholder_Reported_Total_Tax_And_Levies        
,SUM(cast(Coverholder_Reported_Net_Due_To_Insurer * Line_Factor AS DECIMAL (28,2))) AS  Coverholder_Reported_Net_Due_To_Insurer        
,SUM(cast(Total_Taxes_Paid_Locally * Line_Factor AS DECIMAL (28,2))) AS  Total_Taxes_Paid_Locally        
,SUM(cast(Market_Taxes * Line_Factor AS DECIMAL (28,2))) AS  Market_Taxes        
,SUM(cast(Total_Fee_Amount * Line_Factor AS DECIMAL (28,2))) AS  Total_Fee_Amount        
,SUM(cast(Amount_Of_Taxable_Written_Premium * Line_Factor AS DECIMAL (28,2))) AS  Amount_Of_Taxable_Written_Premium        
,SUM(cast(Local_Sub_Producers_Commission_Amount * Line_Factor AS DECIMAL (28,2))) AS  Local_Sub_Producers_Commission_Amount        
,SUM(cast(Transaction_Original_Currency_Accessori_Italy * Line_Factor AS DECIMAL (28,2))) AS  Transaction_Original_Currency_Accessori_Italy        
,SUM(cast(Net_Written_Premium_100_Percent_USD * Line_Factor AS DECIMAL (28,2))) AS  Net_Written_Premium_100_Percent_USD        
,SUM(cast(Accessori_Written_Amount * Line_Factor AS DECIMAL (28,2))) AS  Accessori_Written_Amount        
,SUM(cast(Ultimate_Value * Line_Factor AS DECIMAL (28,2))) AS  Ultimate_Value        
         
FROM etl.stg_Bordereaux_Risk_Premium_Movements_test         
         
UNION ALL          
SELECT 'Fact' as table_type,
SUM(cast(Brokerage_Brokerage_Amount_Original_Currency AS DECIMAL (28,2))) as  Brokerage_Brokerage_Amount_Original_Currency        
,SUM(cast(Final_Net_Premium_Brokerage_Amount_SCC AS DECIMAL (28,2))) as  Final_Net_Premium_Brokerage_Amount_SCC        
,SUM(cast(Final_Net_Premium_OCC AS DECIMAL (28,2))) as  Final_Net_Premium_OCC        
,SUM(cast(Final_Net_Premium_SCC AS DECIMAL (28,2))) as  Final_Net_Premium_SCC        
,SUM(cast(Other_Fees_Or_Deductions_Amount AS DECIMAL (28,2))) as  Other_Fees_Or_Deductions_Amount        
,SUM(cast(Sum_Insured_Amount AS DECIMAL (28,2))) as  Sum_Insured_Amount        
,SUM(cast(Terrorism_Premium_OCC AS DECIMAL (28,2))) as  Terrorism_Premium_OCC        
,SUM(cast(Total_Taxes_And_Levies_OCC AS DECIMAL (28,2))) as  Total_Taxes_And_Levies_OCC        
,SUM(cast(Net_Premium_To_Market_SCC AS DECIMAL (28,2))) as  Net_Premium_To_Market_SCC        
,SUM(cast(Coverholder_Commission_Amount_For_Whole_Risk_Written_Premium AS DECIMAL (28,2))) as  Coverholder_Commission_Amount_For_Whole_Risk_Written_Premium        
,SUM(cast(Tax_Amount_For_The_Whole_Risk_Written_Premium AS DECIMAL (28,2))) as  Tax_Amount_For_The_Whole_Risk_Written_Premium        
,SUM(cast(IA_Levy_Total_Gross_Written_Premium_Amount AS DECIMAL (28,2))) as  IA_Levy_Total_Gross_Written_Premium_Amount        
,SUM(cast(Estimated_Premium_Income AS DECIMAL (28,2))) as  Estimated_Premium_Income        
,SUM(cast(Market_Brokerage_Amount_For_The_Risk AS DECIMAL (28,2))) as  Market_Brokerage_Amount_For_The_Risk        
,SUM(cast(Management_Expense AS DECIMAL (28,2))) as  Management_Expense        
,SUM(cast(Net_Premium_To_Underwriters_For_The_Risk AS DECIMAL (28,2))) as  Net_Premium_To_Underwriters_For_The_Risk        
,SUM(cast(A_Buildings AS DECIMAL (28,2))) as  A_Buildings        
,SUM(cast(C_Contents AS DECIMAL (28,2))) as  C_Contents        
,SUM(cast(D_Business_Interruption AS DECIMAL (28,2))) as  D_Business_Interruption        
,SUM(cast(Insured_Assets AS DECIMAL (28,2))) as  Insured_Assets        
,SUM(cast(Rebuilding_Cost AS DECIMAL (28,2))) as  Rebuilding_Cost        
,SUM(cast(Building_Excess_Amount AS DECIMAL (28,2))) as  Building_Excess_Amount        
,SUM(cast(Building_New_Annual_Premium AS DECIMAL (28,2))) as  Building_New_Annual_Premium        
,SUM(cast(Building_Transaction_Premium AS DECIMAL (28,2))) as  Building_Transaction_Premium        
,SUM(cast(Building_Alternative_Accommodation_Limit AS DECIMAL (28,2))) as  Building_Alternative_Accommodation_Limit        
,SUM(cast(High_Value_Art AS DECIMAL (28,2))) as  High_Value_Art        
,SUM(cast(Contents_Blanket_Sum_Insured AS DECIMAL (28,2))) as  Contents_Blanket_Sum_Insured        
,SUM(cast(Contents_Excess_Sum AS DECIMAL (28,2))) as  Contents_Excess_Sum        
,SUM(cast(Contents_New_Annual_Premium AS DECIMAL (28,2))) as  Contents_New_Annual_Premium        
,SUM(cast(Contents_Transaction_Premium AS DECIMAL (28,2))) as  Contents_Transaction_Premium        
,SUM(cast(Contents_Alternative_Accommodation_Limit AS DECIMAL (28,2))) as  Contents_Alternative_Accommodation_Limit        
,SUM(cast(Property_Total_Premium_Payable AS DECIMAL (28,2))) as  Property_Total_Premium_Payable        
,SUM(cast(Limit_Of_Indemnity AS DECIMAL (28,2))) as  Limit_Of_Indemnity        
,SUM(cast(Insured_Professional_Fees AS DECIMAL (28,2))) as  Insured_Professional_Fees        
,SUM(cast(Other_Risk_Factor_Value AS DECIMAL (28,2))) as  Other_Risk_Factor_Value        
,SUM(cast(Total_Premium_Sum_Of_Instalments AS DECIMAL (28,2))) as  Total_Premium_Sum_Of_Instalments        
,SUM(cast(Commission_Amount_OCC AS DECIMAL (28,2))) as  Commission_Amount_OCC        
,SUM(cast(Gross_Premium_Paid_This_Time_OCC AS DECIMAL (28,2))) as  Gross_Premium_Paid_This_Time_OCC        
,SUM(cast(Net_Premium_To_Market_OCC AS DECIMAL (28,2))) as  Net_Premium_To_Market_OCC        
,SUM(cast(Medical_Payment_Premium AS DECIMAL (28,2))) as  Medical_Payment_Premium        
,SUM(cast(Policy_Fee AS DECIMAL (28,2))) as  Policy_Fee        
,SUM(cast(B_Other_Structures_100_Percent AS DECIMAL (28,2))) as  B_Other_Structures_100_Percent        
,SUM(cast(Earthquake_Premium_Amount AS DECIMAL (28,2))) as  Earthquake_Premium_Amount        
,SUM(cast(Natural_Disaster_Premium_Amount AS DECIMAL (28,2))) as  Natural_Disaster_Premium_Amount        
,SUM(cast(This_Year_As_If_Premium AS DECIMAL (28,2))) as  This_Year_As_If_Premium        
,SUM(cast(This_Year_As_If_Premium_New_Business AS DECIMAL (28,2))) as  This_Year_As_If_Premium_New_Business        
,SUM(cast(This_Year_As_If_Premium_Renewal AS DECIMAL (28,2))) as  This_Year_As_If_Premium_Renewal        
,SUM(cast(Total_Tax AS DECIMAL (28,2))) as  Total_Tax        
,SUM(cast(A_Buildings_SI AS DECIMAL (28,2))) as  A_Buildings_SI        
,SUM(cast(Total_Sum_Insured_DV AS DECIMAL (28,2))) as  Total_Sum_Insured_DV        
,SUM(cast(Fee_Amount AS DECIMAL (28,2))) as  Fee_Amount        
,SUM(cast(Coverholder_Reported_Total_Tax_And_Levies AS DECIMAL (28,2))) as  Coverholder_Reported_Total_Tax_And_Levies        
,SUM(cast(Coverholder_Reported_Net_Due_To_Insurer AS DECIMAL (28,2))) as  Coverholder_Reported_Net_Due_To_Insurer        
,SUM(cast(Total_Taxes_Paid_Locally AS DECIMAL (28,2))) as  Total_Taxes_Paid_Locally        
,SUM(cast(Market_Taxes AS DECIMAL (28,2))) as  Market_Taxes        
,SUM(cast(Total_Fee_Amount AS DECIMAL (28,2))) as  Total_Fee_Amount        
,SUM(cast(Amount_Of_Taxable_Written_Premium AS DECIMAL (28,2))) as  Amount_Of_Taxable_Written_Premium        
,SUM(cast(Local_Sub_Producers_Commission_Amount AS DECIMAL (28,2))) as  Local_Sub_Producers_Commission_Amount        
,SUM(cast(Transaction_Original_Currency_Accessori_Italy AS DECIMAL (28,2))) as  Transaction_Original_Currency_Accessori_Italy        
,SUM(cast(Net_Written_Premium_100_Percent_USD AS DECIMAL (28,2))) as  Net_Written_Premium_100_Percent_USD        
,SUM(cast(Accessori_Written_Amount AS DECIMAL (28,2))) as  Accessori_Written_Amount        
,SUM(cast(Ultimate_Value AS DECIMAL (28,2))) as  Ultimate_Value        
         
FROM dwh.Fact_Bdx_Risk_Premium_Transaction_Movements
"""

pd_dataframe_Fact_Bdx_Risk_Premium_Movmnts = pd.read_sql_query(Fact_Bdx_Risk_Premium_Movmnts_METRIC, connDw)

df_Bdx_Fact_Bdx_Risk_Premium_Movmnts = spark.createDataFrame(pd_dataframe_Fact_Bdx_Risk_Premium_Movmnts)
df_Bdx_Fact_Bdx_Risk_Premium_Movmnts_count = df_Bdx_Fact_Bdx_Risk_Premium_Movmnts.drop('table_type').dropDuplicates().count()
if(df_Bdx_Fact_Bdx_Risk_Premium_Movmnts_count > 1):
    print('Output : Metric check is failed for Fact Bdx_Risk_Premium_Movmnts ')
    display(df_Bdx_Fact_Bdx_Risk_Premium_Movmnts)
else:
    print('Output : Metric check is passed for Fact Bdx_Risk_Premium_Movmnts !!')



# COMMAND ----------

