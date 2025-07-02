# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>ref_unstructuredSource_dbvw_dbdt</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Loading data from databricks view to delta</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Framework</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2019/01/01</td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC <table>
# MAGIC   <tr>
# MAGIC     <th>Parameter Name</th>
# MAGIC     <th>Parameter Description</th>
# MAGIC     <th>Example Parameter</th>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>batchId</td>
# MAGIC     <td>@batchId to retrieve batch details</td>
# MAGIC     <td>@batchId = 7</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>batchTaskId</td>
# MAGIC     <td>@batchTaskId to retrieve batchTask details</td>
# MAGIC     <td>@batchTaskId = 10</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>adfPipeLineName</td>
# MAGIC     <td>@adfPipeLineName to retrieve DataFactory pipelinename details</td>
# MAGIC     <td>@adfPipeLineName = 'testpipepline'</td>
# MAGIC   </tr>
# MAGIC    <tr>
# MAGIC     <td>notebookName</td>
# MAGIC     <td>@notebookName to retrieve notebookname </td>
# MAGIC     <td>@notebookName = 'testnotebook'</td>
# MAGIC   </tr>
# MAGIC     <tr>
# MAGIC     <td>clusterId</td>
# MAGIC     <td>@clusterId to retrieve cluster details </td>
# MAGIC     <td>@clusterId = '12345'</td>
# MAGIC   </tr>
# MAGIC    <tr>
# MAGIC     <td>sourceId</td>
# MAGIC     <td>@sourceId to retrieve source details </td>
# MAGIC     <td>@sourceId = '1'</td>
# MAGIC   </tr>
# MAGIC   
# MAGIC   </table>
# MAGIC   
# MAGIC ## Change Log
# MAGIC <table>
# MAGIC   <tr>
# MAGIC     <th>Date</th>
# MAGIC     <th>Changed By</th>
# MAGIC     <th>Change Description</th>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>2019/01/01</td>
# MAGIC     <td>Framework</td>
# MAGIC     <tD>Removed broadcast from PolicyAdminRisk statement.
# MAGIC          <br> Removed currentParty view and added party view, Updated 
# MAGIC        Vw_Staging_Reference_unstructuredSource_unstructuredSource_PolicyAdmin_Policy_Party
# MAGIC       ,Vw_Staging_Reference_unstructuredSource_unstructuredSource_PolicyAdmin_PartyInstance
# MAGIC       ,Vw_Staging_Reference_unstructuredSource_unstructuredSource_PolicyAdmin_Risk
# MAGIC       ,Vw_Staging_Reference_unstructuredSource_unstructuredSource_PolicyAdmin_WebUser</td>
# MAGIC   </tr>
# MAGIC 
# MAGIC </table>
# MAGIC 
# MAGIC ##General Standards applying to each notebook
# MAGIC <ol>
# MAGIC   <li>Above information completed</li>
# MAGIC   <li>Deployed to approriate workspace folder path</li>
# MAGIC   <li>Commited to source control</li>
# MAGIC   <li>Code tested</li>
# MAGIC   <li>Only include libraries and imports that are being used - i.e. don't include something if it is not used</li>
# MAGIC   <li>Code commented</li>
# MAGIC   <li>Code Peer reviewed</li>
# MAGIC </ol>

# COMMAND ----------

# DBTITLE 1,Run log function notebook
# MAGIC %run ../../util/gen/logging_functions

# COMMAND ----------

# DBTITLE 1,Run standard function notebook
# MAGIC %run ../../util/gen/standard_functions

# COMMAND ----------

# DBTITLE 1,Run unstructuredSource functions notebook
# MAGIC %run ../../util/spe/unstructuredSource_functions

# COMMAND ----------

# DBTITLE 1,Import modules
try:
  import pandas as pd
  import pyodbc
  import json
  from pyspark.sql.types import StructType,StructField,StringType,IntegerType
  from datetime import datetime,timedelta
  from pyspark.sql.functions import lit,col,explode,concat_ws,create_map,struct,collect_list,coalesce
  from functools import reduce
  from itertools import chain  
except Exception as e:
  errorMessage="Exception occured while import modules " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Initialise variables
try: 
  #varible for current_time
  currentTs=datetime.now()
  #get the last 3 positions of the microseconds as we need to remove them
  currentTsMicroseconds = int(str(currentTs.strftime('%f'))[3:6])
  #take away the last three microseconds
  currentTs = currentTs - timedelta(microseconds=currentTsMicroseconds)
  
  #get the date as an int format
  CreatedDate = int(currentTs.strftime('%Y%m%d'))
  #get the hour
  CreatedHour = currentTs.hour
  CreatedTimestamp = currentTs
  LastUpdatedTimestamp = currentTs
  #use the same time for the date also
  date=currentTs
  
  #PARAMETER FOR logError
  errorMessage = ''
  adfPipelineName = ''
  clusterId = ''
  notebookName = ''
  batchId = -1
  batchTaskId = -1

  #parameter for log_task_end
  batchTaskStatus = ''
  batchTaskSourceRows = 0
  batchTaskRowsLoaded = 0
  batchTaskRejectRows = 0
  batchTaskResult = ''
  batchTaskResultLocation = ''
  batchTaskProgressMessage = '' 
  
  #variable to retrieve the batch rows loaded or not
  retrieveBatchRowsLoaded = False
  
except Exception as e:
  errorMessage = "Exception occured while variable initialisation :" + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Get notebook parameters into variables and initialise error log location
try:
  #GET batchTaskId FROM WIDGETS   
  dbutils.widgets.text("batchTaskId","")
  batchTaskId = dbutils.widgets.get("batchTaskId")

  #GET batchId FROM WIDGETS
  dbutils.widgets.text("batchId","")
  batchId = dbutils.widgets.get("batchId")
  
  #GET sourceId  FROM WIDGETS   
  dbutils.widgets.text("sourceId","")
  sourceId  = dbutils.widgets.get("sourceId")

  #GET adfPipelineName  FROM WIDGETS
  dbutils.widgets.text("adfPipelineName","")
  adfPipelineName  = dbutils.widgets.get("adfPipelineName")
  
  #GET notebookName  FROM WIDGETS   
  dbutils.widgets.text("notebookName","")
  notebookName  = dbutils.widgets.get("notebookName")

  #GET clusterId  FROM WIDGETS
  dbutils.widgets.text("clusterId","")
  clusterId  = dbutils.widgets.get("clusterId")
  
  #assigned the source and batch to other variables that are referenced from the metadata
  SourceID=sourceId
  CreatedBatchID=batchId
  LastUpdatedBatchID=batchId
  
  #call the get_logging_path function to create a log file path as a string and store it in a variable
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date, 'error')  
except Exception as e:
  errorMessage = "Exception occured while getting parameters and initiliasing error log location: " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,Establish SQL Database connection
#access secret of database connection details from azure key vault
dbconn=dbutils.secrets.get(scope="data-scope-01", key="sql-dbrks-connection-01")

#call function sqlDbConn to establish Database connection with given scope and key values
try:
  conn,cursor = sqlDbConn(dbconn,
                          batchTaskId,                          
                          adfPipelineName,
                          clusterId,
                          notebookName,
                          errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,"Successfully Established SQL Connection")
except Exception as e:
  errorMessage="unable to establish DB connection: " + str(e)
  logToFile(errorLogFileLocation,errorMessage)
  assert False

# COMMAND ----------

# DBTITLE 1,Get metadata - Execute high and low level stored procedures
#call spExecHighAndLowLevel function to retrieve values from store procedure and store in pandas dataframe                                         
try:        
  sourceDestinationTableDetails,sourceDestinationFieldsDetails = spExecHighAndLowLevel(conn
                                                                                       ,cursor
                                                                                       ,batchTaskId
                                                                                       ,adfPipelineName
                                                                                       ,clusterId
                                                                                       ,notebookName
                                                                                       ,errorLogFileLocation)

  #Replace the nulls with 'none' as this sp always returns single record no need to specify schema
  sourceDestinationTableDetails=sourceDestinationTableDetails.fillna('none')
  #sourceDestinationFieldsDetails =sourceDestinationFieldsDetails.fillna('-1')
  logTaskProgress(cursor,batchTaskId,"Executed high and low level store procedures")
except Exception as e:
  errorMessage="Exception occured while execution of object high and low level stored procedures: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Specify schema For High Level and Low Level store proc by calling funciton
#call spExecHighAndLowLevel function to retrieve values from store procedure and store in pandas dataframe                                         
try:        
  #Schema for High level stor proc as pandas stores different null values for different types having an issue while conveting to spark df for infering types for null columns.
  sourceDestinationFieldsDetailsSchema = getStructForHighAndLowLevelDf()
  logTaskProgress(cursor,batchTaskId,"Got the schema for destination field details")
except Exception as e:
  errorMessage="Exception occured while executing function getStructForHighAndLowLevelDf: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 


# COMMAND ----------

# DBTITLE 1,Transform metadata into spark dataframe
#call Transform_pd_df_to_Pyspark_df function to convert pandas to pyspark dataframe and store return values in variables
try:
  srcAndDesTableDF,srcAndDesFieldsDF = convertPandasToSparkDf(sourceDestinationTableDetails,
                                                                  sourceDestinationFieldsDetails,
                                                                  sourceDestinationFieldsDetailsSchema,
                                                                  cursor,
                                                                  batchTaskId,
                                                                  adfPipelineName,
                                                                  clusterId,
                                                                  notebookName,
                                                                  errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,"Converted sourceDestinationTableDetails and sourceDestinationFieldsDetails pandas to spark dataframes")
except Exception as e:
  errorMessage="Unable to convert pandas to spark dataframe: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Get dataframe values into variables
try:
  #collect dataframe row as a dictionary
  srcDesDict = srcAndDesTableDF.collect()[0].asDict()
  #the list of source columns that are required for destination e.g. ["name as firstname", "dob as dateofbirth"]
  sourceColumns = srcAndDesFieldsDF.agg(collect_list(srcAndDesFieldsDF.source_as_destination)).collect()[0][0]
  #the id of the source object taken from teh procedure output
  sourceObjectId = srcDesDict['source_object_id']  
  #the name of the source taken from the procedure output
  sourceTableName = srcDesDict['source_object_name']
  #the calculated where expression to be applied in source dataframe
  whereExpression = srcDesDict['source_where_clause']
  #the xml start string to be applied when calculating xml fields
  xmlStartString = srcDesDict['source_XML_string_prefix']
  #the id of the target object taken from the procedure ouptut
  targetObjectId=srcDesDict['destination_object_id']
  #the name of the target table taken from the procedure output
  targetObjectName=srcDesDict['destination_object_name']
  #the format of the target taken from the procedure output
  targetFormat=srcDesDict['destination_format']  
  
  logTaskProgress(cursor,batchTaskId,"Got dataframe values into variables")
except Exception as e:
  errorMessage="Unable to retrieve the dataframe values as variables: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Set view sql into individual variables
#prepare variables for sql views
vwSourceStagingReferenceunstructuredSourceDirectPolicyAdminPolicy = """SELECT    policy.Policy_ID                     AS Policy_ID
                                                                               , policy.Policy_Status_Code            AS Policy_Status_Code 
                                                                               , policy.StartDate                     AS StartDate
                                                                               , COALESCE(CancellationDate, EndDate,StartDate)  AS EndDate
                                                                       FROM Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvPolicy as policy
                                                                        """

vwSourceStagingReferenceunstructuredSourceDirectPolicyAdminClaim = """SELECT   /*+ BROADCAST(policy) */
                                                                                claim.CLMPTYRP_ID                    AS CLMPTYRP_ID
                                                                              , COALESCE(CancellationDate, EndDate,StartDate)  AS EndDate
                                                                       FROM Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvPolicy as policy
                                                                       INNER JOIN Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvClaim as claim
                                                                               ON policy.Policy_ID = claim.Policy_ID
                                                                    """
vwSourceStagingReferenceunstructuredSourceDirectPolicyAdminAccount        =                       """WITH acc AS ( SELECT     Account_Entry_ID, Policy_ID FROM Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvAccount
                                                                                    UNION ALL
                                                                                    SELECT Dbl_Account_Entry_ID, Policy_ID FROM Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvAccountDbl
                                                                                    )
                                                                      SELECT/*+ BROADCAST(policy) */
                                                                      DISTINCT acc.Account_Entry_ID                               AS Account_Entry_ID
                                                                             , policy.Policy_Status_Code                          AS Policy_Status_Code 
                                                                             , policy.StartDate                                   AS StartDate
                                                                             , COALESCE(policy.CancellationDate, policy.EndDate,policy.StartDate)  AS EndDate
                                                                      FROM Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvPolicy AS policy
                                                                      INNER JOIN acc 
                                                                         ON policy.Policy_ID = acc.Policy_ID"""

vwSourceStagingReferenceunstructuredSourceDirectPolicyAdminRisk            =      """-- find the Risk's policy holder's party id from named party and pick the best policy across all those parties.
-- can't just find the best policy for a risk, as not all risks are in business event


/* This query finds the best Policy for each Party that is a proposer, by choosing:
 *    1. The Policy with the latest end date for each Party.
 *    2. If more than one has the same start date, choose the one that has the "most complete" status - see list in statuses for ranking of status codes
 *    3. Associate that Party's dates with all Risk IDs proposed by that proposer.
 */

WITH policy_parties AS (SELECT DISTINCT np.Party_ID AS Party_ID -- for each party id, get all policy IDs associated with the party id
                                        , be.DoneToPolicy_ID AS Policy_ID -- includes duplicates as a party can have many policies, and a policy many parties
                                   FROM Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvBusEvent be
                                   JOIN Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvNamedParty np
                                     ON be.DoneToRisk_ID = np.AssociatedRisk_ID
                                  WHERE be.DoneToPolicy_ID IS NOT NULL
                                    AND be.DoneToRisk_ID IS NOT NULL 
                                    AND np.Party_ID IS NOT NULL
                                    AND np.RELTOPROPOSER_CODE = 'Proposer' -- only proposers as there is always one proposer party per risk - we only need to find their policy details.
)
           
, party_policy_with_duplicates AS (SELECT DISTINCT pp.Party_ID -- now get the status and start date of every policy associated with each party
                                                 , pp.Policy_ID
                                                 , pol.StartDate Start_Date
                                                 , pol.Policy_Status_Code
                                              FROM policy_parties pp
                                              JOIN Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvPolicy pol
                                                ON pp.Policy_ID = pol.Policy_ID)
 
, party_policy_latest_start as (SELECT ppwd.Party_ID -- for each party, find the latest start date associated with it
                                     , MAX(ppwd.Start_Date) Latest_Start_Date -- most recent start date is the frist stage of the tie breaker.
                                  FROM party_policy_with_duplicates ppwd
                              GROUP BY ppwd.Party_ID)
                             
, party_policy_duplicates_ranked AS (SELECT ppwd.Party_ID
                                          , ppwd.Policy_ID
                                          , RANK() OVER (PARTITION
                                                                BY ppls.Party_ID
                                                             ORDER
                                                                BY CASE WHEN ppwd.Policy_Status_Code = 'ConfTrans'  THEN 15
                                                                        WHEN ppwd.Policy_Status_Code = 'Confirmed'  THEN 11
                                                                        WHEN ppwd.Policy_Status_Code = 'InRenCycle' THEN 13
                                                                        WHEN ppwd.Policy_Status_Code = 'NewBus'     THEN 14
                                                                        WHEN ppwd.Policy_Status_Code = 'Renewed'    THEN 12
                                                                        WHEN ppwd.Policy_Status_Code = 'Lapsed'     THEN 22
                                                                        WHEN ppwd.Policy_Status_Code = 'Cancelled'  THEN 23
                                                                        WHEN ppwd.Policy_Status_Code = 'Replaced'   THEN 31
                                                                        WHEN ppwd.Policy_Status_Code = 'Quote'      THEN 32
                                                                        WHEN ppwd.Policy_Status_Code = 'Prequote'   THEN 33
                                                                                                                    ELSE 50 END ) AS Policy_Rank
                                       FROM party_policy_with_duplicates ppwd
                                       JOIN party_policy_latest_start ppls
                                         ON ppwd.Party_ID = ppls.Party_ID
                                        AND ppwd.Start_Date = ppls.Latest_Start_Date) -- for each party, get only those policies associated with latest start date for the party, and rank those policies by status
                                        
-- now select the details of each party's best policy and apply it to all risks that are the party is the proposer of:
SELECT np.AssociatedRisk_ID AS Risk_ID 
     , pol.Policy_Status_Code 
     , pol.StartDate
     , MAX(COALESCE(pol.CancellationDate, pol.EndDate, pol.StartDate)) EndDate -- max because if 2 or more policies have the same status and start date, takethe one that ran for the longest.
  FROM party_policy_duplicates_ranked pp
  JOIN Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvPolicy pol
    ON pp.Policy_ID = pol.Policy_ID
  JOIN Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvNamedParty np
    ON pp.Party_ID = np.Party_ID
 WHERE pp.Policy_Rank = 1
   AND np.RELTOPROPOSER_CODE = 'Proposer'
 GROUP BY np.AssociatedRisk_ID
        , pol.Policy_Status_Code 
        , pol.StartDate"""

vwSourceStagingReferenceunstructuredSourceDirectPolicyAdminPartyInstance = """WITH business_event_risks AS (SELECT DISTINCT be.DoneToRisk_ID   AS Risk_ID 
                                                                                                                , be.DoneToPolicy_ID AS Policy_ID -- includesa small number of duplicates - these will be tie-broken by Party-Instance / Policy_ID duplicate tiebreaker below.
                                                                                                    FROM Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvBusEvent be
                                                                                                   WHERE be.DoneToPolicy_ID IS NOT NULL
                                                                                                     AND be.DoneToRisk_ID IS NOT NULL )
                                                                    -- some risks and policies are not in business events, so these need to be found by taking the claims and removing any where the risk is already is in burinss event risks
                                                                    , claim_risks AS (SELECT DISTINCT  -- all risks associated with Claims except rhose alrady dealt with by business events:
                                                                                                      claim.RISK_ID         AS Risk_ID
                                                                                                    , claim.Policy_ID       AS Policy_ID -- this will include duplicates where the same risk is applied to several policies
                                                                                       FROM Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvClaim claim              
                                                                                       LEFT
                                                                                       JOIN business_event_risks be
                                                                                         ON be.Risk_ID = claim.RISK_ID
                                                                                      WHERE be.Risk_ID IS NULL)  -- dont include risks where the policy was already identified via business event, as that is more reliable.

                                                                    , pi_policy_with_duplcates as   (SELECT  /*+ BROADCAST(pol) */
                                                                                                            np.PartyInstance_ID
                                                                                                          , ber.Policy_ID
                                                                                                          , pol.StartDate Start_Date
                                                                                                          , pol.Policy_Status_Code
                                                                                                       FROM business_event_risks ber
                                                                                                       JOIN Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvPolicy pol
                                                                                                         ON ber.Policy_ID = pol.Policy_ID
                                                                                                       JOIN Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvNamedParty np -- this join introduces more duplicates when the party instance is an organisation - but rules are same as risk - latest start date then prioritise by status code.
                                                                                                         ON ber.Risk_ID = np.AssociatedRisk_ID
                                                                                                  UNION -- not union all as duplicates from either half to be eliminated.
                                                                                                     SELECT  /*+ BROADCAST(pol)*/
                                                                                                     np.PartyInstance_ID
                                                                                                          , cr.Policy_ID
                                                                                                          , pol.StartDate Start_Date
                                                                                                          , pol.Policy_Status_Code
                                                                                                       FROM claim_risks cr
                                                                                                       JOIN Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvPolicy pol
                                                                                                         ON cr.Policy_ID = pol.Policy_ID
                                                                                                       JOIN Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvNamedParty np -- this join introduces more duplicates when the party instance is an organisation - but rules are same as risk - latest start date then prioritise by status code.
                                                                                                         ON cr.Risk_ID = np.AssociatedRisk_ID)

                                                                    , pi_policy_latest_start as   (SELECT pipwd.PartyInstance_ID
                                                                                                        , MAX(pipwd.Start_Date) Latest_Start_Date -- most recent start date is the frist stage of the tie breaker.
                                                                                                     FROM pi_policy_with_duplcates pipwd
                                                                                                 GROUP BY pipwd.PartyInstance_ID)

                                                                    , pi_policy_duplicates_ranked   AS (SELECT  
                                                                                                               pipwd.PartyInstance_ID
                                                                                                             , pipwd.Policy_ID
                                                                                                             , RANK() OVER (PARTITION
                                                                                                                                   BY pipls.PartyInstance_ID
                                                                                                                                ORDER
                                                                                                                                   BY CASE WHEN pipwd.Policy_Status_Code = 'ConfTrans'  THEN 15
                                                                                                                                           WHEN pipwd.Policy_Status_Code = 'Confirmed'  THEN 11
                                                                                                                                           WHEN pipwd.Policy_Status_Code = 'InRenCycle' THEN 13
                                                                                                                                           WHEN pipwd.Policy_Status_Code = 'NewBus'     THEN 14
                                                                                                                                           WHEN pipwd.Policy_Status_Code = 'Renewed'    THEN 12
                                                                                                                                           WHEN pipwd.Policy_Status_Code = 'Cancelled'  THEN 22
                                                                                                                                           WHEN pipwd.Policy_Status_Code = 'Lapsed'     THEN 22
                                                                                                                                           WHEN pipwd.Policy_Status_Code = 'Replaced'   THEN 31
                                                                                                                                           WHEN pipwd.Policy_Status_Code = 'Prequote'   THEN 32
                                                                                                                                                                                         ELSE 50 END ) AS Policy_Rank
                                                                                                          FROM pi_policy_with_duplcates pipwd
                                                                                                          JOIN pi_policy_latest_start pipls
                                                                                                            ON pipwd.PartyInstance_ID = pipls.PartyInstance_ID
                                                                                                           AND pipwd.Start_Date = pipls.Latest_Start_Date) -- get only those polices associated with the party instance who share the latest start date

                                                                    -- ,cte_Test as(                                          
                                                                    -- now select the details of each party instance's best policy:
                                                                    SELECT  /*+ BROADCAST(pol) */
                                                                           PartyInstance_ID
                                                                         , Policy_Status_Code 
                                                                         , StartDate
                                                                         , COALESCE(CancellationDate, EndDate, StartDate) AS EndDate
                                                                      FROM pi_policy_duplicates_ranked pip
                                                                      JOIN Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvPolicy pol
                                                                        ON pip.Policy_ID = pol.Policy_ID
                                                                     WHERE pip.Policy_Rank = 1 

                                                                     UNION
                                                                       ALL
                                                                    SELECT op.Person_ID AS PartyInstance_ID
                                                                         , 'Operator'   AS Policy_Status_Code         
                                                                         , NULL         AS StartDate
                                                                         , op.EndDate   AS EndDate
                                                                      FROM Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvOperatorDetails op
"""

                                                                   
vwSourceStagingReferenceunstructuredSourceDirectPolicyAdminParty = """/* Party is the most important query - the logic to find the best Policy for a Party (across all its instances) is then reused for:
 * - Party Instance (the party's best policy is applied to all instances of the party)
 * - Risk (The party's best policy is applied to all of its risks)
 * - 
 */


/* This query finds the best Policy for each Party, by choosing:
 *    1. The Policy with the latest end date for each Party.
 *    2. If more than one has the same start date, choose the one that has the "most complete" status - see list in statuses for ranking of status codes
 */

WITH policy_parties AS (SELECT DISTINCT np.Party_ID AS Party_ID -- for each party id, get all policy IDs associated with the party id
                                        , be.DoneToPolicy_ID AS Policy_ID -- includes duplicates as a party can have many policies, and a policy many parties
                                   FROM Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvBusEvent be
                                   JOIN Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvNamedParty np
                                     ON be.DoneToRisk_ID = np.AssociatedRisk_ID
                                  WHERE be.DoneToPolicy_ID IS NOT NULL
                                    AND be.DoneToRisk_ID IS NOT NULL 
                                    AND np.Party_ID IS NOT NULL)
           
, party_policy_with_duplicates AS (SELECT DISTINCT pp.Party_ID -- now get the status and start date of every policy associated with each party
                                                , pp.Policy_ID
                                                , pol.StartDate Start_Date
                                                , pol.Policy_Status_Code
                                             FROM policy_parties pp
                                             JOIN Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvPolicy pol
                                               ON pp.Policy_ID = pol.Policy_ID)
 
, party_policy_latest_start as (SELECT ppwd.Party_ID -- for each party, find the lastest start date associated with it
                                     , MAX(ppwd.Start_Date) Latest_Start_Date -- most recent start date is the frist stage of the tie breaker.
                                  FROM party_policy_with_duplicates ppwd
                              GROUP BY ppwd.Party_ID)
                             
, party_policy_duplicates_ranked AS (SELECT ppwd.Party_ID
                                          , ppwd.Policy_ID
                                          , RANK() OVER (PARTITION
                                                                BY ppls.Party_ID
                                                             ORDER
                                                                BY CASE WHEN ppwd.Policy_Status_Code = 'ConfTrans'  THEN 15
                                                                        WHEN ppwd.Policy_Status_Code = 'Confirmed'  THEN 11
                                                                        WHEN ppwd.Policy_Status_Code = 'InRenCycle' THEN 13
                                                                        WHEN ppwd.Policy_Status_Code = 'NewBus'     THEN 14
                                                                        WHEN ppwd.Policy_Status_Code = 'Renewed'    THEN 12
                                                                        WHEN ppwd.Policy_Status_Code = 'Lapsed'     THEN 23 -- lapsed promoted above cancelled, as the cancelled and lapsed starting on the same day - lapsed almost vertainly replaced the cancelled policy
                                                                        WHEN ppwd.Policy_Status_Code = 'Cancelled'  THEN 22
                                                                        WHEN ppwd.Policy_Status_Code = 'Replaced'   THEN 31
                                                                        WHEN ppwd.Policy_Status_Code = 'Quote'      THEN 32
                                                                        WHEN ppwd.Policy_Status_Code = 'Prequote'   THEN 33
                                                                                                                    ELSE 50 END ) AS Policy_Rank
                                       FROM party_policy_with_duplicates ppwd
                                       JOIN party_policy_latest_start ppls
                                         ON ppwd.Party_ID = ppls.Party_ID
                                        AND ppwd.Start_Date = ppls.Latest_Start_Date) -- for each party, get only those policies associated with latest start date for the party, and rank those policies by status
                                      

   SELECT pp.Party_ID -- now select the details of each party's best policy:
        , pol.Policy_Status_Code 
        , pol.StartDate
        , MAX(COALESCE(pol.CancellationDate, pol.EndDate, pol.StartDate)) EndDate -- max because if 2 policies have the same status and start date, takethe one that ran for the longest.

     FROM party_policy_duplicates_ranked pp
     JOIN Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvPolicy pol
       ON pp.Policy_ID = pol.Policy_ID
    WHERE pp.Policy_Rank = 1
 GROUP BY pp.Party_ID
        , pol.Policy_Status_Code 
        , pol.StartDate """

vwSourceStagingReferenceunstructuredSourceDirectPolicyAdminPolicyParty = """-- The logic behind vw_Source_Staging_Reference_unstructuredSource_Direct_PolicyAdmin_Party in the second half
-- of this query did change, and the new logic should be swapped into this view too.
-- The logic for the Policy half of this query did not change with the Febriraru 2021 updates.

WITH Policy AS (
    SELECT 
           policy.Policy_ID                                AS Policy_ID
         , policy.Policy_Status_Code                       AS Policy_Status_Code 
         , policy.StartDate                                AS StartDate
         , COALESCE(CancellationDate, EndDate, StartDate)  AS EndDate
      FROM Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvPolicy as policy
),

-- The below code copied from the vw_Source_Staging_Reference_unstructuredSource_Direct_PolicyAdmin_Party
policy_parties AS (SELECT DISTINCT np.Party_ID AS Party_ID -- for each party id, get all policy IDs associated with the party id
                                        , be.DoneToPolicy_ID AS Policy_ID -- includes duplicates as a party can have many policies, and a policy many parties
                                   FROM Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvBusEvent be
                                   JOIN Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvNamedParty np
                                     ON be.DoneToRisk_ID = np.AssociatedRisk_ID
                                  WHERE be.DoneToPolicy_ID IS NOT NULL
                                    AND be.DoneToRisk_ID IS NOT NULL 
                                    AND np.Party_ID IS NOT NULL)
           
, party_policy_with_duplicates AS (SELECT DISTINCT pp.Party_ID -- now get the status and start date of every policy associated with each party
                                                , pp.Policy_ID
                                                , pol.StartDate Start_Date
                                                , pol.Policy_Status_Code
                                             FROM policy_parties pp
                                             JOIN Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvPolicy pol
                                               ON pp.Policy_ID = pol.Policy_ID)
 
, party_policy_latest_start as (SELECT ppwd.Party_ID -- for each party, find the lastest start date associated with it
                                     , MAX(ppwd.Start_Date) Latest_Start_Date -- most recent start date is the frist stage of the tie breaker.
                                  FROM party_policy_with_duplicates ppwd
                              GROUP BY ppwd.Party_ID)
                             
, party_policy_duplicates_ranked AS (SELECT ppwd.Party_ID
                                          , ppwd.Policy_ID
                                          , RANK() OVER (PARTITION
                                                                BY ppls.Party_ID
                                                             ORDER
                                                                BY CASE WHEN ppwd.Policy_Status_Code = 'ConfTrans'  THEN 15
                                                                        WHEN ppwd.Policy_Status_Code = 'Confirmed'  THEN 11
                                                                        WHEN ppwd.Policy_Status_Code = 'InRenCycle' THEN 13
                                                                        WHEN ppwd.Policy_Status_Code = 'NewBus'     THEN 14
                                                                        WHEN ppwd.Policy_Status_Code = 'Renewed'    THEN 12
                                                                        WHEN ppwd.Policy_Status_Code = 'Lapsed'     THEN 23 -- lapsed promoted above cancelled, as the cancelled and lapsed starting on the same day - lapsed almost vertainly replaced the cancelled policy
                                                                        WHEN ppwd.Policy_Status_Code = 'Cancelled'  THEN 22
                                                                        WHEN ppwd.Policy_Status_Code = 'Replaced'   THEN 31
                                                                        WHEN ppwd.Policy_Status_Code = 'Quote'      THEN 32
                                                                        WHEN ppwd.Policy_Status_Code = 'Prequote'   THEN 33
                                                                                                                    ELSE 50 END ) AS Policy_Rank
                                       FROM party_policy_with_duplicates ppwd
                                       JOIN party_policy_latest_start ppls
                                         ON ppwd.Party_ID = ppls.Party_ID
                                        AND ppwd.Start_Date = ppls.Latest_Start_Date) -- for each party, get only those policies associated with latest start date for the party, and rank those policies by status
                                           
,Party AS (
   SELECT pp.Party_ID -- now select the details of each party's best policy:
        , pol.Policy_Status_Code 
        , pol.StartDate
        , MAX(COALESCE(pol.CancellationDate, pol.EndDate, pol.StartDate)) EndDate -- max because if 2 policies have the same status and start date, takethe one that ran for the longest.

     FROM party_policy_duplicates_ranked pp
     JOIN Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvPolicy pol
       ON pp.Policy_ID = pol.Policy_ID
    WHERE pp.Policy_Rank = 1
 GROUP BY pp.Party_ID
        , pol.Policy_Status_Code 
        , pol.StartDate
        
        )      
    SELECT
           CONCAT('Policy_' , CAST(policy.Policy_ID as STRING)) AS Policy_Party_ID
         , policy.Policy_Status_Code                     AS Policy_Status_Code 
         , policy.StartDate                              AS StartDate
         , policy.EndDate                                AS EndDate
      FROM Policy as policy
 UNION ALL
    SELECT 
           CONCAT('Party_' , CAST(party.Party_ID as STRING))   AS Policy_Party_ID
         , party.Policy_Status_Code                     AS Policy_Status_Code 
         , party.StartDate                              AS StartDate
         , party.EndDate                                AS EndDate
      FROM Party as party  """

vwSourceStagingReferenceunstructuredSourceDirectPolicyAdminWebUser = """/* This query finds the best Policy for each Web user, by choosing:
 *    1. The Policy with the latest end date for each Party.
 *    2. If more than one has the same start date, choose the one that has the "most complete" status - see list in statuses for ranking of status codes
 *    3. Then associate each party with all of its party instances (people only as web users are always person not organisation)
 *    4. Finally join back to web user using its most recent party instance
 */


WITH policy_parties AS (SELECT DISTINCT np.Party_ID AS Party_ID -- for each party id, get all policy IDs associated with the party id
                                      , be.DoneToPolicy_ID AS Policy_ID -- includes duplicates as a party can have many policies, and a policy many parties
                                   FROM Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvBusEvent be
                                   JOIN Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvNamedParty np
                                     ON be.DoneToRisk_ID = np.AssociatedRisk_ID
                                  WHERE be.DoneToPolicy_ID IS NOT NULL
                                    AND be.DoneToRisk_ID IS NOT NULL 
                                    AND np.Party_ID IS NOT NULL) 
            
, party_policy_with_duplicates AS (SELECT DISTINCT pp.Party_ID -- now get the status and start date of every policy associated with each party
                                                 , pp.Policy_ID
                                                 , pol.StartDate Start_Date
                                                 , pol.Policy_Status_Code
                                              FROM policy_parties pp
                                              JOIN Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvPolicy pol
                                                ON pp.Policy_ID = pol.Policy_ID)

, party_policy_latest_start as (SELECT ppwd.Party_ID -- for each party, find the lastest start date associated with it
                                     , MAX(ppwd.Start_Date) Latest_Start_Date -- most recent start date is the frist stage of the tie breaker.
                                  FROM party_policy_with_duplicates ppwd
                              GROUP BY ppwd.Party_ID)                          
, party_policy_duplicates_ranked AS (SELECT ppwd.Party_ID
                                          , ppwd.Policy_ID
                                          , RANK() OVER (PARTITION
                                                                BY ppls.Party_ID
                                                             ORDER
                                                                BY CASE WHEN ppwd.Policy_Status_Code = 'ConfTrans'  THEN 15
                                                                        WHEN ppwd.Policy_Status_Code = 'Confirmed'  THEN 11
                                                                        WHEN ppwd.Policy_Status_Code = 'InRenCycle' THEN 13
                                                                        WHEN ppwd.Policy_Status_Code = 'NewBus'     THEN 14
                                                                        WHEN ppwd.Policy_Status_Code = 'Renewed'    THEN 12
                                                                        WHEN ppwd.Policy_Status_Code = 'Lapsed'     THEN 22
                                                                        WHEN ppwd.Policy_Status_Code = 'Cancelled'  THEN 23
                                                                        WHEN ppwd.Policy_Status_Code = 'Replaced'   THEN 31
                                                                        WHEN ppwd.Policy_Status_Code = 'Quote'      THEN 32
                                                                        WHEN ppwd.Policy_Status_Code = 'Prequote'   THEN 33
                                                                                                                    ELSE 50 END ) AS Policy_Rank
                                       FROM party_policy_with_duplicates ppwd
                                       JOIN party_policy_latest_start ppls
                                         ON ppwd.Party_ID = ppls.Party_ID
                                        AND ppwd.Start_Date = ppls.Latest_Start_Date) -- for each party, get only those policies associated with latest start date for the party, and rank those policies by status
                                        
 , Party_PartyInstance_Relationship as (SELECT per.Party_ID
                                             , per.Person_ID AS PartyInstance_ID
                                          FROM Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvPersonDetails per)  -- no need for organisations - only people required for web users.  


-- each web users can have several party instances - find the latest one:
, latestWebUserVersion AS ( SELECT WEBUSER_UID
                               , MAX(WEBU_PAR_REL_UPDATED_DATE) WEBU_PAR_REL_UPDATED_DATE 
                            FROM Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvWebUserPartyRel 
                        GROUP BY WEBUSER_UID)
,    latestWUTieBreak AS (
    SELECT wu.WEBUSER_UID -- some web users have two Party Relationships created and updated with identical timestamps.  Have to pick one.
                               , MAX(PERSON_ID) PERSON_ID
                            FROM Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvWebUserPartyRel wu
                      INNER JOIN latestWebUserVersion latest 
                              ON wu.WEBUSER_UID = latest.WEBUSER_UID
                             AND wu.WEBU_PAR_REL_UPDATED_DATE = latest.WEBU_PAR_REL_UPDATED_DATE 
                        GROUP BY wu.WEBUSER_UID)             
                    
-- now select the details of each party's best policy and apply it to all web users
-- that are linked to at least one of the instances of that party:

  SELECT wu.WEBUSER_UID AS WebUser_ID
       , pol.Policy_Status_Code 
       , pol.StartDate
       , MAX(COALESCE(pol.CancellationDate, pol.EndDate, pol.StartDate)) EndDate -- max because if 2 or more policies have the same status and start date, takethe one that ran for the longest.

    FROM party_policy_duplicates_ranked pp
    JOIN Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvPolicy pol
      ON pp.Policy_ID = pol.Policy_ID
    JOIN Party_PartyInstance_Relationship pa
      ON pp.Party_ID = pa.Party_ID
    JOIN latestWUTieBreak wu
      ON pa.PartyInstance_ID = wu.PERSON_ID
   WHERE pp.Policy_Rank = 1
GROUP BY wu.WEBUSER_UID
       , pol.Policy_Status_Code 
       , pol.StartDate
"""

# COMMAND ----------

vwSourceStagingReferenceunstructuredSourceDirectPolicyAdminPartyInstance = """/* This query finds the best Policy for each Party Instance, by choosing:
 *    1. The Policy with the latest end date for each Party.
 *    2. If more than one has the same start date, choose the one that has the "most complete" status - see list in statuses for ranking of status codes
 *    3. Then associate each party with all of its party instances
 *    4. Finally add the operators via a UNION ALL
 */


WITH policy_parties AS (SELECT DISTINCT np.Party_ID AS Party_ID -- for each party id, get all policy IDs associated with the party id
                                      , be.DoneToPolicy_ID AS Policy_ID -- includes duplicates as a party can have many policies, and a policy many parties
                                   FROM Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvBusEvent be
                                   JOIN Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvNamedParty np
                                     ON be.DoneToRisk_ID = np.AssociatedRisk_ID
                                  WHERE be.DoneToPolicy_ID IS NOT NULL
                                    AND be.DoneToRisk_ID IS NOT NULL 
                                    AND np.Party_ID IS NOT NULL)
               
, party_policy_with_duplicates AS (SELECT DISTINCT pp.Party_ID -- now get the status and start date of every policy associated with each party
                                                 , pp.Policy_ID
                                                 , pol.StartDate Start_Date
                                                 , pol.Policy_Status_Code
                                              FROM policy_parties pp
                                              JOIN Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvPolicy pol
                                                ON pp.Policy_ID = pol.Policy_ID)
 
, party_policy_latest_start as (SELECT ppwd.Party_ID -- for each party, find the lastest start date associated with it
                                     , MAX(ppwd.Start_Date) Latest_Start_Date -- most recent start date is the frist stage of the tie breaker.
                                  FROM party_policy_with_duplicates ppwd
                              GROUP BY ppwd.Party_ID)
                                  
, party_policy_duplicates_ranked AS (SELECT ppwd.Party_ID
                                          , ppwd.Policy_ID
                                          , RANK() OVER (PARTITION
                                                                BY ppls.Party_ID
                                                             ORDER
                                                                BY CASE WHEN ppwd.Policy_Status_Code = 'ConfTrans'  THEN 15
                                                                        WHEN ppwd.Policy_Status_Code = 'Confirmed'  THEN 11
                                                                        WHEN ppwd.Policy_Status_Code = 'InRenCycle' THEN 13
                                                                        WHEN ppwd.Policy_Status_Code = 'NewBus'     THEN 14
                                                                        WHEN ppwd.Policy_Status_Code = 'Renewed'    THEN 12
                                                                        WHEN ppwd.Policy_Status_Code = 'Lapsed'     THEN 22
                                                                        WHEN ppwd.Policy_Status_Code = 'Cancelled'  THEN 23
                                                                        WHEN ppwd.Policy_Status_Code = 'Replaced'   THEN 31
                                                                        WHEN ppwd.Policy_Status_Code = 'Quote'      THEN 32
                                                                        WHEN ppwd.Policy_Status_Code = 'Prequote'   THEN 33
                                                                                                                    ELSE 50 END ) AS Policy_Rank
                                       FROM party_policy_with_duplicates ppwd
                                       JOIN party_policy_latest_start ppls
                                         ON ppwd.Party_ID = ppls.Party_ID
                                        AND ppwd.Start_Date = ppls.Latest_Start_Date) -- for each party, get only those policies associated with latest start date for the party, and rank those policies by status
                                    
 , Party_PartyInstance_Relationship as (SELECT per.Party_ID
                                             , per.Person_ID AS PartyInstance_ID
                                          FROM Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvPersonDetails per
                                     UNION ALL 
                                        SELECT org.Party_ID
                                             , org.Organisation_ID AS PartyInstance_ID
                                          FROM Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvOrganisation org)  
                                      
                                     
-- now select the details of each party's best policy and apply it to all instances of that party:
SELECT pa.PartyInstance_ID
     , pol.Policy_Status_Code 
     , pol.StartDate
     , MAX(COALESCE(pol.CancellationDate, pol.EndDate, pol.StartDate)) EndDate -- max because if 2 or more policies have the same status and start date, takethe one that ran for the longest.
  FROM party_policy_duplicates_ranked pp
  JOIN Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvPolicy pol
    ON pp.Policy_ID = pol.Policy_ID
  JOIN Party_PartyInstance_Relationship pa
    ON pp.Party_ID = pa.Party_ID
 WHERE pp.Policy_Rank = 1
 GROUP BY pa.PartyInstance_ID
        , pol.Policy_Status_Code 
        , pol.StartDate
 UNION
   ALL
SELECT op.Person_ID AS PartyInstance_ID
     , 'Operator'   AS Policy_Status_Code         
     , NULL         AS StartDate
     , op.EndDate   AS EndDate
  FROM Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvOperatorDetails op
"""

# COMMAND ----------

# DBTITLE 1,source Policy Admin
vwSourceStagingReferenceunstructuredSourceunstructuredSourcePolicyAdminPolicy=vwSourceStagingReferenceunstructuredSourceDirectPolicyAdminPolicy.replace('Direct','unstructuredSource')
vwSourceStagingReferenceunstructuredSourceunstructuredSourcePolicyAdminClaim=vwSourceStagingReferenceunstructuredSourceDirectPolicyAdminClaim.replace('Direct','unstructuredSource')
vwSourceStagingReferenceunstructuredSourceunstructuredSourcePolicyAdminAccount=vwSourceStagingReferenceunstructuredSourceDirectPolicyAdminAccount.replace('Direct','unstructuredSource')
vwSourceStagingReferenceunstructuredSourceunstructuredSourcePolicyAdminRisk=vwSourceStagingReferenceunstructuredSourceDirectPolicyAdminRisk.replace('Direct','unstructuredSource')
vwSourceStagingReferenceunstructuredSourceunstructuredSourcePolicyAdminPartyInstance=vwSourceStagingReferenceunstructuredSourceDirectPolicyAdminPartyInstance.replace('Direct','unstructuredSource')
vwSourceStagingReferenceunstructuredSourceunstructuredSourcePolicyAdminParty=vwSourceStagingReferenceunstructuredSourceDirectPolicyAdminParty.replace('Direct','unstructuredSource')
vwSourceStagingReferenceunstructuredSourceunstructuredSourcePolicyAdminPolicyParty=vwSourceStagingReferenceunstructuredSourceDirectPolicyAdminPolicyParty.replace('Direct','unstructuredSource')
vwSourceStagingReferenceunstructuredSourceunstructuredSourcePolicyAdminWebUser=vwSourceStagingReferenceunstructuredSourceDirectPolicyAdminWebUser.replace('Direct','unstructuredSource')

# COMMAND ----------

# DBTITLE 1,Supporting reference table metadata preparation into  Direct objects
#add the view sql variables to view dictionary
sourceViewDictionary = {'vw_Source_Staging_Reference_unstructuredSource_Direct_PolicyAdmin_Policy': vwSourceStagingReferenceunstructuredSourceDirectPolicyAdminPolicy}
sourceViewDictionary.update({'vw_Source_Staging_Reference_unstructuredSource_Direct_PolicyAdmin_Claim': vwSourceStagingReferenceunstructuredSourceDirectPolicyAdminClaim})
sourceViewDictionary.update({'vw_Source_Staging_Reference_unstructuredSource_Direct_PolicyAdmin_Account': vwSourceStagingReferenceunstructuredSourceDirectPolicyAdminAccount})
sourceViewDictionary.update({'vw_Source_Staging_Reference_unstructuredSource_Direct_PolicyAdmin_Risk': vwSourceStagingReferenceunstructuredSourceDirectPolicyAdminRisk})
sourceViewDictionary.update({'vw_Source_Staging_Reference_unstructuredSource_Direct_PolicyAdmin_PartyInstance': vwSourceStagingReferenceunstructuredSourceDirectPolicyAdminPartyInstance})
sourceViewDictionary.update({'vw_Source_Staging_Reference_unstructuredSource_Direct_PolicyAdmin_Party': vwSourceStagingReferenceunstructuredSourceDirectPolicyAdminParty})
sourceViewDictionary.update({'vw_Source_Staging_Reference_unstructuredSource_Direct_PolicyAdmin_Policy_Party': vwSourceStagingReferenceunstructuredSourceDirectPolicyAdminPolicyParty})
sourceViewDictionary.update({'vw_Source_Staging_Reference_unstructuredSource_Direct_PolicyAdmin_WebUser': vwSourceStagingReferenceunstructuredSourceDirectPolicyAdminWebUser})
#prepare dictionaries for dependent tables
dependentTablesDict = {'Staging_Reference_unstructuredSource_Direct_PolicyAdmin_Policy': ['Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvPolicy']}
dependentTablesDict.update({'Staging_Reference_unstructuredSource_Direct_PolicyAdmin_Claim': ['Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvPolicy','Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvClaim']})
dependentTablesDict.update({'Staging_Reference_unstructuredSource_Direct_PolicyAdmin_Account': ['Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvAccount','Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvAccountDbl','Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvPolicy']})
dependentTablesDict.update({'Staging_Reference_unstructuredSource_Direct_PolicyAdmin_Risk': ['Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvBusEvent','Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvNamedParty','Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvPolicy','Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvNamedParty']})
dependentTablesDict.update({'Staging_Reference_unstructuredSource_Direct_PolicyAdmin_PartyInstance': ['Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvBusEvent','Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvNamedParty','Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvPolicy','Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvPersonDetails','Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvOrganisation','Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvOperatorDetails']})
dependentTablesDict.update({'Staging_Reference_unstructuredSource_Direct_PolicyAdmin_Party': 
['Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvBusEvent','Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvNamedParty','Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvPolicy']})
dependentTablesDict.update({'Staging_Reference_unstructuredSource_Direct_PolicyAdmin_Policy_Party':['Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvPolicy','Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvBusEvent','Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvNamedParty']})
dependentTablesDict.update({'Staging_Reference_unstructuredSource_Direct_PolicyAdmin_WebUser':['Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvBusEvent','Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvNamedParty','Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvPolicy','Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvPersonDetails','Staging_unstructuredSourceDirectPolicyAdmin_CurrentState_MvWebUserPartyRel']})

# COMMAND ----------

# DBTITLE 1,Supporting reference table metadata preparation into source objects
#add the view sql variables to view dictionary
sourceViewDictionary.update({'vw_Source_Staging_Reference_unstructuredSource_unstructuredSource_PolicyAdmin_Policy': vwSourceStagingReferenceunstructuredSourceunstructuredSourcePolicyAdminPolicy})
sourceViewDictionary.update({'vw_Source_Staging_Reference_unstructuredSource_unstructuredSource_PolicyAdmin_Claim': vwSourceStagingReferenceunstructuredSourceunstructuredSourcePolicyAdminClaim})
sourceViewDictionary.update({'vw_Source_Staging_Reference_unstructuredSource_unstructuredSource_PolicyAdmin_Account': vwSourceStagingReferenceunstructuredSourceunstructuredSourcePolicyAdminAccount})
sourceViewDictionary.update({'vw_Source_Staging_Reference_unstructuredSource_unstructuredSource_PolicyAdmin_Risk': vwSourceStagingReferenceunstructuredSourceunstructuredSourcePolicyAdminRisk})
sourceViewDictionary.update({'vw_Source_Staging_Reference_unstructuredSource_unstructuredSource_PolicyAdmin_PartyInstance': vwSourceStagingReferenceunstructuredSourceunstructuredSourcePolicyAdminPartyInstance})
sourceViewDictionary.update({'vw_Source_Staging_Reference_unstructuredSource_unstructuredSource_PolicyAdmin_Party': vwSourceStagingReferenceunstructuredSourceunstructuredSourcePolicyAdminParty})
sourceViewDictionary.update({'vw_Source_Staging_Reference_unstructuredSource_unstructuredSource_PolicyAdmin_Policy_Party': vwSourceStagingReferenceunstructuredSourceunstructuredSourcePolicyAdminPolicyParty})
sourceViewDictionary.update({'vw_Source_Staging_Reference_unstructuredSource_unstructuredSource_PolicyAdmin_WebUser': vwSourceStagingReferenceunstructuredSourceunstructuredSourcePolicyAdminWebUser})
#prepare dictionaries for dependent tables
dependentTablesDict.update({'Staging_Reference_unstructuredSource_unstructuredSource_PolicyAdmin_Policy': ['Staging_unstructuredSourceunstructuredSourcePolicyAdmin_CurrentState_MvPolicy']})
dependentTablesDict.update({'Staging_Reference_unstructuredSource_unstructuredSource_PolicyAdmin_Claim': ['Staging_unstructuredSourceunstructuredSourcePolicyAdmin_CurrentState_MvPolicy','Staging_unstructuredSourceunstructuredSourcePolicyAdmin_CurrentState_MvClaim']})
dependentTablesDict.update({'Staging_Reference_unstructuredSource_unstructuredSource_PolicyAdmin_Account': ['Staging_unstructuredSourceunstructuredSourcePolicyAdmin_CurrentState_MvAccount','Staging_unstructuredSourceunstructuredSourcePolicyAdmin_CurrentState_MvAccountDbl','Staging_unstructuredSourceunstructuredSourcePolicyAdmin_CurrentState_MvPolicy']})
dependentTablesDict.update({'Staging_Reference_unstructuredSource_unstructuredSource_PolicyAdmin_Risk': ['Staging_unstructuredSourceunstructuredSourcePolicyAdmin_CurrentState_MvBusEvent','Staging_unstructuredSourceunstructuredSourcePolicyAdmin_CurrentState_MvNamedParty','Staging_unstructuredSourceunstructuredSourcePolicyAdmin_CurrentState_MvPolicy','Staging_unstructuredSourceunstructuredSourcePolicyAdmin_CurrentState_MvNamedParty']})
dependentTablesDict.update({'Staging_Reference_unstructuredSource_unstructuredSource_PolicyAdmin_PartyInstance': ['Staging_unstructuredSourceunstructuredSourcePolicyAdmin_CurrentState_MvBusEvent','Staging_unstructuredSourceunstructuredSourcePolicyAdmin_CurrentState_MvNamedParty','Staging_unstructuredSourceunstructuredSourcePolicyAdmin_CurrentState_MvPolicy','Staging_unstructuredSourceunstructuredSourcePolicyAdmin_CurrentState_MvPersonDetails','Staging_unstructuredSourceunstructuredSourcePolicyAdmin_CurrentState_MvOrganisation','Staging_unstructuredSourceunstructuredSourcePolicyAdmin_CurrentState_MvOperatorDetails']})
dependentTablesDict.update({'Staging_Reference_unstructuredSource_unstructuredSource_PolicyAdmin_Party': 
['Staging_unstructuredSourceunstructuredSourcePolicyAdmin_CurrentState_MvBusEvent','Staging_unstructuredSourceunstructuredSourcePolicyAdmin_CurrentState_MvNamedParty','Staging_unstructuredSourceunstructuredSourcePolicyAdmin_CurrentState_MvPolicy']})
dependentTablesDict.update({'Staging_Reference_unstructuredSource_unstructuredSource_PolicyAdmin_Policy_Party':['Staging_unstructuredSourceunstructuredSourcePolicyAdmin_CurrentState_MvPolicy','Staging_unstructuredSourceunstructuredSourcePolicyAdmin_CurrentState_MvBusEvent','Staging_unstructuredSourceunstructuredSourcePolicyAdmin_CurrentState_MvNamedParty']})
dependentTablesDict.update({'Staging_Reference_unstructuredSource_unstructuredSource_PolicyAdmin_WebUser':['Staging_unstructuredSourceunstructuredSourcePolicyAdmin_CurrentState_MvBusEvent','Staging_unstructuredSourceunstructuredSourcePolicyAdmin_CurrentState_MvNamedParty','Staging_unstructuredSourceunstructuredSourcePolicyAdmin_CurrentState_MvPolicy','Staging_unstructuredSourceunstructuredSourcePolicyAdmin_CurrentState_MvPersonDetails','Staging_unstructuredSourceunstructuredSourcePolicyAdmin_CurrentState_MvWebUserPartyRel']})

# COMMAND ----------

# DBTITLE 1,Use function to check if there is new data to load
#call identifyIfNewDataInDependentTables to check if there is new data present in dependent tables
try:
  #get the dependent table list from the dictionary
  dependentTableNamesList = dependentTablesDict.get(targetObjectName)
  
  dependentTableIsUpdated = identifyIfNewDataInDependentTables(targetObjectName
                                                               ,dependentTableNamesList
                                                               ,cursor
                                                               ,batchTaskId
                                                               ,adfPipelineName
                                                               ,clusterId
                                                               ,notebookName
                                                               ,errorLogFileLocation)
  
  logTaskProgress(cursor,batchTaskId,"function call made to identify if new data in dependent tables is updated for target object {}".format(targetObjectName))
  
  #if there is no new data in dependent tables then log that we won't do any more work to load it
  if dependentTableIsUpdated == False:
    logTaskProgress(cursor,batchTaskId,"No work required to load target object {} as dependent tables have no new data since it was last loaded".format(targetObjectName))
    
except Exception as e:
  errorMessage="Check if there is new data in dependent tables using function identifyIfNewDataInDependentTables: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Create or replace the temporary view
#call identifyIfNewDataInDependentTables to check if there is new data present in dependent tables
try:
  #if there is new data to load
  if dependentTableIsUpdated == True:
    #get the view definion from the dictionary
    sourceViewDefinition = sourceViewDictionary.get(sourceTableName)
    #call function to create or replace the temporary view
    createOrReplaceTemporaryView(sourceTableName
                                 ,sourceViewDefinition
                                 ,cursor
                                 ,batchTaskId
                                 ,adfPipelineName
                                 ,clusterId
                                 ,notebookName
                                 ,errorLogFileLocation)
    #log that the view was created or replaced
    logTaskProgress(cursor,batchTaskId,"temporary view {} is created".format(sourceTableName))
    
except Exception as e:
  errorMessage="Check if there is new data in dependent tables using function createOrReplaceTemporaryView: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Get Column List Details
#call Get_Select_And_With_Column_Details to get Columnlist tuple
try:
  #if there is new data to load
  if dependentTableIsUpdated == True:
    columnList = getSelectAndWithColumnDetails(srcAndDesFieldsDF
                                               ,cursor 
                                               ,batchTaskId
                                               ,adfPipelineName
                                               ,clusterId
                                               ,notebookName
                                               ,errorLogFileLocation)
    logTaskProgress(cursor,batchTaskId,"successfully called function getSelectAndWithColumnDetails to retrieve the column list")
except Exception as e:
  errorMessage="Unable to retrieve he column Details for With Expression and select: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Get With And Select Column Expressions
try:
  #if there is new data to load
  if dependentTableIsUpdated == True:
    #Get With Column Expression and Select Expression Details by calling the function
    withExpressionListFinal,columnListFinal=getWithExpressionsAndSelectList(columnList
                                                                            ,xmlStartString
                                                                            ,cursor
                                                                            ,batchTaskId
                                                                            ,adfPipelineName
                                                                            ,clusterId
                                                                            ,notebookName
                                                                            ,errorLogFileLocation)
    
    #Prepare select expression
    selectExpression=[item[1] for item in columnListFinal]
    
    logTaskProgress(cursor,batchTaskId,"with column and select column expressions are created")
    
except Exception as e:
  errorMessage="Unable to retrieve with and selectedExpressions: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Selection of destination columns from source
# call select_dest_from_src function to select destination column from the source and store return values in a variable.
try:
  #if there is new data to load
  if dependentTableIsUpdated == True:
    sourceSelectWhereDF = selectDestFromSrcTable(sourceTableName
                                                 ,withExpressionListFinal
                                                 ,selectExpression
                                                 ,whereExpression
                                                 ,cursor
                                                 ,batchTaskId
                                                 ,adfPipelineName
                                                 ,clusterId
                                                 ,notebookName
                                                 ,errorLogFileLocation)
    
    logTaskProgress(cursor,batchTaskId,"Destination dataframes are created")
except Exception as e:
  errorMessage="Exception occured while reading destination columns from the source: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)  
  assert False

# COMMAND ----------

# DBTITLE 1,write to target location
#call write_to_target function to write dataframe into destination table
try:
  #if there is new data to load
  if dependentTableIsUpdated == True:
    logTaskProgress(cursor,batchTaskId,"Started writing the destination dataframes to target location")
    writeToTargetTableWithOverwrite(sourceSelectWhereDF
                                   ,targetObjectName
                                   ,targetFormat
                                   ,cursor
                                   ,batchTaskId
                                   ,adfPipelineName
                                   ,clusterId
                                   ,notebookName
                                   ,errorLogFileLocation)
    logTaskProgress(cursor,batchTaskId,"Finished writing the destination dataframes to target location")
except Exception as e:
  errorMessage="Exception occured while loading into target location: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Call getObjectReferenceDetails to get the column details into variables
try:
  #if there is new data to load
  if dependentTableIsUpdated == True:
    #call the unstructuredSource function to retrieve the columns used in the reference view
    objectunstructuredSourceReferenceDetails = getObjectReferenceDetails(targetObjectName 
                                                              ,cursor 
                                                              ,batchTaskId
                                                              ,adfPipelineName
                                                              ,clusterId 
                                                              ,notebookName
                                                              ,errorLogFileLocation)
    
    primaryKeyFields = ""
    changeableFields = ""
    #if one row is returned
    if objectunstructuredSourceReferenceDetails.shape[0] == 1:
      primaryKeyFields = objectunstructuredSourceReferenceDetails['primary_key_fields'][0]
      changeableFields = objectunstructuredSourceReferenceDetails['changeable_fields'][0]
    
    logTaskProgress(cursor,batchTaskId,"Retrieved the primary key and changeable fields for object {}".format(targetObjectName))
except Exception as e:
  errorMessage="Exception occured while retrieving the primary key and changeable fields: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Capture rows loaded into target object
try:
  #if there is new data to load
  if dependentTableIsUpdated == True:
    #this will only work when this notebook is executed from ADF or as part of a databricks job. Otherwise a row count of -1 will be returned
    if retrieveBatchRowsLoaded == True:
      batchTaskRowsLoaded = getBatchLevelObjectRowCount(targetObjectName)
    
except Exception as e:
  pass  


# COMMAND ----------

# DBTITLE 1,Cache the loaded target object
try:
  #if there is new data to load
  if dependentTableIsUpdated == True:
    #initialise empty column list
    columnList = []
    
    #if there is a primary key add to the list
    if (len(primaryKeyFields) > 0):
      columnList.append(primaryKeyFields)

    #if there are changeable fieilds add to the list. Also, stip whitespace
    if len(changeableFields) > 0:
      for x in changeableFields.split(','):
        columnList.append(x.strip())
        
    #call the unstructuredSource function to retrieve the columns used in the reference view
    cacheDatabricksTable(targetObjectName
                         ,columnList
                         ,cursor
                         ,batchTaskId
                         ,adfPipelineName
                         ,clusterId
                         ,notebookName
                         ,errorLogFileLocation)

    
    #logging is handled from within the function call
except Exception as e:
  errorMessage="Exception occured while caching table: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,complete task and close connection
#call task_end_and_close_conn function to close the database connection and mark end of task in batch_task_table
try:
  taskEndAndCloseConn(cursor
                      ,conn
                      ,batchTaskId
                      ,batchTaskSourceRows
                      ,batchTaskRowsLoaded
                      ,batchTaskRejectRows
                      ,batchTaskResult
                      ,batchTaskResultLocation
                      ,adfPipelineName
                      ,clusterId
                      ,notebookName
                      ,errorLogFileLocation)
except:
  assert False