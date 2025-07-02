# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction
# MAGIC <table>
# MAGIC   <tr><td><b>Notebook Name</b></td><td>load_intali_dbdt_dbdt_movements</td></tr>
# MAGIC   <tr><td><b>One Liner</b></td><td>Loading data from delta to delta</td></tr>
# MAGIC   <tr><td><b>Author</b></td><td>Karthik</td></tr>
# MAGIC   <tr><td><b>Creation Date</b></td><td>2021/12/15</td></tr>
# MAGIC </table>
# MAGIC 
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
# MAGIC     <td>Karthik</td>
# MAGIC     <td>2022-03-31</td>
# MAGIC     <td>Updated call to applyScdType2onDest</td>
# MAGIC   </tr>  
# MAGIC   <tr>
# MAGIC     <td>Harish N</td>
# MAGIC     <td>2022-06-22</td>
# MAGIC     <td>Added Coalesce to the lag return values</td>
# MAGIC   </tr>
# MAGIC     <tr>
# MAGIC     <td>Harish N</td>
# MAGIC     <td>2022-11-30</td>
# MAGIC     <td>Updated the raw mds views to standardised mds tables</td>
# MAGIC   </tr>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC     <td>Harish N</td>
# MAGIC     <td>2022-11-30</td>
# MAGIC     <td>Updated the view to include missing columns and updated concat_ws with upper and substring</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>03/02/2023</td>
# MAGIC     <td>HARISH N</td>
# MAGIC     <td> Added code to Remove Duplicate values for the HBK with Delete and Active Row</td>
# MAGIC   </tr>  
# MAGIC   <tr>
# MAGIC     <td>17/04/2023</td>
# MAGIC     <td>HARISH N</td>
# MAGIC     <td> Added code to Remove THE DELETES while calculating movments and changed the scd type 2 function to handle deletes with full referesh</td>
# MAGIC   </tr>
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
# MAGIC %run ../../../../dtp/nb/util/gen/logging_functions

# COMMAND ----------

# DBTITLE 1,Run standard function notebook
# MAGIC %run ../../../../dtp/nb/util/gen/standard_functions

# COMMAND ----------

# DBTITLE 1,Import modules
try:
  import pandas as pd
  import pyodbc
  from pyspark.sql.functions import rank,desc,col,lag, when, lit, concat,row_number,collect_list, coalesce, concat_ws
  from pyspark.sql import Window
  from pyspark.sql.types import LongType,StringType,IntegerType,BooleanType, DecimalType,StructField, StructType, ShortType
  from functools import reduce
  from decimal import *
  import sys
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
  createdDate = int(currentTs.strftime('%Y%m%d'))
  lastUpdateDate = createdDate
  #get the hour
  createdHour = currentTs.hour
  createdTimestamp = currentTs
  lastUpdateTimestamp = currentTs
  #use the same time for the date also
  date=currentTs
  validToTimestamp = None
  
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
  isActive=True
  
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
  sourceId=sourceId
  createdBatchId=batchId
  lastUpdateBatchId=batchId
  
  #call the get_logging_path function to create a log file path as a string and store it in a variable
  errorLogFileLocation = getLoggingPath(batchId,batchTaskId,date, 'error')  
except Exception as e:
  errorMessage = "Exception occured while getting parameters and initiliasing error log location: " + str(e)
  assert False

# COMMAND ----------

# DBTITLE 1,View to query latest MDS master reference table - needs to replaced with standardized tables
# MAGIC %sql
# MAGIC --replicated query from Canopius
# MAGIC Create OR REPLACE VIEW Standardised_MDM.vw_latest_mdm_intrali_masterreferencetable
# MAGIC AS
# MAGIC SELECT DISTINCT
# MAGIC     coalesce(Map_ID, 0) AS MapID, 
# MAGIC     coalesce(Map_Name, '') AS MapName,
# MAGIC     Transaction_Group_Behaviour_Type AS TransGroupBehaviourType
# MAGIC FROM Standardised_MDM.mdm_Intrali_Master_Reference_Table
# MAGIC WHERE CONCAT(VersionName, '-', VersionNumber) IN (
# MAGIC                                                      select CONCAT(MAX(VersionName), '-', MAX(VersionNumber))
# MAGIC                                                      FROM Standardised_MDM.mdm_Intrali_Master_Reference_Table 
# MAGIC                                                  )

# COMMAND ----------

# DBTITLE 1,View to query latest MDS aggregation matrix - needs to replaced with standardized tables
# MAGIC %sql
# MAGIC --replicated query from Canopius
# MAGIC Create OR REPLACE VIEW Standardised_MDM.vw_latest_intrali_aggregationmatrixconfigurationtable
# MAGIC AS
# MAGIC SELECT DISTINCT
# MAGIC     coalesce(Table_Column_Name, '') AS TableColumnName,
# MAGIC     coalesce(Trans_Group_Behaviour_Type, '') AS TransGroupBehaviourType,
# MAGIC     coalesce(Table_Column_Agg_Type, '') AS TableColumnAggType
# MAGIC FROM Standardised_MDM.mdm_Intrali_Aggregation_Matrix_Conf
# MAGIC WHERE CONCAT(VersionName, '-', VersionNumber) IN (
# MAGIC                                                          select CONCAT(MAX(VersionName), '-', MAX(VersionNumber))
# MAGIC                                                          FROM Standardised_MDM.mdm_Intrali_Aggregation_Matrix_Conf
# MAGIC                                                      )

# COMMAND ----------

# DBTITLE 1,Establish SQL Database connection
#access secret of database connection details from azure key vault
dbconn = dbutils.secrets.get(scope = 'lza-da-kv-001-d', key = 'lza-dp-sqlacct-001-databricks-sql-connection')

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

# DBTITLE 1,Get structure for High and Low level dataframe
#Schema for High level stor proc as pandas stores different null values for different types having an issue while conveting to spark df for infering types for null columns.
sourceDestinationFieldsDetailsSchema = getStructForHighAndLowLevelDf()
logTaskProgress(cursor,batchTaskId,"Got the schema for destination field details")

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
  sourceTableName = "standardised_intrali4444_vw_lloydspremium"
  #the location of the source taken from the procedure output
  sourceLocation = srcDesDict['source_location']
  #the format of the source taken from the procedure output
  sourceFormat = srcDesDict['source_format']
  #the header of the source taken from the procedure output
  sourceHeader=srcDesDict['source_header']
  #the calculated where expression to be applied in source dataframe
  #whereExpression = srcDesDict['source_where_clause']
  whereExpression = '1=1'
  #the xml start string to be applied when calculating xml fields
  xmlStartString = srcDesDict['source_XML_string_prefix']
  #the id of the target object taken from the procedure ouptut
  targetObjectId=srcDesDict['destination_object_id']
  #the name of the target table taken from the procedure output
  targetObjectName=srcDesDict['destination_object_name']
  #the name of the target location taken from the procedure output
  targetLocation=srcDesDict['destination_location']
  #the format of the target taken from the procedure output
  targetFormat=srcDesDict['destination_format']
  #Check Target object has Pii attributes
  calculatePiiHash=srcDesDict['calculate_pii_hash']
  primaryKeyFieldsList=srcAndDesFieldsDF.where("primary_key_order is not null").orderBy('primary_key_order').agg(collect_list(srcAndDesFieldsDF.destination_object_attribute_name)  ).collect()[0][0]
  type2FieldsList=srcAndDesFieldsDF.where("track_type_2_changes>0").orderBy('destination_object_attribute_order').agg(collect_list(srcAndDesFieldsDF.destination_object_attribute_name)).collect()[0][0]

  sourceFieldList = srcAndDesFieldsDF.where("source_object_attribute_name is not null").agg(collect_list(srcAndDesFieldsDF.source_object_attribute_name)).collect()[0][0]
  
  batch_effective_datetime = srcDesDict['batch_effective_datetime']
  batchEffectiveDatetime= srcDesDict['batch_effective_datetime']
  logTaskProgress(cursor,batchTaskId,"Got dataframe values into variables")
except Exception as e:
  errorMessage="Unable to retrieve the dataframe values as variables: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Source View Creation For Movements
vwDefinition= f''' WITH location_mapping as (
    SELECT
        case
            when lakeDeletedTimestamp is not null then null
            else source_location_code
        END AS source_location_code,
        case
            when lakeDeletedTimestamp is not null then null
            else Location_Sub_Division_Code
        END AS Location_Sub_Division_Code,
        case
            when lakeDeletedTimestamp is not null then null
            else Location_Country_Code
        END AS Location_Country_Code,
        case
            when lakeDeletedTimestamp is not null then null
            else Location_Sub_Region_Code
        END AS Location_Sub_Region_Code,
        lakeLastUpdateDate,
        lakeLastUpdateTimestamp,
        lakeDeletedTimestamp,
        lakeValidFromTimestamp
    FROM
        standardised_mdm.mdm_location_mapping a
    WHERE
        trim(lower(source_name)) = 'intrali4444'
        AND '{batch_effective_datetime}' >= a.lakevalidfromtimestamp
        AND '{batch_effective_datetime}' < COALESCE(
            a.lakevalidtotimestamp,
            CURRENT_TIMESTAMP()
        )
),
lloyds_risk_code as (
    select
        lloyds_risk_code,
        lakeLastUpdateDate,
        lakeLastUpdateTimestamp,
        lakeDeletedTimestamp,
        lakeValidFromTimestamp
    from
        standardised_mdm.mdm_lloyds_risk_code a
    WHERE
        '{batch_effective_datetime}' >= a.lakevalidfromtimestamp
        AND '{batch_effective_datetime}' < COALESCE(
            a.lakevalidtotimestamp,
            CURRENT_TIMESTAMP()
        )
),
lloyds_premium_source_query as (
    select
        concat(lp.InternalReference, '_CAN') AS Policy_Header_Reference,
        concat(lp.InternalReference, '_CAN') AS Policy_Section_Reference,
        CASE
            WHEN lp.US_StateOfFiling IS NULL THEN ' | '
            ELSE upper(
                substring(
                    concat_ws('|', '', lp.US_StateOfFiling),
                    1,
                    20
                )
            )
        END as Location_Source_Code_US_State_Of_Filing,
        upper(
            substring(
                concat_ws(
                    '|',
                    COALESCE(lp.LocationOfRisk_Country, ''),
                    COALESCE(
                        lp.LocationOfRisk_CountrySub_Division_StateProvinceTerritoryCantonEtc,
                        ''
                    )
                ),
                1,
                20
            )
        ) as Location_Source_Code_Location_Of_Risk,
        upper(
            substring(
                concat_ws(
                    '|',
                    COALESCE(
                        lp.InsuredOrReinsuredIfReinsurance_Country,
                        ''
                    ),
                    COALESCE(
                        lp.InsuredOrReinsuredIfReinsurance_CountrySub_Division_StateProvinceTerritoryCantonEtc,
                        ''
                    )
                ),
                1,
                20
            )
        ) as Location_Source_Code_Insured,
        upper(
            substring(
                concat_ws(
                    '|',
                    COALESCE(lp.Intermediary1_Country, ''),
                    COALESCE(
                        lp.Intermediary1_CountrySub_Division_StateProvinceTerritoryCantonEtc,
                        ''
                    )
                ),
                1,
                20
            )
        ) as Location_Source_Code_Intermediary_1,
        upper(
            substring(
                concat_ws(
                    '|',
                    COALESCE(lp.Intermediary2_Country, ''),
                    COALESCE(
                        lp.Intermediary2_CountrySub_Division_StateProvinceTerritoryCantonEtc,
                        ''
                    )
                ),
                1,
                20
            )
        ) as Location_Source_Code_Intermediary_2,
        upper(
            substring(
                concat_ws(
                    '|',
                    COALESCE(
                        lp.Risk_FurtherDetails_CountryOfRegistration,
                        ''
                    ),
                    ''
                ),
                1,
                20
            )
        ) as Location_Source_Code_Country_Of_Registration,
        upper(
            substring(
                concat_ws(
                    '|',
                    COALESCE(
                        lp.SurplusLinesBroker_Country,
                        ''
                    ),
                    COALESCE(
                        lp.InsuredOrReinsuredIfReinsurance_CountrySub_Division_StateProvinceTerritoryCantonEtc,
                        ''
                    )
                ),
                1,
                20
            )
        ) as Location_Source_Code_Surplus_Lines_Broker,
        lp.*
    from
        Standardised_Intrali4444.Intrali_Bordereaux_LloydsPremium lp
    WHERE
        '{batch_effective_datetime}' >= lp.lakevalidfromtimestamp
        AND '{batch_effective_datetime}' < COALESCE(
            lp.lakevalidtotimestamp,
            CURRENT_TIMESTAMP()
        ) --limit 50
)
select
    lp.HashedPartitionKey,
    lp.HashedBusinessKey,
    lp.HashValue,
    greatest(
        lp.lakeLastUpdateDate,
        loc_map_us_state_of_filing.lakeLastUpdateDate,
        loc_map_risk.lakeLastUpdateDate,
        loc_map_insured.lakeLastUpdateDate,
        loc_map_inter_1.lakeLastUpdateDate,
        loc_map_inter_2.lakeLastUpdateDate,
        loc_map_cty_reg.lakeLastUpdateDate,
        loc_map_slb.lakeLastUpdateDate,
        lrc.lakeLastUpdateDate,
        up.lakeLastUpdateDate,
        st.lakeLastUpdateDate
    ) lakeLastUpdateDate,
    greatest(
        lp.lakeLastUpdateTimestamp,
        loc_map_us_state_of_filing.lakeLastUpdateTimestamp,
        loc_map_risk.lakeLastUpdateTimestamp,
        loc_map_insured.lakeLastUpdateTimestamp,
        loc_map_inter_1.lakeLastUpdateTimestamp,
        loc_map_inter_2.lakeLastUpdateTimestamp,
        loc_map_cty_reg.lakeLastUpdateTimestamp,
        loc_map_slb.lakeLastUpdateTimestamp,
        lrc.lakeLastUpdateTimestamp,
        up.lakeLastUpdateTimestamp,
        st.lakeLastUpdateTimestamp
    ) lakeLastUpdateTimestamp,
    coalesce(
        lp.lakeDeletedTimestamp,
        up.lakeDeletedTimestamp,
        st.lakeDeletedTimestamp
    ) lakeDeletedTimestamp,
    lp.PiiHash,
    lp.PiiTraceabilityHash,
    lp.PiiHashVersion,
    lp.FK_MappingGroups_HBK,
    lp.FK_Binders_HBK,
    lp.FK_Upload_HBK,
    lp.UploadOrder,
    lp.RankedRowId,
    lp.BinderReferenceAtLeastOneToBeShown_AgreementNo,
    lp.Brokerage_BrokeragePercentOfGrossPremium,
    lp.Brokerage_BrokerageAmountOriginalCurrency,
    lp.ClassOfBusinessAtLeastOneToBeShown_ClassOfBusiness,
    lp.ClassOfBusinessAtLeastOneToBeShown_RiskCode,
    lp.ClassOfBusinessAtLeastOneToBeShown_SectionNo,
    lp.Coverholder_CoverholderPin,
    lp.Deductible_Amount,
    lp.Deductible_Basis,
    lp.Deductible_Currency,
    lp.FinalNetPremium_PercentForLloyds,
    lp.FinalNetPremium_BrokerageAmountSettlementCurrency,
    lp.FinalNetPremium_FinalNetPremiumOriginalCurrency,
    lp.FinalNetPremium_FinalNetPremiumSettlementCurrency,
    lp.FinalNetPremium_RateOfExchange,
    lp.FinalNetPremium_SettlementCurrency,
    lp.InsuredOrReinsured_FurtherDetails_Address,
    lp.InsuredOrReinsured_FurtherDetails_FiscalCodeCodiceFiscale,
    lp.InsuredOrReinsured_FurtherDetails_PostcodeZipCodeOrSimilar,
    lp.InsuredOrReinsured_FurtherDetails_Part2_Insured_PolicyholderType,
    lp.InsuredOrReinsured_FurtherDetails_Part2_Insured_RevenueOrTurnover,
    lp.InsuredOrReinsured_FurtherDetails_Part2_Insured_TotalNumberOfEmployees,
    lp.InsuredOrReinsured_FurtherDetails_Part2_ReasonForCancellation,
    lp.InsuredOrReinsuredIfReinsurance_CountrySub_Division_StateProvinceTerritoryCantonEtc,
    lp.InsuredOrReinsuredIfReinsurance_FirstName,
    lp.Intermediary1_Address,
    lp.Intermediary1_Country,
    lp.Intermediary1_CountrySub_Division_StateProvinceTerritoryCantonEtc,
    lp.Intermediary1_Name,
    lp.Intermediary1_PostcodeZipOrSimilar,
    lp.Intermediary1_ReferenceNoEtc,
    lp.Intermediary1_Role,
    lp.Intermediary2_Address,
    lp.Intermediary2_Country,
    lp.Intermediary2_CountrySub_Division_StateProvinceTerritoryCantonEtc,
    lp.Intermediary2_Name,
    lp.Intermediary2_PostcodeZipOrSimilar,
    lp.Intermediary2_ReferenceNoEtc,
    lp.Intermediary2_Role,
    lp.LloydsRef_LondonBrokerReference,
    lp.LloydsRef_YearOfAccount,
    lp.LocationOfRisk_CountrySub_Division_StateProvinceTerritoryCantonEtc,
    lp.LocationOfRisk_FurtherDetails_Address,
    lp.LocationOfRisk_FurtherDetails_County,
    lp.LocationOfRisk_FurtherDetails_PostcodeZipCodeOrSimilar,
    lp.Notes_Notes,
    lp.OtherFeesOrDeductions_Amount,
    lp.OtherFeesOrDeductions_Description,
    lp.PolicyIssuance_PolicyIssuanceDate,
    lp.ReportingPeriod_ReportingPeriodEndDate,
    lp.ReportingPeriod_ReportingPeriodStartDate,
    lp.Risk_FurtherDetails_CountryOfRegistration,
    lp.Risk_FurtherDetails_IMOShipIdentificationNumber,
    lp.Risk_FurtherDetails_NameOrRegistrationNoOfAircraftVehicleVesselEtc,
    lp.Risk_FurtherDetails_NumberOfPolicies,
    lp.Risk_FurtherDetails_NumberOfVehicles,
    lp.Risk_FurtherDetails_PeriodOfCover_Narrative,
    lp.Risk_FurtherDetails_PolicyOrGroupRef,
    lp.Risk_FurtherDetails_ReferredToLondon,
    lp.Risk_FurtherDetails_WetMarineIndicator,
    lp.SumInsured_Amount,
    lp.SurplusLinesBroker_Address,
    lp.SurplusLinesBroker_Country,
    lp.SurplusLinesBroker_LicenceNo,
    lp.SurplusLinesBroker_Name,
    lp.SurplusLinesBroker_NewJerseySLANo,
    lp.SurplusLinesBroker_State,
    lp.SurplusLinesBroker_ZipCode,
    lp.TaxOrLevyOrParaFiscalCharge1_Percent,
    lp.TaxOrLevyOrParaFiscalCharge1_AdministeredBy,
    lp.TaxOrLevyOrParaFiscalCharge1_AmountOfTaxablePremium,
    lp.TaxOrLevyOrParaFiscalCharge1_FixedRate,
    lp.TaxOrLevyOrParaFiscalCharge1_Jurisdiction_CountryStateProvinceTerritory,
    lp.TaxOrLevyOrParaFiscalCharge1_Multiplier,
    lp.TaxOrLevyOrParaFiscalCharge1_PayableBy,
    lp.TaxOrLevyOrParaFiscalCharge1_TaxAmount,
    lp.TaxOrLevyOrParaFiscalCharge1_TaxType,
    lp.TaxOrLevyOrParaFiscalCharge2_Percent,
    lp.TaxOrLevyOrParaFiscalCharge2_AdministeredBy,
    lp.TaxOrLevyOrParaFiscalCharge2_AmountOfTaxablePremium,
    lp.TaxOrLevyOrParaFiscalCharge2_FixedRate,
    lp.TaxOrLevyOrParaFiscalCharge2_Jurisdiction_CountryStateProvinceTerritory,
    lp.TaxOrLevyOrParaFiscalCharge2_Multiplier,
    lp.TaxOrLevyOrParaFiscalCharge2_PayableBy,
    lp.TaxOrLevyOrParaFiscalCharge2_TaxAmount,
    lp.TaxOrLevyOrParaFiscalCharge2_TaxType,
    lp.TaxOrLevyOrParaFiscalCharge3_Percent,
    lp.TaxOrLevyOrParaFiscalCharge3_AdministeredBy,
    lp.TaxOrLevyOrParaFiscalCharge3_AmountOfTaxablePremium,
    lp.TaxOrLevyOrParaFiscalCharge3_FixedRate,
    lp.TaxOrLevyOrParaFiscalCharge3_Jurisdiction_CountryStateProvinceTerritory,
    lp.TaxOrLevyOrParaFiscalCharge3_Multiplier,
    lp.TaxOrLevyOrParaFiscalCharge3_PayableBy,
    lp.TaxOrLevyOrParaFiscalCharge3_TaxAmount,
    lp.TaxOrLevyOrParaFiscalCharge3_TaxType,
    lp.TaxOrLevyOrParaFiscalCharge4_Percent,
    lp.TaxOrLevyOrParaFiscalCharge4_AdministeredBy,
    lp.TaxOrLevyOrParaFiscalCharge4_AmountOfTaxablePremium,
    lp.TaxOrLevyOrParaFiscalCharge4_FixedRate,
    lp.TaxOrLevyOrParaFiscalCharge4_Jurisdiction_CountryStateProvinceTerritory,
    lp.TaxOrLevyOrParaFiscalCharge4_Multiplier,
    lp.TaxOrLevyOrParaFiscalCharge4_PayableBy,
    lp.TaxOrLevyOrParaFiscalCharge4_TaxAmount,
    lp.TaxOrLevyOrParaFiscalCharge4_TaxType,
    lp.TaxOrLevyOrParaFiscalCharge5_Percent,
    lp.TaxOrLevyOrParaFiscalCharge5_AdministeredBy,
    lp.TaxOrLevyOrParaFiscalCharge5_AmountOfTaxablePremium,
    lp.TaxOrLevyOrParaFiscalCharge5_FixedRate,
    lp.TaxOrLevyOrParaFiscalCharge5_Jurisdiction_CountryStateProvinceTerritory,
    lp.TaxOrLevyOrParaFiscalCharge5_Multiplier,
    lp.TaxOrLevyOrParaFiscalCharge5_PayableBy,
    lp.TaxOrLevyOrParaFiscalCharge5_TaxAmount,
    lp.TaxOrLevyOrParaFiscalCharge5_TaxType,
    lp.Transaction_EffectiveDateOfTransaction,
    lp.Transaction_ExpiryDateOfTransaction,
    lp.Transaction_OriginalCurrency_TerrorismPremium,
    lp.Transaction_OriginalCurrency_TotalTaxesAndLevies,
    lp.Transaction_SettlementCurrency_NetPremiumToLondon,
    lp.Transaction_SettlementCurrency_RateOfExchange,
    lp.US_StateOfFiling,
    lp.US_USClassification,
    lp.USReins_NAICCode,
    lp.Data_CoverType,
    lp.Data_CoverholderCommissionAmountForWholeRiskwrittenPremium,
    lp.Data_TaxAmountForTheWholeRiskwrittenPremium,
    lp.Data_IALevy_Co_InsuranceCode,
    lp.Data_IALevy_TotalGrossWrittenPremiumAmount,
    lp.Data_SpanishTaxIdentificationNumber,
    lp.Data_LeadSyndicateNumber,
    lp.Data_HongKongInsuredOccupationCode,
    lp.Data_IAAccountingClass,
    lp.Data_IASub_AccountingClass,
    lp.Data_TypeOfVehicleCode,
    lp.Data_NumberOfVessels,
    lp.Data_EstimatedPremiumIncome,
    lp.Data_LloydsBrokerageAmountForTheRisk,
    lp.Data_ManagementExpense,
    lp.Data_NetPremiumToUnderwritersForTheRisk,
    lp.Data_CoverType_Level1,
    lp.Data_CoverType_Level2,
    lp.Data_CoverType_Level3,
    lp.Data_AustraliaInsuredOccupationCode,
    lp.Data_UKInsuredOccupationCode,
    lp.Data_A_Buildings,
    lp.Data_C_Contents,
    lp.Data_D_BusinessInterruption,
    lp.Data_ParticipationPercentCeded,
    lp.Data_TerrorismAcceptanceDate,
    lp.Data_TerrorismDeclinationDate,
    lp.Data_ReasonForEndorsement,
    lp.Data_ReasonForReinstatement,
    lp.Data_OriginalInceptionDate,
    lp.Data_NumberOfLocations,
    lp.Data_InsuredOccupationnatureOrOrganisation,
    lp.Data_InsuredAssets,
    lp.Data_AdditionalInsuredPolicyType,
    lp.Data_AdditionalInsuredEmployerName,
    lp.Data_AdditionalInsuredERNExemptFlag,
    lp.Data_AdditionalInsuredEmployerReferenceNumber,
    lp.Data_FRID,
    lp.Data_PropertyType,
    lp.Data_EligibilityCategory,
    lp.Data_ResilienceWork,
    lp.Data_Basement,
    lp.Data_BasementUsage,
    lp.Data_NoOfBedrooms,
    lp.Data_WallConstruction,
    lp.Data_RoofConstruction,
    lp.Data_RebuildingCost,
    lp.Data_RebuildingBasisIndicator,
    lp.Data_TransactionRatingDate,
    lp.Data_BuildingExcessAmount,
    lp.Data_BuildingNewAnnualPremium,
    lp.Data_BuildingTransactionPremium,
    lp.Data_BuildingAlternativeAccommodationLimit,
    lp.Data_HighValueArt,
    lp.Data_ContentsBlanketSumInsured,
    lp.Data_ContentsTransactionRatingDate,
    lp.Data_ContentsExcessSum,
    lp.Data_ContentsNewAnnualPremium,
    lp.Data_ContentsTransactionPremium,
    lp.Data_ContentsAlternativeAccommodationLimit,
    lp.Data_PropertyTotalPremiumPayable,
    lp.Data_PolicyBasis,
    lp.Data_LimitOfIndemnity,
    lp.Data_InsuredProfessionalFees,
    lp.Data_OtherRiskFactorDescription,
    lp.Data_OtherRiskFactorValue,
    lp.Data_APRAProductType,
    lp.Data_PoolReZone,
    lp.Data_DistributionChannel,
    lp.User_AgeLimits,
    lp.User_AggregateLimitOfLiability,
    lp.User_BenefitPeriodWeeks,
    lp.User_BondAmount,
    lp.User_CapitalBenefitsDeath,
    lp.User_ClientClassification,
    lp.User_ComplaintStatus,
    lp.User_DeferralPeriod,
    lp.User_InsuranceTermMonths,
    lp.User_InvoiceReference,
    lp.User_DateOfPolicySale,
    lp.User_SalesAgentSalesPersonConsultant,
    lp.User_ScopeOfCover,
    lp.User_WeeklyAccidentAndIllnessCoverBenefit,
    lp.User_WeeklyBusinessExpensesLimit,
    lp.User_ProductLine,
    lp.User_DeductibleOrExcessDays,
    lp.BinderReferenceAtLeastOneToBeShown_UniqueMarketReferenceUMR,
    lp.Coverholder_CoverholderName,
    lp.InsuredOrReinsuredIfReinsurance_Country,
    lp.InsuredOrReinsuredIfReinsurance_FullNameLastNameOrCompanyName,
    lp.LocationOfRisk_Country,
    lp.Ref_CertificateRef,
    lp.RiskDetails_RiskExpiryDate,
    lp.RiskDetails_RiskInceptionDate,
    lp.RiskDetails_RiskTransactionType,
    lp.SumInsured_Currency,
    lp.TotalPremium_TotalPremiumSumOfInstalments,
    lp.Transaction_TransactionType_OriginalPremiumEtc,
    lp.Transaction_OriginalCurrency_CommissionPercent,
    lp.Transaction_OriginalCurrency_CommissionAmount,
    lp.Transaction_OriginalCurrency_GrossPremiumPaidThisTime,
    lp.Transaction_OriginalCurrency_NetPremiumToLondon,
    lp.Transaction_OriginalCurrency_OriginalCurrency,
    lp.Transaction_SettlementCurrency_SettlementCurrency,
    lp.Type_TypeOfInsuranceDirectOrTypeOrReinsurance,
    lp.Hash,
    lp.InternalReference,
    lp.UploadId,
    lp.BordereauxPeriod,
    lp.PolicyHasBeenRestated,
    lp.UploadDate,
    lp.PolicyIdentifier,
    lp.LineType,
    lp.Audit_SpecialAcceptance,
    lp.Audit_Notes,
    lp.Audit_AcceptedBy,
    lp.BordereauxPeriodEnd,
    lp.BinderId,
    lp.Binder_UMR,
    lp.RowGUID,
    lp.NumberOfWarnings,
    lp.NumberOfErrors,
    lp.CreatedDate,
    lp.Period_YearFrom,
    lp.Period_MonthFrom,
    lp.Period_YearTo,
    lp.Period_MonthTo,
    lp.MappingGroupId,
    lp.OriginalRowId,
    lp.User_FL_IncYN,
    lp.User_EQ_IncYN,
    lp.User_NS_IncYN,
    lp.User_AOP_IncYN,
    lp.User_PersonalLinesOrCommercialOrCombined,
    lp.User_TR_IncYN,
    lp.User_WS_IncYN,
    lp.User_LastReplumbed,
    lp.User_LastRewired,
    lp.User_YearBuilt,
    lp.User_FL_DeductibleAmount,
    lp.User_EQ_Limit,
    lp.User_ConstructionDescription,
    lp.User_NoOfStories,
    lp.User_ProtectionClass,
    lp.User_AddressAccuracy,
    lp.User_BurglarAlarmYN,
    lp.User_ConstructionQuality,
    lp.User_CoverholderUnderwriterName,
    lp.User_CyberCoverageYN,
    lp.User_FireAlarmYN,
    lp.User_FrameFoundationConnection,
    lp.User_GatedCommunity,
    lp.User_HazardZone,
    lp.User_HomeSystemsProtectionYN,
    lp.User_IDTheftRecoveryCoverageYN,
    lp.User_NS_DeductibleBasis,
    lp.User_Office,
    lp.User_PolicyForm,
    lp.User_ProducingAgent,
    lp.User_RatedFloodZone,
    lp.User_RMSRoofEquipmentHurricaneBracingCode,
    lp.User_RoofAnchorCode,
    lp.User_RoofAnchorDescription,
    lp.User_RoofAnchorYN,
    lp.User_RoofCoveringCode,
    lp.User_RoofCoveringDescription,
    lp.User_RoofShapeCode,
    lp.User_ServiceLineCoverageYN,
    lp.User_ShuttersimpactGlassYN,
    lp.User_SinkholeCoverageYN,
    lp.User_SprinklersInstalled,
    lp.User_SwimmingPoolFenceYN,
    lp.User_SwimmingPoolYN,
    lp.User_Usage,
    lp.User_VacancyStatus,
    lp.User_ValuationRCVACV,
    lp.User_WindDrivenRainYN,
    lp.User_ExpiringGrossPropertyPremiumRate,
    lp.User_ExtendedReplacementCostpercent,
    lp.User_GrossPropertyPremiumRate,
    lp.User_NS_DeductiblePercent,
    lp.User_PrimaryBuildingRate,
    lp.User_DistanceToBay,
    lp.User_DistanceToCoast,
    lp.User_Latitude,
    lp.User_Longitude,
    lp.User_UnitFloorNumber,
    lp.User_Year_RoofUpdated,
    lp.User_YearHeatingUpdated,
    lp.User_ExpiringGrossPropertyPremium100Percent,
    lp.User_ComprehensivePersonalLiabilityLimit,
    lp.User_ComprehensivePersonalLiabilityPremium100Percent,
    lp.User_ExcessCoverageBuildingsLimit,
    lp.User_ExcessCoverageContentsLimit,
    lp.User_ExcessPolicyYN,
    lp.User_LossAssessmentLimit,
    lp.User_MedicalPaymentLimit,
    lp.User_MedicalPaymentPremium,
    lp.User_MoldLimit,
    lp.User_NS_DeductibleAmount,
    lp.User_NS_Limit,
    lp.User_PolicyFee,
    lp.User_WaterBackupLimit,
    lp.User_AOP_DeductibleAmount,
    lp.User_AOP_DeductiblePercent,
    lp.User_AOP_DeductibleBasis,
    lp.User_AOP_Limit,
    lp.User_B_OtherStructures100Percent,
    lp.User_BinderInceptionDate,
    lp.User_BindersExpiry,
    lp.User_BuildingCladdingDescription,
    lp.User_EQ_DeductiblePercent,
    lp.User_EQ_DeductibleAmount,
    lp.User_EQ_DeductibleBasis,
    lp.User_FL_DeductiblePercent,
    lp.User_FL_DeductibleBasis,
    lp.User_FL_Limit,
    lp.User_LastYearStructurallyUpdated,
    lp.User_LocationNumber,
    lp.User_NoOfBuildings,
    lp.User_OccupancyCode,
    lp.User_OccupancyDescription,
    lp.User_RoofShapeDescription,
    lp.User_SoftStory,
    lp.User_SquareFootage,
    lp.User_TR_AcceptanceDate,
    lp.User_TR_DeclinationDate,
    lp.User_TR_DeductiblePercent,
    lp.User_TR_DeductibleAmount,
    lp.User_TR_DeductibleBasis,
    lp.User_TR_Limit,
    lp.User_TR_TerrorismPremium,
    lp.User_TypeOfConstructionCode,
    lp.User_TypeOfOccupancyCode,
    lp.User_WS_DeductiblePercent,
    lp.User_WS_DeductibleAmount,
    lp.User_WS_DeductibleBasis,
    lp.User_WS_Limit,
    lp.User_CondominiumName,
    lp.User_CurrentRate,
    lp.User_LastYearRate,
    lp.User_BaseFloodElevation,
    lp.User_AgeOfInsured,
    lp.User_Alarm,
    lp.User_ARPCRate,
    lp.User_ARPCTier,
    lp.User_BuildingsRating,
    lp.User_CCJsBankruptcy,
    lp.User_ClaimsPreparationCostsCPCSumInsured,
    lp.User_DiscountPercentage,
    lp.User_ContentsRating,
    lp.User_CrestaZone,
    lp.User_CriminalConvictions,
    lp.User_DiscountAmount,
    lp.User_EarthquakePremiumAmount,
    lp.User_IncreasedCostOfWorkingICOWSumInsured,
    lp.User_LastYearAsIfPremium,
    lp.User_LastYearAsIfPremiumRenewal,
    lp.User_Locks,
    lp.User_Make,
    lp.User_ModelNo,
    lp.User_NaturalDisasterExcessAmount,
    lp.User_NaturalDisasterPremiumAmount,
    lp.User_NaturalDisasterSumInsured,
    lp.User_NCDBuildingscaravanvehicles,
    lp.User_NCDContents,
    lp.User_OtherSumInsured,
    lp.User_OwnershipStatus,
    lp.User_PercentageUnderBinder,
    lp.User_PropertycaravanvehicleType,
    lp.User_RateMovement,
    lp.User_SecuredToTheGround,
    lp.User_SmokeDetectors,
    lp.User_TenantType,
    lp.User_ThisYearAsIfPremium,
    lp.User_ThisYearAsIfPremiumNewBusiness,
    lp.User_ThisYearAsIfPremiumRenewal,
    lp.User_VoluntaryExcesses,
    lp.User_WagesRedundancySumInsured,
    lp.User_Within250MOfABodyOfWater,
    lp.User_TotalTax,
    lp.User_AnnualOrMonthlyPayment,
    lp.User_AgentCode,
    lp.User_CNPUnderwriterInitials,
    lp.User_EmployersLiabilityIncludedYN,
    lp.User_ExpiringYearNetPremExTax,
    lp.User_FloorConstruction,
    lp.User_Limit1Basis,
    lp.User_Limit2Basis,
    lp.User_MarketingCode,
    lp.User_ProductCode,
    lp.User_ProductsLiabilityIncludedYN,
    lp.User_PropertyOwnersLiabilityIncludedYN,
    lp.User_PropertyType2,
    lp.User_RenewalMonth,
    lp.User_Terrorism_RiskCode,
    lp.User_CNPAgreementDate,
    lp.User_DateCreated,
    lp.User_DateOfBirth,
    lp.User_DatePremiumPaid,
    lp.User_FirstInceptionDate,
    lp.User_A_BuildingsUpliftPercent,
    lp.User_C_ContentsUpliftsPercent,
    lp.User_D_BusinessInterruptionIndemPeriod,
    lp.User_A_BuildingsSI,
    lp.User_AssociatedEquipment,
    lp.User_C_ContentsUpliftSI,
    lp.User_ClericalWages,
    lp.User_EmployersLiabilityLimit,
    lp.User_ExpiringYearDVSI,
    lp.User_GrossPremium_RiskCode_B5MD,
    lp.User_GrossPremium_RiskCode_NA_PL,
    lp.User_GrossPremium_RiskCode_TUTerror,
    lp.User_GrossPremium_RiskCode_W3EL,
    lp.User_LossOfRentSumInsured,
    lp.User_ManualWages,
    lp.User_NetPremium_RiskCode_B5MD,
    lp.User_NetPremium_RiskCode_NA_PL,
    lp.User_NetPremium_RiskCode_TUTerror,
    lp.User_NetPremium_RiskCode_W3EL,
    lp.User_ProductsLiabilityLimit,
    lp.User_PropertyOwnersLiabilityLimit,
    lp.User_PublicLiabilityLimit,
    lp.User_S_StockSumInsured,
    lp.User_TotalSumInsuredDV,
    lp.User_Limit1,
    lp.User_Limit2,
    lp.User_FeeAmount,
    lp.User_GrossProfitRevenueSumInsured,
    lp.User_CoverholderReported_TotalTaxAndLevies,
    lp.User_CoverholderReported_NetDueToInsurer,
    lp.Data_TransactionNumber,
    lp.Data_TotalTaxesPaidLocally,
    lp.Data_LondonTaxes,
    lp.Data_NumberOfInstalments,
    lp.Data_InstalmentBasis,
    lp.Data_TotalFeeAmount,
    lp.Data_AmountOfTaxableWrittenPremium,
    lp.Data_LocalSubProducersCommissionAmount,
    lp.Data_LocalSubProducersCommissionPercentage,
    lp.Data_ReinsuranceBasis,
    lp.User_CoverageDescription,
    lp.User_CoverageLimitDescriptionKMMilesHours,
    lp.User_VehicleIdentificationNumber,
    lp.User_KMMilesHoursAtDateOfPolicySale,
    lp.User_CancelledPriorToDDCollectionYN,
    lp.User_DealerRetailerFleetProviderFleetOperator,
    lp.User_EngineCCHP,
    lp.User_FreeCoverYN,
    lp.User_InsuranceTermKMMilesHours,
    lp.User_InsuredE_MailAddress,
    lp.User_ManufacturerTermMonths,
    lp.User_MaxNoClaimsPerAnnum,
    lp.User_PolicyStatus,
    lp.User_TypeOfProductProviderDefined,
    lp.User_YearOfRegistration,
    lp.User_ProductCategoryDescriptionType,
    lp.User_EndOfFreeCoverPeriod,
    lp.User_DateOfPurchaseInsuredItem,
    lp.User_DateOfFirstRegistration,
    lp.User_Model,
    lp.User_DateOfCancellation,
    lp.User_ExtendedWarrantyInceptionDate,
    lp.Transaction_OriginalCurrency_AccessoriItaly,
    lp.Data_100PercentNetWrittenPremiumInUSD,
    lp.Data_LloydsPlatform,
    lp.Data_IndustrialSectorOfTheInsured,
    lp.Data_AccessoriWrittenAmount,
    lp.User_IndemnityPeriod,
    lp.User_UltimateValue,
    lp.User_ExcessUnderlying,
    lp.User_TechnicalRatingRatio,
    lp.User_ActualRateCharged,
    lp.User_RegistrationSerialNumber,
    lp.User_Manufacturer,
    lp.User_ConstructionCode,
    lp.User_PersonalInjuryYN,
    lp.User_ElevationDifference,
    lp.User_CoverageCountry,
    lp.User_ExpiringCertificateReference,
    lp.User_MonthlyBenefitAmount,
    lp.User_AdditionalInsured,
    lp.User_AdminFeesOrDeductionsPercent,
    lp.User_ExcessPeriod,
    lp.User_OtherDeductionsAmount,
    lp.User_SchemeName,
    lp.ID,
    lp.Data_TaxAmount,
    lp.User_InterestCargoDescription,
    lp.User_VoyageFrom,
    lp.User_VoyageTo,
    lp.User_SanctionsCheck,
    lp.User_MeansOfSelling,
    lp.User_HowHeardOfProduct,
    lp.User_BrokerType,
    lp.User_OtherDeductionsPercent,
    lp.User_Producer,
    lp.User_Package,
    lp.User_PolicyBandIndividualFamily,
    lp.User_ChildCoverYN,
    lp.User_FranchiseWaitingPeriod,
    lp.User_RadiusOfUse,
    lp.User_APDPremiumRate,
    lp.User_NumberOfDrivers,
    lp.User_AnyOneCombinationLimit,
    lp.User_AnyOneLossTerminalLimit,
    lp.User_AnyOneUnitLimit,
    lp.User_GLISOClassDescription,
    lp.User_GLPremiumBasisType,
    lp.User_GLBaseRate,
    lp.User_GLDeductible,
    lp.User_GLISOClassCode,
    lp.User_GLAggregateLimit,
    lp.User_GLGrossPremium,
    lp.User_GLOccurrenceLimit,
    lp.User_MaintenancePeriod,
    lp.User_InsurerProportionPercent,
    lp.User_VesselDetails_Tonnage,
    lp.User_VesselDetails_Type,
    lp.User_BenchmarkRatePercent,
    lp.User_SubjectiveRateChange,
    lp.User_ProductPartnerName,
    lp.User_VavePolicyID,
    lp.User_SchemeBuilder,
    lp.User_FeesAddedToLoan,
    lp.User_IncomeMultiple,
    lp.User_IndemnityMultiple,
    lp.Policy_Header_Reference,
    lp.Policy_Section_Reference,
    lp.Location_Source_Code_US_State_Of_Filing,
    case
        when loc_map_us_state_of_filing.lakeDeletedTimestamp is not null then 'UNMAPPED'
        else COALESCE(
            loc_map_us_state_of_filing.Location_Sub_Region_Code,
            'UNMAPPED'
        )
    end AS US_State_Of_Filing_Location_Sub_Region_Code,
    case
        when loc_map_us_state_of_filing.lakeDeletedTimestamp is not null then 'UNMAPPED'
        else COALESCE(
            loc_map_us_state_of_filing.Location_Country_Code,
            'UNMAPPED'
        )
    end AS US_State_Of_Filing_Country_Alpha_3_Code,
    case
        when loc_map_us_state_of_filing.lakeDeletedTimestamp is not null then 'UNMAPPED'
        else COALESCE(
            loc_map_us_state_of_filing.Location_Sub_Division_Code,
            'UNMAPPED'
        )
    end AS US_State_Of_Filing_Location_Sub_Division_Code,
    lp.Location_Source_Code_Location_Of_Risk,
    case
        when loc_map_risk.lakeDeletedTimestamp is not null then 'UNMAPPED'
        else COALESCE(
            loc_map_risk.Location_Sub_Region_Code,
            'UNMAPPED'
        )
    end AS Location_Of_Risk_Location_Sub_Region_Code,
    case
        when loc_map_risk.lakeDeletedTimestamp is not null then 'UNMAPPED'
        else COALESCE(
            loc_map_risk.Location_Country_Code,
            'UNMAPPED'
        )
    end AS Location_Of_Risk_Country_Alpha_3_Code,
    case
        when loc_map_risk.lakeDeletedTimestamp is not null then 'UNMAPPED'
        else COALESCE(
            loc_map_risk.Location_Sub_Division_Code,
            'UNMAPPED'
        )
    end AS Location_Of_Risk_Location_Sub_Division_Code,
    lp.Location_Source_Code_Insured,
    case
        when loc_map_insured.lakeDeletedTimestamp is not null then 'UNMAPPED'
        else COALESCE(
            loc_map_insured.Location_Sub_Region_Code,
            'UNMAPPED'
        )
    end AS Insured_Location_Sub_Region_Code,
    case
        when loc_map_insured.lakeDeletedTimestamp is not null then 'UNMAPPED'
        else COALESCE(
            loc_map_insured.Location_Country_Code,
            'UNMAPPED'
        )
    end AS Insured_Country_Alpha_3_Code,
    case
        when loc_map_insured.lakeDeletedTimestamp is not null then 'UNMAPPED'
        else COALESCE(
            loc_map_insured.Location_Sub_Division_Code,
            'UNMAPPED'
        )
    end AS Insured_Location_Sub_Division_Code,
    lp.Location_Source_Code_Intermediary_1,
    case
        when loc_map_inter_1.lakeDeletedTimestamp is not null then 'UNMAPPED'
        else COALESCE(
            loc_map_inter_1.Location_Sub_Region_Code,
            'UNMAPPED'
        )
    end AS Intermediary_1_Location_Sub_Region_Code,
    case
        when loc_map_inter_1.lakeDeletedTimestamp is not null then 'UNMAPPED'
        else COALESCE(
            loc_map_inter_1.Location_Country_Code,
            'UNMAPPED'
        )
    end AS Intermediary_1_Country_Alpha_3_Code,
    case
        when loc_map_inter_1.lakeDeletedTimestamp is not null then 'UNMAPPED'
        else COALESCE(
            loc_map_inter_1.Location_Sub_Division_Code,
            'UNMAPPED'
        )
    end AS Intermediary_1_Location_Sub_Division_Code,
    lp.Location_Source_Code_Intermediary_2,
    case
        when loc_map_inter_2.lakeDeletedTimestamp is not null then 'UNMAPPED'
        else COALESCE(
            loc_map_inter_2.Location_Sub_Region_Code,
            'UNMAPPED'
        )
    end AS Intermediary_2_Location_Sub_Region_Code,
    case
        when loc_map_inter_2.lakeDeletedTimestamp is not null then 'UNMAPPED'
        else COALESCE(
            loc_map_inter_2.Location_Country_Code,
            'UNMAPPED'
        )
    end AS Intermediary_2_Country_Alpha_3_Code,
    case
        when loc_map_inter_2.lakeDeletedTimestamp is not null then 'UNMAPPED'
        else COALESCE(
            loc_map_inter_2.Location_Sub_Division_Code,
            'UNMAPPED'
        )
    end AS Intermediary_2_Location_Sub_Division_Code,
    lp.Location_Source_Code_Country_Of_Registration,
    case
        when loc_map_cty_reg.lakeDeletedTimestamp is not null then 'UNMAPPED'
        else COALESCE(
            loc_map_cty_reg.Location_Sub_Region_Code,
            'UNMAPPED'
        )
    end AS Country_Of_Registration_Location_Sub_Region_Code,
    case
        when loc_map_cty_reg.lakeDeletedTimestamp is not null then 'UNMAPPED'
        else COALESCE(
            loc_map_cty_reg.Location_Country_Code,
            'UNMAPPED'
        )
    end AS Country_Of_Registration_Country_Alpha_3_Code,
    case
        when loc_map_cty_reg.lakeDeletedTimestamp is not null then 'UNMAPPED'
        else COALESCE(
            loc_map_cty_reg.Location_Sub_Division_Code,
            'UNMAPPED'
        )
    end AS Country_Of_Registration_Location_Sub_Division_Code,
    lp.Location_Source_Code_Surplus_Lines_Broker,
    case
        when loc_map_slb.lakeDeletedTimestamp is not null then 'UNMAPPED'
        else COALESCE(
            loc_map_slb.Location_Sub_Region_Code,
            'UNMAPPED'
        )
    end AS Surplus_Lines_Broker_Location_Sub_Region_Code,
    case
        when loc_map_slb.lakeDeletedTimestamp is not null then 'UNMAPPED'
        else COALESCE(
            loc_map_slb.Location_Country_Code,
            'UNMAPPED'
        )
    end AS Surplus_Lines_Broker_Country_Alpha_3_Code,
    case
        when loc_map_slb.lakeDeletedTimestamp is not null then 'UNMAPPED'
        else COALESCE(
            loc_map_slb.Location_Sub_Division_Code,
            'UNMAPPED'
        )
    end AS Surplus_Lines_Broker_Location_Sub_Division_Code,
    case
        when lrc.lakeDeletedTimestamp is not null then 'UNMAPPED'
        else COALESCE(lrc.Lloyds_Risk_Code, 'UNMAPPED')
    end AS lloyds_risk_code,
    up.ReportingPeriodEnd as UploadReportingPeriodEnd,
    COALESCE(mr.TransGroupBehaviourType,'A') AS TransGroupBehaviourType
from
    lloyds_premium_source_query lp
    left join location_mapping loc_map_us_state_of_filing on lp.Location_Source_Code_US_State_Of_Filing = loc_map_us_state_of_filing.source_location_code
    left join location_mapping loc_map_risk on lp.Location_Source_Code_Location_Of_Risk = loc_map_risk.source_location_code
    left join location_mapping loc_map_insured on lp.Location_Source_Code_Insured = loc_map_insured.source_location_code
    left join location_mapping loc_map_inter_1 on lp.Location_Source_Code_Intermediary_1 = loc_map_inter_1.source_location_code
    left join location_mapping loc_map_inter_2 on lp.Location_Source_Code_Intermediary_2 = loc_map_inter_2.source_location_code
    left join location_mapping loc_map_cty_reg on lp.Location_Source_Code_Country_Of_Registration = loc_map_cty_reg.source_location_code
    left join location_mapping loc_map_slb on lp.Location_Source_Code_Surplus_Lines_Broker = loc_map_slb.source_location_code
    left join lloyds_risk_code lrc on upper(
        trim(
            lp.ClassOfBusinessAtLeastOneToBeShown_RiskCode
        )
    ) = upper(trim(lrc.lloyds_risk_code))
    inner join (
        SELECT
            a.HashedBusinessKey,
            a.FK_uploadStatus_HBK,
            a.ReportingPeriodEnd,
            a.mapid,
            a.lakeLastUpdateDate,
            a.lakeLastUpdateTimestamp,
            a.lakeDeletedTimestamp,
            lakeValidFromTimestamp
        FROM
            Standardised_Intrali4444.dbo_Uploads a
        WHERE
            '{batch_effective_datetime}' >= a.lakevalidfromtimestamp
            AND '{batch_effective_datetime}' < COALESCE(
                a.lakevalidtotimestamp,
                CURRENT_TIMESTAMP()
            )
    ) up on lp.FK_Upload_HBK = up.HashedBusinessKey
    inner join (
        SELECT
            a.HashedBusinessKey,
            a.Description,
            a.lakeLastUpdateDate,
            a.lakeLastUpdateTimestamp,
            a.lakeDeletedTimestamp,
            lakeValidFromTimestamp
        FROM
            Standardised_Intrali4444.dbo_UploadStatus a
        WHERE
            '{batch_effective_datetime}' >= a.lakevalidfromtimestamp
            AND '{batch_effective_datetime}' < COALESCE(
                a.lakevalidtotimestamp,
                CURRENT_TIMESTAMP()
            )
    ) st on up.FK_uploadStatus_HBK = st.HashedBusinessKey
    inner join (
        SELECT
            a.HashedBusinessKey,
            a.id,
            a.LakeIsActive,
            a.LakeLastUpdateDate,
            a.LakeLastUpdateTimestamp,
            a.LakeDeletedTimestamp
        FROM
            standardised_intrali4444.dbo_maps a
        WHERE
            '{ batch_effective_datetime }' >= a.lakevalidfromtimestamp
            AND '{ batch_effective_datetime }' < COALESCE(a.lakevalidtotimestamp, CURRENT_TIMESTAMP())
    ) mp on mp.id = up.mapid -- should be changed to hashedbusinesskey once it is available
    and mp.LakeIsActive = 1
    left join Standardised_MDM.vw_latest_mdm_intrali_masterreferencetable mr on mr.mapid = mp.id
where
    st.Description = 'Complete'
    and coalesce(
        lp.lakeDeletedTimestamp,
        up.lakeDeletedTimestamp,
        st.lakeDeletedTimestamp,mp.lakeDeletedTimestamp
    ) is  null
    '''


# COMMAND ----------

# DBTITLE 1,Create View
vwName='standardised_intrali4444_vw_lloydspremium'
createOrReplaceTemporaryView (vwName,vwDefinition,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)

# COMMAND ----------

# DBTITLE 1,Get list of columns for movements calculation
try:    
    movementFieldsDF = spark.sql(f"""select regexp_replace(databasecolumnname, "&", "And") as databasecolumnname
    from standardised_intrali4444.dbo_fields f
        LEFT JOIN 
        
        (select HashedBusinessKey,FK_Standards_HBK from 
        standardised_intrali4444.dbo_standardcategories 
        where 
        '{ batch_effective_datetime }' >=lakevalidfromtimestamp
         AND '{ batch_effective_datetime }' < COALESCE(lakevalidtotimestamp, CURRENT_TIMESTAMP()))sc
        
            on f.FK_StandardCategories_HBK = sc.HashedBusinessKey
        LEFT JOIN (select HashedBusinessKey,Name from standardised_intrali4444.dbo_standards where '{ batch_effective_datetime }' >= lakevalidfromtimestamp
         AND '{ batch_effective_datetime }' < COALESCE(lakevalidtotimestamp, CURRENT_TIMESTAMP())) s
            on sc.FK_Standards_HBK = s.HashedBusinessKey
        LEFT JOIN (select id, name from  standardised_intrali4444.dbo_standards where '{ batch_effective_datetime }' >= lakevalidfromtimestamp
         AND '{ batch_effective_datetime }' < COALESCE(lakevalidtotimestamp, CURRENT_TIMESTAMP())) s2
            on s2.id = f.standardid
    where (
              s.Name like '%Premium'
              or s2.name like '%Premium'
          )
          and f.issummable = 1""")
    movementsColumnList = movementFieldsDF.rdd.map(lambda x:x[0]).collect()
    movementsColumnList = set(movementsColumnList).intersection(sourceFieldList)
    logTaskProgress(cursor,batchTaskId,"Got list of columns for movements calculation")
except Exception as e:
  errorMessage="Unable to get list of columns for movements calculation: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False


# COMMAND ----------

# DBTITLE 1,Get column list
#call Get_Select_And_With_Column_Details to get Columnlist tuple
try:
  columnList = getSelectAndWithColumnDetails(srcAndDesFieldsDF
                                             ,cursor 
                                             ,batchTaskId
                                             ,adfPipelineName
                                             ,clusterId
                                             ,notebookName
                                             ,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,"column list tuples are created")
except Exception as e:
  errorMessage="Unable to retrieve he column Details for With Expression and select: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Get list of columns in required format
try:
  #Get With Column Expression and Select Expression Details by calling the function
  withExpressionListFinal,columnListFinal=getWithExpressionsAndSelectList(columnList
                                                                          ,xmlStartString
                                                                          ,cursor
                                                                          ,batchTaskId
                                                                          ,adfPipelineName
                                                                          ,clusterId
                                                                          ,notebookName
                                                                          ,errorLogFileLocation)
  logTaskProgress(cursor,batchTaskId,"with column and select column expressions are created")
  #Prepare select expression
  selectExpression=[item[1] for item in columnListFinal]
except Exception as e:
  errorMessage="Unable to retrieve with and selectedExpressions: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Get aggregation matrix into a dictonary
try:
    dfAggregationMatrix = spark.read.table("Standardised_MDM.vw_latest_intrali_aggregationmatrixconfigurationtable").withColumn("Key",concat(col("TableColumnName"),col("TransGroupBehaviourType"))).selectExpr("Key as Key","TableColumnAggType as Value")
    mdsDic = generateDict(dfAggregationMatrix,batchId,cursor,batchTaskId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
    logTaskProgress(cursor,batchTaskId,"Got aggregation matrix into a dictonary")
except Exception as e:
  errorMessage="Unable to get aggregation matrix into a dictonary: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 


# COMMAND ----------

# DBTITLE 1,Udf To Get Aggregate Type
try:
    #Function to get aggregation type for a given column and transaction group
    def getAggregateType(columnName,transactionGroup):
        return mdsDic.get(columnName+transactionGroup, "T")

    getAggregateTypeUdf = udf(getAggregateType,StringType())
    logTaskProgress(cursor,batchTaskId,"Register Udf To Get Aggregate Type")
except Exception as e:
  errorMessage="Unable to register Udf To Get Aggregate Type: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Function to calculate movements
try: 
    #
    windowSpec = Window.partitionBy("PolicyIdentifier","ClassOfBusinessAtLeastOneToBeShown_RiskCode","InternalReference","RankedRowId","Transaction_OriginalCurrency_OriginalCurrency","Transaction_SettlementCurrency_SettlementCurrency").orderBy("UploadReportingPeriodEnd")
    #function to calculate movements
    def calculateMovements(columnName, lineType, transGroupBehaviourType):
       currentAggregateType= getAggregateTypeUdf(lit(columnName),transGroupBehaviourType)
       #case when inforce then currentvalue - previous month value
       #when transactional type bordereau record and the type column is restated then find whether previous month row is also restated then current values - previous month value otherwise present current value
       return (when((lineType == lit("In Force"))
                    ,col(columnName) - coalesce(lag(columnName,1,0).over(windowSpec),lit(0)))
                    .otherwise(when(currentAggregateType == lit("R")
                                    ,when( getAggregateTypeUdf(lit(columnName),lag("TransGroupBehaviourType",1,0).over(windowSpec)) == lit("R")
                                          ,col(columnName) - coalesce(lag(columnName,1,0).over(windowSpec),lit(0)))
                                          .otherwise(col(columnName)))
                                    .otherwise(col(columnName))))

    logTaskProgress(cursor,batchTaskId,"Defined function to calculate movements")
except Exception as e:
  errorMessage="Exception while defining function to calculate movements: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Register hash udf
try:
  computeHashValueUdfRegistration()
  logTaskProgress(cursor,batchTaskId,"Hash udfs are registered")
except Exception as e:
  errorMessage="Unable register Hash udfs: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False   

# COMMAND ----------

# DBTITLE 1,Get calculated data into dataframe
try:
    #Calculates movements for each column in the columnlist
    movementDF = reduce(lambda dataframe,columnlist:dataframe.withColumn(columnlist,calculateMovements(columnlist,col("LineType"),col("TransGroupBehaviourType"))),movementsColumnList,spark.read.table(sourceTableName)).selectExpr(selectExpression)
    logTaskProgress(cursor,batchTaskId,"Got calculated movements data into dataframe")
except Exception as e:
  errorMessage="Unable to get calculated data into dataframe: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Add computed fields using metadata
try:
    movementDF=reduce(lambda dataframe,columnList:dataframe.withColumn(columnList[0],eval(columnList[1])),withExpressionListFinal,movementDF)
    logTaskProgress(cursor,batchTaskId,"Added computed fields to movements data frame using metadata")
except Exception as e:
  errorMessage="Unable to get calculated data into dataframe: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False  

# COMMAND ----------

# DBTITLE 1,Remove Duplicate values for the HBK with Delete and Active Row
try:
  

    #Get only active rows if a deleted and active record present for same HBK
    window_spec=Window.partitionBy("HashedBusinessKey")
    movementDF_rank=(movementDF
                              .withColumn("RankedHbk"
                                          ,row_number().over(window_spec.orderBy(
                                              coalesce(col('lakeDeletedTimestamp')
                                                      ,lit('9999-12-31T00:00:00.000')
                                                      )
                                             .desc()))))
    movementDF_rank=(movementDF_rank
                              .where("RankedHbk=1")
                              .drop("RankedHbk"))
    
    logTaskProgress(cursor,batchTaskId,"Removed Duplicates from soruce view which has deletes and active rows for same HBK")
except Exception as e:
  errorMessage="Exception occured while deleting duplicates : " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)  
  assert False

# COMMAND ----------

# DBTITLE 1,Apply ScdType2 
try:
    # pplyScdType2onDestWithDelete(sourceDF,targetObjectName,columnList,currentTs,cursor,batchTaskId,batchId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
#   applyScdType2onDest(movementDF,targetObjectName,columnList,currentTs,cursor,batchTaskId,batchId,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  applyScdType2onDestWithDelete_conf(movementDF_rank
                                  ,targetObjectName
                                  ,columnList
                                  ,currentTs
                                  ,batchEffectiveDatetime
                                  ,cursor
                                  ,batchTaskId
                                  ,batchId
                                  ,adfPipelineName
                                  ,clusterId
                                  ,notebookName
                                  ,errorLogFileLocation)
except Exception as e:
  errorMessage="Unable to apply the scd type 2: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False 

# COMMAND ----------

# DBTITLE 1,Mark the dates as loaded into the destination
try:
  conn, cursor = markDatesLoaded(targetObjectId
                                ,sourceObjectId
                                ,currentTs
                                ,currentTs
                                ,dbconn
                                ,conn
                                ,cursor
                                ,batchTaskId
                                ,adfPipelineName
                                ,clusterId
                                ,notebookName
                                ,errorLogFileLocation)
  
  logTaskProgress(cursor,batchTaskId,"Loaded dates are marked for destination object")
except Exception as e:
  errorMessage="Exception occurred while marking the dates as loaded: " + str(e)
  logError(cursor,batchTaskId,errorMessage,adfPipelineName,clusterId,notebookName,errorLogFileLocation)
  assert False

# COMMAND ----------

# DBTITLE 1,Complete task and close connection
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