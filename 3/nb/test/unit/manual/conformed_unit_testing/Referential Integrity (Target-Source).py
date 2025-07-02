# Databricks notebook source
# DBTITLE 1,Clause
# MAGIC %sql
# MAGIC 
# MAGIC -- (Returns no record - as expected/passed)
# MAGIC select upper(Policy_Header_reference), clause_type from conformed_subscribe.Clause where LakeIsActive = 1
# MAGIC minus
# MAGIC select upper(concat(a.PolId,'_',a.UnitPsu)),
# MAGIC COALESCE(ct_ref.Conformed_Clause_Type, Concat('unmapped - ', COALESCE(NULLIF(Trim(b.sectty),''),'UNCODED')))
# MAGIC from standardised_subscribe.dbo_PolMain a
# MAGIC inner join 
# MAGIC ( SELECT upper(polid) AS polid, unitpsu, sectty,
# MAGIC row_number() OVER(partition BY upper(polid), unitpsu, sectty ORDER BY sectnarrseqno DESC) AS rnk
# MAGIC FROM standardised_subscribe.dbo_polsectnarr
# MAGIC WHERE upper(sectty) IN ('CAS_PREMIUM PAYMENT WARRANTY', 'SRCC EXCLUSION', 'COMMUNICABLE DISEASE EXCLUSION',
# MAGIC 'CYBER EXCLUDED', 'CYBER EXCLUSION REFERENCE', 'PREMIUM PAYMENT WARRANTY (PPW)', 'TERRORISM AND WAR PERILS CODES',
# MAGIC 'CAS_WAR AND TERROR', 'CYBER') AND LakeIsActive = 1) b
# MAGIC ON upper(a.polid) = upper(b.polid)
# MAGIC AND upper(a.unitpsu) = upper(b.unitpsu)
# MAGIC AND a.LakeIsActive = 1
# MAGIC left join  standardised_mds.dbo_clause_type_ref ct_ref
# MAGIC ON b.sectty = ct_ref.Code
# MAGIC AND lower(ct_ref.Name) = 'subscribe'
# MAGIC AND ct_ref.LakeIsActive = 1

# COMMAND ----------

# DBTITLE 1,Coverage
# MAGIC %sql
# MAGIC 
# MAGIC -- (Returns no record - as expected/passed)
# MAGIC select upper(Policy_Header_reference), coverage_type from conformed_subscribe.coverage  where LakeIsActive = 1 
# MAGIC minus
# MAGIC select upper(concat(anyl.PolId,'_',anyl.UnitPsu)),
# MAGIC COALESCE( ct_ref.Conformed_Coverage_Type_Code, Concat('unmapped - ', COALESCE(NULLIF(Trim(anyl.cd), ''), 'UNCODED'))) from standardised_subscribe.dbo_PolAnlyCd anyl
# MAGIC inner join standardised_subscribe.dbo_PolMain main
# MAGIC on upper(anyl.PolId) = upper(main.PolId) and upper(anyl.UnitPsu) = upper(main.UnitPsu) 
# MAGIC and anyl.LakeIsActive = 1 and main.LakeIsActive = 1 
# MAGIC and upper(anyl.Ty) = 'BUSINESS'
# MAGIC LEFT JOIN  standardised_mds.dbo_coverage_type_ref ct_ref
# MAGIC ON anyl.cd = ct_ref.Code AND lower(ct_ref.Name) = 'subscribe'
# MAGIC and ct_ref.lakeisactive=1

# COMMAND ----------

# DBTITLE 1,Deduction
# MAGIC %sql
# MAGIC 
# MAGIC --(Returns no record - as expected/passed)
# MAGIC select upper(Policy_Header_reference), deduction_type, coverage_type from conformed_subscribe.Deduction where LakeIsActive = 1
# MAGIC minus
# MAGIC select upper(concat(deds.PolId,'_',deds.UnitPsu)),
# MAGIC COALESCE(dt_ref.Conformed_Deduction_Type_Code, Concat('unmapped - ',COALESCE(NULLIF(Trim(deds.dedty),''),'UNCODED'))) AS Deduction_Type,
# MAGIC COALESCE(ct_ref.Conformed_Coverage_Type_Code, Concat('unmapped - ', COALESCE(NULLIF(Trim(anyl.cd),''),'UNCODED'))) AS Coverage_Type
# MAGIC 
# MAGIC from standardised_subscribe.dbo_PolPmDeds deds
# MAGIC inner join standardised_subscribe.dbo_PolMain main
# MAGIC on deds.PolId = main.PolId and deds.UnitPsu = main.UnitPsu 
# MAGIC and deds.LakeIsActive = 1 and main.LakeIsActive = 1
# MAGIC LEFT JOIN  standardised_mds.dbo_deduction_type_ref dt_ref
# MAGIC ON deds.dedty = dt_ref.Code
# MAGIC AND lower(dt_ref.Name) = 'subscribe'
# MAGIC and dt_ref.lakeisactive=1
# MAGIC LEFT JOIN  standardised_subscribe.dbo_polanlycd anyl
# MAGIC ON upper(deds.polid) = upper(anyl.polid)
# MAGIC AND upper(deds.unitpsu) = upper(anyl.unitpsu)
# MAGIC AND upper(anyl.ty) = 'BUSINESS'
# MAGIC and anyl.lakeisactive=1
# MAGIC LEFT JOIN  standardised_mds.dbo_coverage_type_ref ct_ref
# MAGIC ON anyl.cd = ct_ref.Code
# MAGIC AND lower(ct_ref.Name) = 'subscribe'
# MAGIC and dt_ref.lakeisactive=1

# COMMAND ----------

# DBTITLE 1,Endorsement
# MAGIC %sql
# MAGIC 
# MAGIC --(Returns no record - as expected/passed)
# MAGIC select upper(Policy_Header_reference), Endorsement_Reference from conformed_subscribe.Endorsement where LakeIsActive = 1
# MAGIC minus
# MAGIC select upper(concat(polpm.PolId,'_',polpm.UnitPsu)),
# MAGIC Concat(polpm.polid, '_', polpm.unitpsu, '_', polpm.pmlineno) AS Endorsement_Reference
# MAGIC 
# MAGIC from standardised_subscribe.dbo_PolPm polpm
# MAGIC inner join standardised_subscribe.dbo_PolMain main
# MAGIC on upper(polpm.PolId) = upper(main.PolId) and upper(polpm.UnitPsu) = upper(main.UnitPsu)
# MAGIC and polpm.PMSeqNo <> 1 --Assume first movement is the original agreed premium
# MAGIC AND upper(polpm.EntTy) IN ('AP','RP','AJ') --Assume subsequent movements to the original premium are corrections, not endorsements
# MAGIC AND upper(polpm.PMSt) = 'ORIG' --Only use Premium Status of "Original". Ignore the Cancelled/Contras
# MAGIC and polpm.LakeIsActive = 1 and main.LakeIsActive = 1

# COMMAND ----------

# DBTITLE 1,EPI Transaction
# MAGIC %sql
# MAGIC 
# MAGIC select sequence_number, item_type, source_transaction_identifier, upper(policy_header_reference) from conformed_subscribe.EPI_Transaction where lakeisactive = 1
# MAGIC 
# MAGIC minus
# MAGIC select 
# MAGIC pm.pmseqno  AS sequence_number,
# MAGIC COALESCE(it_ref.Conformed_Item_Type_Code, Concat( 'unmapped - ', COALESCE(NULLIF(Trim(item.cd), ''), 'UNCODED'))) AS item_type,
# MAGIC Concat_ws('_',pm.polid, pm.unitpsu, pm.occ, pm.pmseqno) AS source_transaction_identifier,
# MAGIC upper(concat(pm.PolID,'_', pm.UnitPsu))
# MAGIC FROM standardised_subscribe.dbo_polpm pm  --to fetch MktOrigPMAmt
# MAGIC INNER JOIN 
# MAGIC standardised_subscribe.dbo_InPolPtpt AS ipp --to fetch Split percentage
# MAGIC ON upper(ipp.PolId) = upper(pm.PolId) 
# MAGIC AND upper(ipp.UnitPsu) = upper(pm.UnitPsu)
# MAGIC INNER JOIN 
# MAGIC standardised_subscribe.dbo_AnlyCdSplt AS rsk -- to fetch the risk percentage
# MAGIC ON upper(rsk.PolId) = upper(pm.PolId) 
# MAGIC AND upper(rsk.UnitPsu) = upper(pm.UnitPsu) 
# MAGIC AND upper(trim(rsk.Ty)) = 'RSK'
# MAGIC INNER JOIN 
# MAGIC standardised_subscribe.dbo_inpol up -- to fetch calcn
# MAGIC ON upper(up.polid) = upper(pm.polid) 
# MAGIC AND upper(up.UnitPsu) = upper(pm.UnitPsu)
# MAGIC INNER JOIN 
# MAGIC standardised_subscribe.dbo_polmain pl --to keep policies which are only present in pol main
# MAGIC ON upper(pl.polid) = upper(pm.polid) 
# MAGIC AND upper(pl.UnitPsu) = upper(pm.UnitPsu)
# MAGIC and pm.lakeisactive=1 and ipp.lakeisactive=1 and pl.lakeisactive=1 and rsk.lakeisactive=1 and up.lakeisactive=1
# MAGIC LEFT JOIN  standardised_subscribe.dbo_polanlycd item 
# MAGIC --to get the item type
# MAGIC ON         upper(pm.polid) = upper(item.polid)
# MAGIC AND        upper(pm.unitpsu) = upper(item.unitpsu)
# MAGIC AND        upper(item.ty) IN ('STATSCODE')
# MAGIC and item.lakeisactive=1
# MAGIC 
# MAGIC LEFT JOIN  standardised_mds.dbo_item_type it_ref
# MAGIC ON         item.cd = it_ref.Code
# MAGIC AND        lower(it_ref.Name) = 'subscribe'
# MAGIC and it_ref.lakeisactive=1

# COMMAND ----------

# DBTITLE 1,Item
# MAGIC %sql
# MAGIC --(Returns no record - as expected/passed)
# MAGIC select upper(Policy_Header_reference), Item_Type from conformed_subscribe.Item where LakeIsActive = 1
# MAGIC minus
# MAGIC select upper(concat(item.PolId,'_',item.UnitPsu)),
# MAGIC COALESCE(it_ref.Conformed_Item_Type_Code, Concat('unmapped - ', COALESCE(NULLIF(Trim(item.cd),''),'UNCODED')))   AS Item_Type
# MAGIC from standardised_subscribe.dbo_PolAnlyCd item
# MAGIC inner join standardised_subscribe.dbo_PolMain main
# MAGIC on upper(item.PolId) = upper(main.PolId) and upper(item.UnitPsu) = upper(main.UnitPsu) 
# MAGIC and upper(item.Ty) = 'STATSCODE' and item.LakeIsActive = 1 and main.LakeIsActive = 1
# MAGIC LEFT JOIN  standardised_mds.dbo_item_type it_ref
# MAGIC ON         item.cd = it_ref.Code
# MAGIC AND        lower(it_ref.Name) = 'subscribe'
# MAGIC and it_ref.lakeisactive=1

# COMMAND ----------

# DBTITLE 1,Limit
# MAGIC %sql
# MAGIC 
# MAGIC --(Returns no record - as expected/passed)
# MAGIC select upper(Policy_Header_Reference), Coverage_Type, Item_Type, Limit_Type
# MAGIC from conformed_subscribe.Limit where LakeIsActive = 1
# MAGIC minus
# MAGIC select upper(concat(lmt.PolId,'_',lmt.UnitPsu)),
# MAGIC COALESCE(ct_ref.Conformed_Coverage_Type_Code, Concat('unmapped - ', COALESCE(NULLIF(Trim(cvr.cd), ''), 'UNCODED'))) AS Coverage_Type,
# MAGIC COALESCE(itm_ref.Conformed_Item_Type_Code, Concat('unmapped - ', COALESCE(NULLIF(Trim(itm.cd), ''), 'UNCODED')))    AS Item_Type,
# MAGIC COALESCE(lmt_ref.Conformed_Limit_Basis_Name, Concat('unmapped - ', COALESCE(NULLIF(Trim(lmt.lmtty),''),'UNCODED'))) AS Limit_Type
# MAGIC from 
# MAGIC (select * from standardised_subscribe.dbo_PolMltLmt where lakeisactive=1) lmt
# MAGIC inner join 
# MAGIC (select * from standardised_subscribe.dbo_PolMain where lakeisactive=1) main
# MAGIC on upper(lmt.PolId) = upper(main.PolId) and upper(lmt.UnitPsu) = upper(main.UnitPsu) 
# MAGIC --and LmtTy in ('Excess', 'Deductibles', 'Limit') 
# MAGIC LEFT JOIN  standardised_subscribe.dbo_polanlycd itm
# MAGIC ON         upper(lmt.polid) = upper(itm.polid)
# MAGIC AND        upper(lmt.unitpsu) = upper(itm.unitpsu)
# MAGIC AND        upper(trim(itm.ty)) = 'STATSCODE'
# MAGIC and itm.lakeisactive=1
# MAGIC LEFT JOIN  standardised_subscribe.dbo_polanlycd cvr
# MAGIC ON         upper(lmt.polid) = upper(cvr.polid)
# MAGIC AND        upper(lmt.unitpsu) = upper(cvr.unitpsu)
# MAGIC AND        upper(trim(cvr.ty)) = 'BUSINESS'
# MAGIC and cvr.lakeisactive=1
# MAGIC            ---MDS Fields : LimitType
# MAGIC LEFT JOIN  standardised_mds.dbo_limit_basis_ref lmt_ref
# MAGIC ON         lmt.lmtty = lmt_ref.Code
# MAGIC AND        lower(lmt_ref.Name) = 'subscribe'
# MAGIC and lmt_ref.lakeisactive=1
# MAGIC            ---MDS Fields : ItemType
# MAGIC LEFT JOIN  standardised_mds.dbo_item_type itm_ref
# MAGIC ON         itm.cd = itm_ref.Code
# MAGIC AND        lower(itm_ref.Name) = 'subscribe'
# MAGIC and itm_ref.lakeisactive=1
# MAGIC 
# MAGIC            ---MDS Fields : CoverageType
# MAGIC LEFT JOIN  standardised_mds.dbo_coverage_type_ref ct_ref
# MAGIC ON         cvr.cd = ct_ref.Code
# MAGIC AND        lower(ct_ref.Name) = 'subscribe'
# MAGIC and ct_ref.lakeisactive=1

# COMMAND ----------

# DBTITLE 1,Line
# MAGIC %sql
# MAGIC 
# MAGIC --(Returns no record - as expected/passed)
# MAGIC select upper(Policy_Header_Reference),
# MAGIC Business_Entity_Code
# MAGIC from conformed_subscribe.line where LakeIsActive = 1
# MAGIC minus
# MAGIC select upper(concat(ip.PolId,'_',ip.UnitPsu)),
# MAGIC COALESCE(be_ref.Business_Entity_Group_Code, Concat('unmapped - ', COALESCE(NULLIF(Trim(inptpt.unitid),''),'UNCODED'))) AS Business_Entity_Code
# MAGIC from standardised_subscribe.dbo_Inpol ip
# MAGIC --to fetch the inwards policy that exists in main table
# MAGIC inner Join standardised_subscribe.dbo_PolMain main
# MAGIC on upper(ip.PolId) = upper(main.PolId) and upper(ip.UnitPsu) = upper(main.UnitPsu)
# MAGIC INNER JOIN standardised_subscribe.dbo_inpolptpt inptpt
# MAGIC ON         upper(ip.polid) = upper(inptpt.polid)
# MAGIC AND        upper(ip.unitpsu) = upper(inptpt.unitpsu)
# MAGIC and ip.lakeIsActive = 1 and main.lakeIsActive = 1  and inptpt.lakeIsActive = 1
# MAGIC LEFT JOIN  standardised_mds.dbo_business_entity_ref be_ref
# MAGIC ON         inptpt.unitid = be_ref.Code
# MAGIC and be_ref.lakeisactive=1

# COMMAND ----------

# DBTITLE 1,Notes
# MAGIC %sql
# MAGIC select upper(Policy_Header_Reference) from conformed_subscribe.note where lakeIsActive = 1
# MAGIC minus
# MAGIC SELECT concat(Nt.PolId,'_',Nt.UnitPsu) as Ref
# MAGIC from standardised_subscribe.dbo_PolNt Nt
# MAGIC inner join standardised_subscribe.dbo_PolMain Mn
# MAGIC on upper(Nt.PolId) = upper(Mn.PolId) and upper(Nt.UnitPsu) = upper(Mn.UnitPsu)
# MAGIC where (Nt.lakeisactive=1 and Mn.lakeisactive=1) 

# COMMAND ----------

# DBTITLE 1,Office Line Relationship
# MAGIC %sql
# MAGIC --(Returns no record - as expected/passed)
# MAGIC select upper(Policy_Header_Reference),
# MAGIC Business_Entity_Code
# MAGIC from conformed_subscribe.office_line_relationship where LakeIsActive = 1
# MAGIC minus
# MAGIC select upper(concat(PM.PolId,'_',PM.UnitPsu)),
# MAGIC COALESCE(be_ref.Business_Entity_Group_Code, Concat('unmapped - ', COALESCE(NULLIF(Trim(ipp.unitid),''),'UNCODED'))) AS Business_Entity_Code
# MAGIC from standardised_subscribe.dbo_InPolPtPt IPP
# MAGIC Inner Join standardised_subscribe.dbo_PolMain PM
# MAGIC on upper(IPP.PolId ) = upper(PM.PolId) and upper(IPP.UnitPsu ) = upper(PM.UnitPsu) 
# MAGIC and ipp.LakeIsActive = 1 and pm.LakeIsActive = 1
# MAGIC LEFT JOIN  standardised_mds.dbo_business_entity_ref be_ref
# MAGIC ON         ipp.unitid = be_ref.Code
# MAGIC and be_ref.lakeisactive=1

# COMMAND ----------

# DBTITLE 1,Party
# MAGIC %sql
# MAGIC select SUBSTRING_INDEX(Party_Code,'_',1) as prtycd, Party_Name from conformed_subscribe.Party where lakeisactive=1
# MAGIC minus
# MAGIC select distinct trim(upper(PartyCode)) as Party_Code, 
# MAGIC case when trim(lower(PartyName)) = 'unknown broker - -' then 'UNKNOWN BROKER' ELSE trim(UPPER(PartyName)) END as Party_Name from (select
# MAGIC 
# MAGIC trim(AnlyCd) as PartyCode,
# MAGIC replace(AnlyNm,'  ',' ') as PartyName
# MAGIC from standardised_subscribe.dbo_Anly 
# MAGIC where UPPER(trim(AnlyTy)) in ('LLOYDSLEAD', 'AGREEPARTY', 'MARKETLEAD', 'SLPLEAD') 
# MAGIC and nullif(trim(AnlyCd), '') IS NOT NULL and lakeisactive=1
# MAGIC 
# MAGIC UNION
# MAGIC 
# MAGIC -- To fetch party details for Broker
# MAGIC SELECT
# MAGIC nullif(bkr.BkrCd,'dc937b59892') AS PartyCode,
# MAGIC COALESCE(nullif(bkr.BkrNm,'dc937b59892'),'UNCODED') as PartyName
# MAGIC 
# MAGIC FROM 
# MAGIC (
# MAGIC select bkrcd, bkrnm, bkrpsu, TelNo, Addr1, Addr2, Addr3, Addr4, lakeIsActive,LakeValidFromTimestamp,LakeValidToTimestamp,LakeLastUpdateDate,LakeLastUpdateTimestamp from
# MAGIC (
# MAGIC select bkrcd, bkrnm,bkrpsu, TelNo, Addr1, Addr2, Addr3, Addr4,
# MAGIC lakevalidfromtimestamp, lakevalidtotimestamp, lakeisactive, lakelastupdatedate, lakelastupdatetimestamp,
# MAGIC row_number() OVER(partition BY bkrcd, upper(bkrpsu) ORDER BY bkrseqid DESC) AS rnk
# MAGIC FROM standardised_subscribe.dbo_bkr
# MAGIC where lakeisactive = 1
# MAGIC ) where rnk=1
# MAGIC ) bkr
# MAGIC 
# MAGIC UNION
# MAGIC 
# MAGIC select DISTINCT
# MAGIC trim(bkrcd) as bkrcd, 
# MAGIC CONCAT_WS('','Unknown Broker - ',U.BkrCd,' - ',coalesce(upper(trim(nullif(bkrpsu,'?'))),'UNCODED')) as bkr_nm
# MAGIC FROM 
# MAGIC standardised_subscribe.dbo_usmmain U where lakeisactive=1
# MAGIC UNION
# MAGIC 
# MAGIC -- To fetch party details for Individual/Organisation
# MAGIC -- for each insdid there should be only one country_div. Otherwise update insdid as insdid_countryDiv
# MAGIC SELECT
# MAGIC insd.InsdId as PartyCode, 
# MAGIC InsdNm as PartyName
# MAGIC 
# MAGIC FROM standardised_subscribe.dbo_Insd insd where lakeisactive=1 
# MAGIC 
# MAGIC     
# MAGIC UNION
# MAGIC 
# MAGIC -- To fetch party details for Underwriter
# MAGIC SELECT
# MAGIC UwrPsu as PartyCode,
# MAGIC Nm as PartyName
# MAGIC FROM standardised_subscribe.dbo_Uwr
# MAGIC WHERE nullif(UwrPsu, '') IS NOT NULL and lakeisactive=1
# MAGIC )x
# MAGIC  

# COMMAND ----------

# DBTITLE 1,Party Policy Header Role
# MAGIC %sql
# MAGIC 
# MAGIC ---( TARGET - SOURCE )
# MAGIC select upper(Policy_Header_Reference) from conformed_Subscribe.Party_Policy_Header_Role where LakeIsActive = 1
# MAGIC minus
# MAGIC select upper(concat(PolID,'_',unitpsu)) as Policy_Header_Reference from (
# MAGIC select PolID,unitpsu from standardised_subscribe.dbo_PolInsd where LakeIsActive = 1 group by 1,2
# MAGIC union
# MAGIC select PolID,unitpsu from standardised_subscribe.dbo_PolAnlyCd
# MAGIC where upper(ty) IN ('MARKETLEAD','AGREEPARTY','LLOYDSLEAD','SLPLEAD') 
# MAGIC and LakeIsActive = 1 group by 1,2
# MAGIC union
# MAGIC SELECT P.PolId,P.UnitPsu
# MAGIC FROM standardised_subscribe.dbo_PolMain P
# MAGIC INNER JOIN standardised_subscribe.dbo_Bkr B
# MAGIC ON P.BkrNo = B.BkrCd
# MAGIC and P.LakeIsActive = 1 and B.LakeIsActive = 1 group by 1,2
# MAGIC union
# MAGIC SELECT P.PolId,P.UnitPsu
# MAGIC FROM standardised_subscribe.dbo_PolMain P
# MAGIC INNER JOIN standardised_subscribe.dbo_uwr U   
# MAGIC ON P.uwr = U.uwrPsu 
# MAGIC and P.LakeIsActive = 1 and U.LakeIsActive = 1 group by 1,2 ) a
# MAGIC where exists (select upper(concat(PolID,'_',unitpsu)) as Policy_Header_Reference from standardised_subscribe.dbo_Polmain b 
# MAGIC where upper(concat(a.PolID,'_',a.unitpsu))= upper(concat(b.PolID,'_',b.unitpsu)) and b.lakeisactive = 1)

# COMMAND ----------

# DBTITLE 1,Party Policy Section Role
# MAGIC %sql
# MAGIC 
# MAGIC ---( TARGET - SOURCE )
# MAGIC select upper(Policy_section_reference) from conformed_Subscribe.Party_Policy_Section_Role where LakeIsActive = 1
# MAGIC minus
# MAGIC select upper(concat(PolID,'_',unitpsu)) as Policy_section_reference from (
# MAGIC select PolID,unitpsu from standardised_subscribe.dbo_PolInsd where LakeIsActive = 1 group by 1,2
# MAGIC union
# MAGIC select PolID,unitpsu from standardised_subscribe.dbo_PolAnlyCd
# MAGIC where upper(ty) IN ('MARKETLEAD','AGREEPARTY','LLOYDSLEAD','SLPLEAD') 
# MAGIC and LakeIsActive = 1 group by 1,2
# MAGIC union
# MAGIC SELECT P.PolId,P.UnitPsu
# MAGIC FROM standardised_subscribe.dbo_PolMain P
# MAGIC INNER JOIN standardised_subscribe.dbo_Bkr B
# MAGIC ON P.BkrNo = B.BkrCd
# MAGIC and P.LakeIsActive = 1 and B.LakeIsActive = 1 group by 1,2
# MAGIC union
# MAGIC SELECT P.PolId,P.UnitPsu
# MAGIC FROM standardised_subscribe.dbo_PolMain P
# MAGIC INNER JOIN standardised_subscribe.dbo_uwr U   
# MAGIC ON P.uwr = U.uwrPsu 
# MAGIC and P.LakeIsActive = 1 and U.LakeIsActive = 1 group by 1,2 ) a
# MAGIC where exists (select upper(concat(PolID,'_',unitpsu)) as Policy_section_reference from standardised_subscribe.dbo_Polmain b 
# MAGIC where upper(concat(a.PolID,'_',a.unitpsu))= upper(concat(b.PolID,'_',b.unitpsu)) and b.lakeisactive = 1)

# COMMAND ----------

# DBTITLE 1,Party_Signing_Transaction_Role
# MAGIC %sql
# MAGIC select  trim(upper(policy_Section_reference)) from conformed_subscribe.party_signing_transaction_role where lakeisactive=1 
# MAGIC --and policy_Section_reference = 'W53582BAA_CAN'
# MAGIC 
# MAGIC minus
# MAGIC 
# MAGIC select policy_Section_reference from
# MAGIC (
# MAGIC select trim(upper(concat(U.PolId,'_', U.Unitpsu))) as policy_Section_reference
# MAGIC from standardised_subscribe.dbo_USMMain U
# MAGIC inner join 
# MAGIC (
# MAGIC select 
# MAGIC PolId,
# MAGIC UnitPsu
# MAGIC from
# MAGIC  standardised_subscribe.dbo_PolInsd where lakeisactive=1
# MAGIC  group by 1,2
# MAGIC ) insd     -- to fetch the role_type of the policies
# MAGIC on upper(U.PolId) = upper(insd.PolId) 
# MAGIC and upper(U.UnitPsu) = upper(insd.UnitPsu) 
# MAGIC where u.lakeisactive=1
# MAGIC 
# MAGIC union
# MAGIC select trim(upper(concat(U.PolId,'_', U.Unitpsu))) as policy_Section_reference
# MAGIC FROM 
# MAGIC standardised_subscribe.dbo_UsmMain U 
# MAGIC inner JOIN 
# MAGIC (SELECT 
# MAGIC bkrcd, bkrpsu, max(bkrseqid) AS rnk
# MAGIC FROM standardised_subscribe.dbo_bkr
# MAGIC where lakeisactive = 1
# MAGIC group by 1,2
# MAGIC ) BKR 
# MAGIC On trim(bkr.BkrCd) = trim(U.BkrCd)
# MAGIC and  trim(upper(bkr.bkrpsu))=trim(UPPER(u.bkrpsu))
# MAGIC where u.lakeisactive=1) x
# MAGIC --where policy_Section_reference = 'W53582BAA_CAN'

# COMMAND ----------

#for %m/%d/yy
from datetime import datetime
def validateDate_8_1(date):
    try:
        datetime.strptime(date, '%d/%m/%y')
        return True
    except ValueError:
        isValidDate = False
        return isValidDate
    
sqlContext.udf.register('isdt_1', validateDate_8_1)


#for %m/%d/yyyy


def validateDate_8_2(date):
    try:
        datetime.strptime(date, '%d/%m/%Y')
        return True
    except ValueError:
        isValidDate = False
        return isValidDate
sqlContext.udf.register('isdt_2', validateDate_8_2)


#for dates present in this format--> 1 Dec 2020

def validateDate_11(date):
    try:
        datetime.strptime(date, '%d %b %Y')
        return True
    except ValueError:
        isValidDate = False
        return isValidDate
    
sqlContext.udf.register('isdt_3', validateDate_11)

#for dates present in this format--> 1 December 2020


def validateDate_12(date):
    try:
        datetime.strptime(date, '%d %B %Y')
        return True
    except ValueError:
        isValidDate = False
        return isValidDate
    
sqlContext.udf.register('isdt_4', validateDate_12)
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

# DBTITLE 1,Policy Activity
# MAGIC %sql
# MAGIC select Policy_Header_Reference, policy_section_reference, Activity_Type, Activity_Date from conformed_subscribe.Policy_Activity 
# MAGIC where lakeisactive =1
# MAGIC 
# MAGIC minus
# MAGIC 
# MAGIC select Policy_Header_Reference,policy_section_reference, 
# MAGIC COALESCE(acttype_ref.Code, CONCAT('unmapped - ', COALESCE(NULLIF(Activity_Type, ''), 'UNCODED')))     AS Activity_Type,
# MAGIC cast(Activity_Date as timestamp) from 
# MAGIC (
# MAGIC SELECT 
# MAGIC upper(concat(trim(inpol.PolId),'_', inpol.UnitPsu)) as Policy_Header_Reference,
# MAGIC upper(concat(trim(inpol.PolId),'_', inpol.UnitPsu)) as policy_section_reference,
# MAGIC 'Policy Status' AS Activity_Type,
# MAGIC To_date('9999-12-31') AS Activity_Date
# MAGIC FROM standardised_subscribe.dbo_inpol inpol
# MAGIC WHERE  lakeisactive=1
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC SELECT  Upper(Concat(Trim(pm.polid), '_', pm.unitpsu)) AS Policy_Header_Reference,
# MAGIC Upper(Concat(Trim(pm.polid), '_', pm.unitpsu)) AS Policy_Section_Reference,
# MAGIC 'Entry Status' AS Activity_Type,
# MAGIC To_date('9999-12-31') AS Activity_Date
# MAGIC FROM   standardised_subscribe.dbo_polmain pm
# MAGIC INNER JOIN standardised_subscribe.dbo_inpol ip
# MAGIC ON pm.polid = ip.polid
# MAGIC AND pm.unitpsu = ip.unitpsu
# MAGIC WHERE  pm.lakeisactive=1 and ip.lakeisactive=1
# MAGIC                   
# MAGIC union all
# MAGIC 
# MAGIC SELECT policy_header_reference,
# MAGIC policy_section_reference,
# MAGIC activity_type,
# MAGIC activity_date
# MAGIC FROM   (
# MAGIC SELECT Upper(Concat(Trim(a.polid), '_', a.unitpsu)) AS Policy_Header_Reference,
# MAGIC Upper(Concat(Trim(a.polid), '_', a.unitpsu)) AS Policy_Section_Reference,
# MAGIC CASE
# MAGIC   WHEN Upper(a.sectty) IN ( 'CONTRACT CERTAINTY COMPLETED DATE' ) THEN 'Contract Certainty'
# MAGIC   WHEN Upper(a.sectty) IN ( 'DATE PEER REVIEW COMPLETED' ) THEN 'Peer Review'
# MAGIC END AS Activity_Type,
# MAGIC CASE
# MAGIC   WHEN Length(Trim(a.sectdsc)) <= 5 THEN '9999-12-31'
# MAGIC   WHEN ( ( Isdt_1(Trim(a.sectdsc)) = true
# MAGIC             OR Isdt_2(Trim(a.sectdsc)) = true
# MAGIC             OR Isdt_3(Trim(a.sectdsc)) = true
# MAGIC             OR Isdt_4(Trim(a.sectdsc)) = true )
# MAGIC          AND Length(a.sectdsc) > 5 ) THEN COALESCE(To_date(a.sectdsc, 'd MMM y'), To_date(a.sectdsc, 'd/M/y'), a.sectdsc)
# MAGIC          ELSE '9999-12-31'
# MAGIC        END                                          AS Activity_Date
# MAGIC FROM   (SELECT Upper(polid)                     AS PolId,
# MAGIC                unitpsu,
# MAGIC                sectty,
# MAGIC                sectdsc,
# MAGIC                sectnarrseqno,lakeisactive,
# MAGIC                Row_number() OVER(partition BY Upper(polid), unitpsu, sectty
# MAGIC                    ORDER BY sectnarrseqno DESC) AS rnk
# MAGIC         FROM   standardised_subscribe.dbo_polsectnarr
# MAGIC         WHERE  Upper(sectty) IN ( 'CONTRACT CERTAINTY COMPLETED DATE',
# MAGIC                                          'DATE PEER REVIEW COMPLETED' ) and lakeisactive=1) a
# MAGIC WHERE  a.rnk = 1 and lakeisactive=1
# MAGIC )univ_2
# MAGIC  INNER JOIN standardised_subscribe.dbo_polmain main
# MAGIC ON Concat(main.polid, '_', main.unitpsu) = univ_2.policy_header_reference and
# MAGIC  main.lakeisactive=1
# MAGIC  INNER JOIN standardised_subscribe.dbo_inpol inpol
# MAGIC  ON Concat(inpol.polid, '_', inpol.unitpsu) = univ_2.policy_header_reference
# MAGIC  and inpol.lakeisactive=1
# MAGIC  )univ
# MAGIC  
# MAGIC  LEFT JOIN standardised_mds.dbo_activity_type acttype_ref
# MAGIC     ON univ.activity_type = acttype_ref.Code

# COMMAND ----------

# DBTITLE 1,Policy Characteristic
# MAGIC %sql
# MAGIC select upper(Policy_Header_Reference), upper(Policy_Characteristic_Name) from conformed_subscribe.Policy_Characteristic 
# MAGIC where LakeIsActive = 1 group by upper(Policy_Header_Reference), upper(Policy_Characteristic_Name)
# MAGIC minus 
# MAGIC select concat(upper(a.PolId),'_',a.UnitPsu) as Policy_Header_Reference, 
# MAGIC upper(a.sectty) as Policy_Characteristic_Name
# MAGIC FROM standardised_subscribe.dbo_PolSectNarr a
# MAGIC inner join standardised_subscribe.dbo_PolMain b
# MAGIC on upper(a.PolId) = upper(b.PolId) and upper(a.UnitPsu) = upper(b.UnitPsu)
# MAGIC and a.LakeIsActive = 1 and b.LakeIsActive = 1
# MAGIC where upper(SECTTY) in ('INDUSTRY SECTION', 'NBC FLAG', 'EXCESS REPORTER', 'FAC OBLIG DECLARATION', 'WAITING PERIOD (IN HOURS)',
# MAGIC 'NEW JERSEY SLA', 'SURPLUS LINES STATE', 'SURPLUS LINES LICENSE NUMBER', 'US REGIONAL STATUS', 'US SEGMENT',
# MAGIC 'WATERTRACE REFERENCE', 'WAR AND TERRORISM WRITEBACK', 'ULR %', 'ULR CODE', 'STATS NAME', 'STATS CODE',
# MAGIC 'SERADATA ID', 'SATELLITE NAME', 'RSCC', 'REVENUE CURRENCY', 'REVENUE', 'REGULATORY CLIENT CLASSIFICATION',
# MAGIC 'PRIVATE TERRORISM', 'PREMIUM PAYMENT WARRANTY (PPW)', 'MAXIMUM EXPECTED DEDUCTIONS (%)',
# MAGIC 'MAINTENANCE PERIOD MONTHS', 'HEADCOUNT', 'CLASH LOCATION', 'CASUALTY GROUPING', 'BULKING LINESLIP?',
# MAGIC 'ASSISTANT', 'AMERICAN DEPOSIT RECEIPT', 'AGGS REQUIRED')

# COMMAND ----------

# DBTITLE 1,Policy Header
# MAGIC %sql
# MAGIC select upper(Policy_Header_Reference) from conformed_subscribe.policy_header where lakeisactive=1  group by 1
# MAGIC minus
# MAGIC select Policy_Header_Reference from (
# MAGIC select upper(concat(polid,'_',UnitPsu)) as Policy_Header_Reference FROM standardised_subscribe.dbo_PolMain main where lakeisactive=1  group by 1
# MAGIC UNION
# MAGIC SELECT  upper(concat(polid,'_',UnitPsu)) FROM standardised_subscribe.dbo_InPol inpol where lakeisactive=1  group by 1
# MAGIC UNION
# MAGIC SELECT upper(concat(polid,'_',UnitPsu)) FROM standardised_subscribe.dbo_PolAnlyCd analyst_cd where lakeisactive=1  group by 1
# MAGIC UNION
# MAGIC select upper(concat(policy_broker.polid,'_',policy_broker.UnitPsu)) from  
# MAGIC FROM standardised_subscribe.dbo_PolBkr policy_broker
# MAGIC inner join standardised_subscribe.dbo_Bkr broker
# MAGIC ON broker.bkrSeqID = policy_broker.bkrSeqID
# MAGIC  where broker.lakeisactive=1  and policy_broker.lakeisactive =1 group by 1
# MAGIC UNION
# MAGIC select upper(concat(polid,'_',UnitPsu)) FROM standardised_subscribe.dbo_PolPmDeds deds where lakeisactive=1  group by 1
# MAGIC union
# MAGIC select upper(concat(polid,'_',UnitPsu)) FROM standardised_subscribe.dbo_PolLyr where lakeisactive=1  group by 1) a 

# COMMAND ----------

# DBTITLE 1,Policy Relationship
# MAGIC %sql
# MAGIC select Child_Policy_Header_Reference, Policy_Relationship_Type
# MAGIC from conformed_subscribe.Policy_Relationship where lakeIsActive = 1
# MAGIC minus
# MAGIC select 
# MAGIC concat(trim(lnk.ToPolId),'_',lnk.ToUnitPsu),
# MAGIC coalesce(nullif(trim(lnkty.Dsc),''),'UNCODED')
# MAGIC from standardised_subscribe.dbo_PolLnk lnk
# MAGIC inner join standardised_subscribe.dbo_PolLnkTy lnkty
# MAGIC on upper(trim(lnk.LnkTy)) = upper(trim(lnkty.ty)) 
# MAGIC and lnk.lakeIsActive = 1 and lnkty.lakeIsActive = 1
# MAGIC inner join standardised_subscribe.dbo_PolMain Frmain
# MAGIC on upper(trim(lnk.FrPolId)) = upper(Frmain.PolId) and upper(lnk.FrUnitPsu) = upper(Frmain.UnitPsu) 
# MAGIC and lnk.lakeIsActive = 1 and Frmain.lakeIsActive = 1
# MAGIC inner join standardised_subscribe.dbo_PolMain Tomain
# MAGIC on upper(trim(lnk.ToPolId)) = upper(Tomain.PolId) and upper(lnk.ToUnitPsu) = upper(Tomain.UnitPsu)
# MAGIC and lnk.lakeIsActive = 1 and Tomain.lakeIsActive = 1

# COMMAND ----------

# DBTITLE 1,Policy Section
# MAGIC %sql
# MAGIC select upper(Policy_section_reference), upper(Policy_Header_Reference), Policy_section_type  from conformed_subscribe.Policy_Section where lakeIsActive = 1
# MAGIC minus
# MAGIC select
# MAGIC concat(fld.PolId,'_',fld.UnitPsu) as Policy_Section_reference,
# MAGIC concat(fld.PolId,'_',fld.UnitPsu) as Policy_Header_Reference,
# MAGIC coalesce(NULLIF(trim(fld.SectTy), ''), 'UNCODED') as Policy_section_type
# MAGIC from
# MAGIC (select upper(PolId) as PolId, unitpsu, SectTy from standardised_subscribe.dbo_PolSectNarr where LakeIsActive = 1 group by 1,2,3) fld
# MAGIC --keeping the policies from PolMain table--
# MAGIC inner join 
# MAGIC (select polid,unitpsu from standardised_subscribe.dbo_PolMain where LakeIsActive = 1) main
# MAGIC on upper(fld.PolId) = upper(main.Polid) and fld.UnitPsu = main.UnitPsu

# COMMAND ----------

# DBTITLE 1,Policy Trust Fund Relationship
# MAGIC %sql
# MAGIC select upper(Policy_section_reference) from conformed_subscribe.policy_trust_fund_relationship where LakeIsActive = 1
# MAGIC minus
# MAGIC select
# MAGIC concat(fld.PolId,'_',fld.UnitPsu) as Policy_Section_reference from
# MAGIC (select upper(PolId) as PolId, unitpsu,Ty from standardised_subscribe.dbo_polanlycd 
# MAGIC where LakeIsActive = 1 group by 1,2,3) fld
# MAGIC --keeping the policies from PolMain table--
# MAGIC inner join 
# MAGIC (select polid,unitpsu from standardised_subscribe.dbo_PolMain where LakeIsActive = 1) main
# MAGIC on upper(fld.PolId) = upper(main.Polid) and fld.UnitPsu = main.UnitPsu and upper(fld.Ty) = 'TFC'

# COMMAND ----------

# DBTITLE 1,Premium
# MAGIC %sql
# MAGIC 
# MAGIC select upper(Policy_Header_Reference) as Policy_Header_Reference , upper(Policy_Section_Reference) as Policy_Header_Reference , Premium_Type, Coverage_type 
# MAGIC from conformed_subscribe.premium where lakeisActive=1
# MAGIC 
# MAGIC minus
# MAGIC 
# MAGIC select upper(CONCAT(PlM.PolId,'_',PlM.UnitPsu)) as Policy_Header_Reference,
# MAGIC upper(CONCAT(PlM.PolId,'_',PlM.UnitPsu)) as Policy_Section_Reference,
# MAGIC Plpm.EntT AS Premium_Type,
# MAGIC Concat('unmapped - ', Coalesce(Nullif(Trim(pac.cd), ''), 'UNCODED')) AS Coverage_Type
# MAGIC  from
# MAGIC (
# MAGIC SELECT upper(PolId) as PolId1, UnitPsu, COALESCE(NULLIF(trim(EntTy),''),'UNCODED') as EntT 
# MAGIC FROM standardised_subscribe.dbo_Polpm where lakeisactive=1 GROUP BY PolId1, UnitPsu, EntT) Plpm
# MAGIC Inner JOIN 
# MAGIC standardised_subscribe.dbo_PolMain PlM
# MAGIC ON upper(Plpm.PolId1) = upper(PlM.PolId) AND upper(Plpm.UnitPsu) = upper(PlM.UnitPsu) 
# MAGIC and plm.LakeIsActive = 1
# MAGIC left join standardised_subscribe.dbo_PolAnlyCd pac --to fetch coverage type for the policy
# MAGIC ON upper(Plpm.PolId1) = upper(pac.PolId) AND upper(Plpm.UnitPsu) = upper(pac.UnitPsu) AND upper(Pac.Ty) = 'BUSINESS'
# MAGIC and pac.LakeIsActive = 1
# MAGIC LEFT JOIN  standardised_mds.dbo_coverage_type_ref cv_ref
# MAGIC ON pac.cd = cv_ref.Code
# MAGIC AND lower(cv_ref.Name) = 'subscribe'

# COMMAND ----------

# DBTITLE 1,Pricing Rating
# MAGIC %sql
# MAGIC select upper(policy_section_reference), upper(policy_header_reference) from conformed_subscribe.Pricing_Rating where LakeIsActive = 1
# MAGIC minus
# MAGIC select upper(concat(fld.PolId,'_',fld.UnitPsu)) as Policy_Section_reference, upper(concat(fld.PolId,'_',fld.UnitPsu)) as policy_header_reference
# MAGIC from standardised_subscribe.dbo_PolFlds fld
# MAGIC --keeping the policies from PolMain table--
# MAGIC inner join standardised_subscribe.dbo_PolMain main
# MAGIC on upper(fld.PolId) = upper(main.Polid) and fld.UnitPsu = main.UnitPsu
# MAGIC and fld.LakeIsActive = 1 and main.LakeIsActive = 1

# COMMAND ----------

# DBTITLE 1,Reinstatement
# MAGIC %sql
# MAGIC 
# MAGIC select upper(Policy_Header_Reference), upper(Policy_Section_Reference), Reinstatement_Number
# MAGIC from conformed_subscribe.Reinstatement where lakeISActive = 1 
# MAGIC minus
# MAGIC select upper(concat(rnst.PolId,'_',rnst.UnitPsu)) as Policy_Header_Reference, upper(concat(rnst.PolId,'_',rnst.UnitPsu)) as Policy_Section_Reference, rnst.SeqNo
# MAGIC From standardised_subscribe.dbo_InPolRnst rnst
# MAGIC inner join standardised_subscribe.dbo_PolMain main
# MAGIC On upper(rnst.PolId) = upper(main.PolId) and upper(rnst.UnitPsu) = upper(main.UnitPsu)
# MAGIC and rnst.lakeISActive = 1  and main.lakeISActive = 1

# COMMAND ----------

# DBTITLE 1,Signing Message Transaction Narrative
# MAGIC %sql
# MAGIC SELECT UPPER(signing_message_narrative_reference), UPPER(Signing_Message_Detail_Reference) 
# MAGIC FROM conformed_subscribe.signing_message_transaction_narrative 
# MAGIC WHERE lakeISActive = 1 
# MAGIC MINUS
# MAGIC SELECT Concat_ws('_', narr.Trnid, narr.Unitpsu, narr.Seqlnno)                              AS Signing_Message_Narrative_Reference,
# MAGIC        Concat_ws('_', usm.Trnid, usm.Trncgy)                                                AS Signing_Message_Detail_Reference
# MAGIC FROM       standardised_subscribe.dbo_usmnarr narr
# MAGIC INNER JOIN standardised_subscribe.dbo_usmmain usm
# MAGIC         ON Upper(narr.Trnid) = Upper(usm.Trnid) AND Upper(narr.Trncgy) = Upper(usm.Trncgy) 
# MAGIC            AND usm.lakeISActive = 1 AND narr.lakeISActive = 1

# COMMAND ----------

# DBTITLE 1,Signing_Message_Treaty_Section_Amount
# MAGIC %sql
# MAGIC SELECT Upper(signing_message_treaty_section_code), Upper(signing_message_detail_reference), UPPER(Lloyds_Central_Accounting_Category_Code)
# MAGIC FROM   conformed_subscribe.signing_message_treaty_section_amount
# MAGIC WHERE  lakeisactive = 1
# MAGIC MINUS
# MAGIC SELECT  COALESCE(NULLIF(TRIM(tty.ttysect), ''), 'UNCODED'),
# MAGIC CONCAT_WS('_', usm.trnid, usm.trncgy),
# MAGIC tty.cac
# MAGIC FROM standardised_subscribe.dbo_usmtty tty
# MAGIC INNER JOIN standardised_subscribe.dbo_usmmain usm
# MAGIC ON Upper(tty.trnid) = Upper(usm.trnid)
# MAGIC AND Upper(tty.trncgy) = Upper(usm.trncgy)
# MAGIC AND tty.lakeisactive = 1
# MAGIC AND usm.lakeisactive = 1

# COMMAND ----------

# DBTITLE 1,Signing Transaction
# MAGIC %sql
# MAGIC SELECT upper(Signing_Installment_Reference) 
# MAGIC FROM conformed_subscribe.Signing_Transaction
# MAGIC WHERE LakeIsActive = 1
# MAGIC MINUS
# MAGIC SELECT upper(concat_ws('_',usm.TrnId,usm.UnitPsu,coalesce(usmd.InstNo, '01')))
# MAGIC FROM 
# MAGIC (select * from standardised_subscribe.dbo_USMMain where lakeisactive=1) usm
# MAGIC LEFT JOIN 
# MAGIC (select * from standardised_subscribe.dbo_USMDefd where lakeisactive=1) usmd
# MAGIC        ON upper(usm.TrnId)=upper(usmd.TrnId) AND upper(usm.TrnCgy) =upper(usmd.TrnCgy)