# Databricks notebook source
# DBTITLE 1,Registering all the tables in the Conformed Layer in a dataframe
# MAGIC %py
# MAGIC from pyspark import SparkConf, SparkContext
# MAGIC from pyspark.sql import SQLContext, HiveContext
# MAGIC cnf_tbl=spark.sql("""
# MAGIC select 'conformed_subscribe.business_classification' as table_name union
# MAGIC select 'conformed_subscribe.business_entity' as table_name union
# MAGIC select 'conformed_subscribe.claim' as table_name union
# MAGIC select 'conformed_subscribe.claim_activity' as table_name union
# MAGIC select 'conformed_subscribe.claim_component' as table_name union
# MAGIC select 'conformed_subscribe.claim_component_characteristic' as table_name union
# MAGIC select 'conformed_subscribe.claim_line' as table_name union
# MAGIC select 'conformed_subscribe.claim_loss_narrative' as table_name union
# MAGIC select 'conformed_subscribe.claim_message_detail' as table_name union
# MAGIC select 'conformed_subscribe.claim_movement' as table_name union
# MAGIC select 'conformed_subscribe.claim_note' as table_name union
# MAGIC select 'conformed_subscribe.claim_relationship' as table_name union
# MAGIC select 'conformed_subscribe.claim_transaction' as table_name union
# MAGIC select 'conformed_subscribe.clause' as table_name union
# MAGIC select 'conformed_subscribe.coverage' as table_name union
# MAGIC select 'conformed_subscribe.deduction' as table_name union
# MAGIC select 'conformed_subscribe.document' as table_name union
# MAGIC select 'conformed_subscribe.endorsement' as table_name union
# MAGIC select 'conformed_subscribe.epi_transaction' as table_name union
# MAGIC select 'conformed_subscribe.fil_2' as table_name union
# MAGIC select 'conformed_subscribe.fil_4' as table_name union
# MAGIC select 'conformed_subscribe.item' as table_name union
# MAGIC select 'conformed_subscribe.item_coverage_characteristic' as table_name union
# MAGIC select 'conformed_subscribe.limit' as table_name union
# MAGIC select 'conformed_subscribe.line' as table_name union
# MAGIC select 'conformed_subscribe.lloyds_risk_code' as table_name union
# MAGIC select 'conformed_subscribe.location_country' as table_name union
# MAGIC select 'conformed_subscribe.location_region' as table_name union
# MAGIC select 'conformed_subscribe.location_sub_division' as table_name union
# MAGIC select 'conformed_subscribe.loss_event' as table_name union
# MAGIC select 'conformed_subscribe.note' as table_name union
# MAGIC select 'conformed_subscribe.office' as table_name union
# MAGIC select 'conformed_subscribe.office_line_relationship' as table_name union
# MAGIC select 'conformed_subscribe.party' as table_name union
# MAGIC select 'conformed_subscribe.party_claim_activity_role' as table_name union
# MAGIC select 'conformed_subscribe.party_claim_component_role' as table_name union
# MAGIC select 'conformed_subscribe.party_claim_movement_role' as table_name union
# MAGIC select 'conformed_subscribe.party_deduction_role' as table_name union
# MAGIC select 'conformed_subscribe.party_policy_header_role' as table_name union
# MAGIC select 'conformed_subscribe.party_policy_section_role' as table_name union
# MAGIC select 'conformed_subscribe.party_rating' as table_name union
# MAGIC select 'conformed_subscribe.party_relationship' as table_name union
# MAGIC select 'conformed_subscribe.party_signing_transaction_role' as table_name union
# MAGIC select 'conformed_subscribe.peril' as table_name union
# MAGIC select 'conformed_subscribe.person' as table_name union
# MAGIC select 'conformed_subscribe.policy_activity' as table_name union
# MAGIC select 'conformed_subscribe.policy_characteristic' as table_name union
# MAGIC select 'conformed_subscribe.policy_header' as table_name union
# MAGIC select 'conformed_subscribe.policy_relationship' as table_name union
# MAGIC select 'conformed_subscribe.policy_section' as table_name union
# MAGIC select 'conformed_subscribe.policy_trust_fund_relationship' as table_name union
# MAGIC select 'conformed_subscribe.premium' as table_name union
# MAGIC select 'conformed_subscribe.pricing_rating' as table_name union
# MAGIC select 'conformed_subscribe.reinstatement' as table_name union
# MAGIC select 'conformed_subscribe.role' as table_name union
# MAGIC select 'conformed_subscribe.signing_message_detail' as table_name union
# MAGIC select 'conformed_subscribe.signing_message_transaction_narrative' as table_name union
# MAGIC select 'conformed_subscribe.signing_message_treaty_section_amount' as table_name union
# MAGIC select 'conformed_subscribe.signing_transaction' as table_name union
# MAGIC select 'conformed_subscribe.underwriting_authority' as table_name""")

# COMMAND ----------

in_tbl=''
cnf_tbl_collect=cnf_tbl.collect()
for row in cnf_tbl_collect:
    in_tbl=row['table_name']
    #performing grain check for individual tables on hashed business key
    x=spark.sql("select  hashedbusinesskey from {} group by 1".format(in_tbl))
    if x.count()<1:
        print ('No data present in table {}'.format(in_tbl))   

# COMMAND ----------

# DBTITLE 1,Grain Check for all conformed tables 
in_tbl=''
cnf_tbl_collect=cnf_tbl.collect()
for row in cnf_tbl_collect:
    in_tbl=row['table_name']
    #performing grain check for individual tables on hashed business key
    x=spark.sql("select hashedbusinesskey, lakeValidFromTimestamp, count(1) from {}  group by hashedbusinesskey, lakeValidFromTimestamp having count(1)>1".format(in_tbl))
    if x.count()>1:
        print ('Grain check for table {} failed'.format(in_tbl))   

# COMMAND ----------

# DBTITLE 1,Count Comparison with previous run
in_tbl=''
in_version_num=''
cnf_tbl_collect=cnf_tbl.collect()
for row in cnf_tbl_collect:
    in_tbl=row['table_name']
    #performing grain check for individual tables on hashed business key
    xx=spark.sql("DESCRIBE HISTORY {}".format(in_tbl)).where("operation == 'MERGE'")
    xx.createOrReplaceTempView('src')
    y=spark.sql("select (max(version)-1) as previous_version_number from (select * from src)")
    for row in y.collect():
        in_version_num=row['previous_version_number']
        if in_version_num is not None:
            z=spark.sql("select hashedbusinesskey from {}".format(in_tbl))
            n=spark.sql("select hashedbusinesskey from {}@v{}".format(in_tbl,in_version_num))
            x=('{} , '+ str(n.count()) +' --> previous_run_count, '+ str(z.count()) +' --> latest_run_count, '+ str(abs(z.count())-abs(n.count()))+ '  delta_records').format(in_tbl)
            print(x)