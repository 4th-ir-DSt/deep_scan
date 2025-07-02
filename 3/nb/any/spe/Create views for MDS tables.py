# Databricks notebook source
# DBTITLE 1,Business_Classification
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW conformed_Subscribe.vw_Business_Classification AS
# MAGIC SELECT * from standardised_mds.dbo_business_classification

# COMMAND ----------

# DBTITLE 1,Business_Entity
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW conformed_Subscribe.vw_Business_Entity AS
# MAGIC select * from standardised_mds.dbo_business_entity_ref

# COMMAND ----------

# DBTITLE 1,FIL_2
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW conformed_Subscribe.vw_Fil_2 AS
# MAGIC select * from standardised_mds.dbo_fil_2_ref

# COMMAND ----------

# DBTITLE 1,FIL_4
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW conformed_Subscribe.vw_Fil_4 AS
# MAGIC select * from standardised_mds.dbo_fil_4_ref

# COMMAND ----------

# DBTITLE 1,Lloyds_risk_code
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW conformed_Subscribe.vw_Lloyds_Risk_Code AS
# MAGIC select * from standardised_mds.dbo_lloyds_risk_code

# COMMAND ----------

# DBTITLE 1,Location_country
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW conformed_Subscribe.vw_Location_Country AS
# MAGIC select * from standardised_mds.dbo_location_country_ref

# COMMAND ----------

# DBTITLE 1,Location_region
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW conformed_Subscribe.vw_Location_Region AS
# MAGIC select * from standardised_mds.dbo_location_region

# COMMAND ----------

# DBTITLE 1,Location_sub_Division
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW conformed_Subscribe.vw_Location_Sub_Division AS
# MAGIC select * from standardised_mds.dbo_location_sub_division

# COMMAND ----------

# DBTITLE 1,Office_line_relationship
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW conformed_Subscribe.vw_Office_Line_Relationship_Type AS
# MAGIC select * from standardised_mds.dbo_office_line_relationship_type

# COMMAND ----------

# DBTITLE 1,Office_name
# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW conformed_Subscribe.vw_Office AS
# MAGIC select * from standardised_mds.dbo_office_name

# COMMAND ----------

# DBTITLE 1,Party_rating
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW conformed_Subscribe.vw_Party_Rating AS
# MAGIC select * from standardised_mds.dbo_party_rating

# COMMAND ----------

# DBTITLE 1,Party_relationship
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW conformed_Subscribe.vw_Party_Relationship_Type AS
# MAGIC select * from standardised_mds.dbo_party_relationship_type

# COMMAND ----------

# DBTITLE 1,Peril
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW conformed_Subscribe.vw_Peril AS
# MAGIC select * from standardised_mds.dbo_peril_ref

# COMMAND ----------

# DBTITLE 1,Role
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW conformed_Subscribe.vw_Role AS
# MAGIC select * from standardised_mds.dbo_role_type