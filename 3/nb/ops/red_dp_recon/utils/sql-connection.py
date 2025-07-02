# Databricks notebook source
from pyspark.sql.functions import *
import pyodbc
import numpy as np
import re
import pandas as pd

# COMMAND ----------

class synapseConnection:
    def sqlDWConnect():
        dbconnDw = dbutils.secrets.get(scope = 'lza-da-kv-001-d', key = 'lza-dp-sqlacct-001-databricks-syn-connection')
        connDw = pyodbc.connect(dbconnDw, autocommit = True)
        cursorDw = connDw.cursor()
        return connDw
    def closeDWConnect(connDw):
        connDw.close()
    print("synapseConnection class setup completed")