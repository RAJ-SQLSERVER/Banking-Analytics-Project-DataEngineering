# Databricks notebook source
# MAGIC %md
# MAGIC # SILVER LAYER - Data Cleaning & Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean and Transform Accounts Data

# COMMAND ----------

# Databricks Notebook: 02_Silver_Transformation

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Read from Bronze
accounts_bronze = spark.table("banking_catalog.bronze.accounts")

# Data Cleaning Operations
accounts_silver = accounts_bronze \
    .withColumn("amount", coalesce(col("amount"), lit(0.0))) \
    .withColumn("status", when(col("status").isin(['ACTIVE', 'INACTIVE', 'PENDING']), col("status")).otherwise('UNKNOWN')) \
    .withColumn("type", when(col("type").isin(['LOAN', 'CURRENT', 'SAVINGS']), col("type")).otherwise('OTHER')) \
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
    .withColumn("remarks", coalesce(col("remarks"), lit("NO_REMARK"))) \
    .withColumn("flag", coalesce(col("flag"), lit("N"))) \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("data_source", lit("accounts.csv"))

# Remove duplicates
accounts_silver = accounts_silver.dropDuplicates(["account_id"])

# Write to Silver
accounts_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("abfss://silver@bankingdatalakest.dfs.core.windows.net/accounts")

print(f"✅ Accounts Silver: {accounts_silver.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean and Transform Customers Data

# COMMAND ----------

# Read from Bronze
customers_bronze = spark.table("banking_catalog.bronze.customers")

# Data Cleaning
customers_silver = customers_bronze \
    .withColumn("name", trim(col("name"))) \
    .withColumn("gender", when(col("gender").isin(['M', 'F']), col("gender")).otherwise('U')) \
    .withColumn("phone", regexp_replace(col("phone"), "[^0-9]", "")) \
    .withColumn("email", lower(trim(col("email")))) \
    .withColumn("city", initcap(trim(col("city")))) \
    .withColumn("state", upper(trim(col("state")))) \
    .withColumn("kyc_status", when(col("kyc_status").isin(['Y', 'N']), col("kyc_status")).otherwise('N')) \
    .withColumn("dob", to_date(col("dob"), "yyyy-MM-dd")) \
    .withColumn("created_date", to_date(col("created_date"), "yyyy-MM-dd")) \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("data_source", lit("customers.csv"))

# Handle NULLs
customers_silver = customers_silver.fillna({
    "phone": "0000000000",
    "email": "unknown@email.com",
    "kyc_status": "N"
})

# Remove duplicates
customers_silver = customers_silver.dropDuplicates(["customer_id"])

# Write to Silver
customers_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("abfss://silver@bankingdatalakest.dfs.core.windows.net/customers")

print(f"✅ Customers Silver: {customers_silver.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean and Transform Transactions Data

# COMMAND ----------

# Read from Bronze
transactions_bronze = spark.table("banking_catalog.bronze.transactions")

# Data Cleaning
transactions_silver = transactions_bronze \
    .withColumn("amount", coalesce(col("amount"), lit(0.0))) \
    .withColumn("status", when(col("status").isin(['ACTIVE', 'INACTIVE', 'PENDING']), col("status")).otherwise('UNKNOWN')) \
    .withColumn("type", when(col("type").isin(['LOAN', 'CURRENT', 'SAVINGS']), col("type")).otherwise('OTHER')) \
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
    .withColumn("remarks", coalesce(col("remarks"), lit("NO_REMARK"))) \
    .withColumn("flag", coalesce(col("flag"), lit("N"))) \
    .withColumn("year", year(col("date"))) \
    .withColumn("month", month(col("date"))) \
    .withColumn("quarter", quarter(col("date"))) \
    .withColumn("ingestion_timestamp", current_timestamp())

# Filter invalid transactions (negative amount or amount > 100000)
transactions_silver = transactions_silver \
    .filter(col("amount") >= 0) \
    .filter(col("amount") <= 100000) \
    .dropDuplicates(["transaction_id"])

# Write to Silver
transactions_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("abfss://silver@bankingdatalakest.dfs.core.windows.net/transactions")

print(f"✅ Transactions Silver: {transactions_silver.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean and Transform Loans Data

# COMMAND ----------

# Read from Bronze
loans_bronze = spark.table("banking_catalog.bronze.loans")

# Data Cleaning
loans_silver = loans_bronze \
    .withColumn("amount", coalesce(col("amount"), lit(0.0))) \
    .withColumn("status", when(col("status").isin(['ACTIVE', 'INACTIVE', 'PENDING']), col("status")).otherwise('UNKNOWN')) \
    .withColumn("type", when(col("type").isin(['LOAN', 'SAVINGS', 'CURRENT']), col("type")).otherwise('OTHER')) \
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
    .withColumn("remarks", coalesce(col("remarks"), lit("NO_REMARK"))) \
    .withColumn("flag", coalesce(col("flag"), lit("N"))) \
    .withColumn("ingestion_timestamp", current_timestamp())

# Remove duplicates
loans_silver = loans_silver.dropDuplicates(["loan_id"])

# Write to Silver
loans_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("abfss://silver@bankingdatalakest.dfs.core.windows.net/loans")

print(f"✅ Loans Silver: {loans_silver.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean and Transform Fraud Alerts Data

# COMMAND ----------

# Read from Bronze
fraud_bronze = spark.table("banking_catalog.bronze.fraud_alerts")

# Data Cleaning
fraud_silver = fraud_bronze \
    .withColumn("amount", coalesce(col("amount"), lit(0.0))) \
    .withColumn("status", when(col("status").isin(['ACTIVE', 'INACTIVE', 'PENDING']), col("status")).otherwise('UNKNOWN')) \
    .withColumn("type", when(col("type").isin(['LOAN', 'CURRENT', 'SAVINGS']), col("type")).otherwise('OTHER')) \
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
    .withColumn("remarks", coalesce(col("remarks"), lit("NO_REMARK"))) \
    .withColumn("flag", coalesce(col("flag"), lit("N"))) \
    .withColumn("is_high_risk", when(col("amount") > 40000, lit("Y")).otherwise(lit("N"))) \
    .withColumn("ingestion_timestamp", current_timestamp())

# Remove duplicates
fraud_silver = fraud_silver.dropDuplicates(["alert_id"])

# Write to Silver
fraud_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("abfss://silver@bankingdatalakest.dfs.core.windows.net/fraud_alerts")

print(f"✅ Fraud Alerts Silver: {fraud_silver.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Silver Tables in Unity Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Silver tables
# MAGIC CREATE TABLE banking_catalog.silver.accounts
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@bankingdatalakest.dfs.core.windows.net/accounts/';
# MAGIC
# MAGIC CREATE TABLE banking_catalog.silver.customers
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@bankingdatalakest.dfs.core.windows.net/customers/';
# MAGIC
# MAGIC CREATE TABLE banking_catalog.silver.transactions
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@bankingdatalakest.dfs.core.windows.net/transactions/';
# MAGIC
# MAGIC CREATE TABLE banking_catalog.silver.loans
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@bankingdatalakest.dfs.core.windows.net/loans/';
# MAGIC
# MAGIC CREATE TABLE banking_catalog.silver.fraud_alerts
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@bankingdatalakest.dfs.core.windows.net/fraud_alerts/';
# MAGIC
# MAGIC CREATE TABLE banking_catalog.silver.atm_transactions
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@bankingdatalakest.dfs.core.windows.net/atm_transactions/';
# MAGIC
# MAGIC CREATE TABLE banking_catalog.silver.branches
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@bankingdatalakest.dfs.core.windows.net/branches/';
# MAGIC
# MAGIC CREATE TABLE banking_catalog.silver.credit_cards
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@bankingdatalakest.dfs.core.windows.net/credit_cards/';
# MAGIC
# MAGIC CREATE TABLE banking_catalog.silver.kyc_documents
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@bankingdatalakest.dfs.core.windows.net/kyc_documents/';
# MAGIC
# MAGIC CREATE TABLE banking_catalog.silver.employees
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@bankingdatalakest.dfs.core.windows.net/employees/';