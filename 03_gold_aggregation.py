# Databricks notebook source
# MAGIC %run /Workspace/FinanceCo/00_config

# COMMAND ----------

from pyspark.sql.functions import *

class GoldIngestion:
        def __init__(self):
                #Using Config files to get the variables for path and objects
                self.config = Config()
                self.catalog = self.config.catalog
                self.db_name = self.config.db_name
                self.raw_path = self.config.raw_path

                ### Adding raw path to store Autoloader Schema reference and Checkpoints
                self.schema_ref = self.raw_path + "autoloader-reference/"
                self.base_schema = f"{self.schema_ref}/schema/gold/"
                self.base_checkpoint = f"{self.schema_ref}/checkpoint/gold/"

        def gold_fact_transaction(self):
                customer_df = spark.read.table(f"{self.catalog}.{self.db_name}.dim_customer")
                account_df = spark.read.table(f"{self.catalog}.{self.db_name}.dim_accounts")
                branch_df = spark.read.table(f"{self.catalog}.{self.db_name}.dim_branch")
                product_df = spark.read.table(f"{self.catalog}.{self.db_name}.dim_product")
                fact_trn_df = spark.readStream\
                                .table(f"{self.catalog}.{self.db_name}.transactions_fact_silver")

                customer_normalized = (
                customer_df.alias("c")
                        .filter("is_current = 'TRUE'")
                        .join(account_df.alias("a"), on="customer_id", how="left")
                        .select(
                        col("a.account_id"), col("a.customer_id"),
                        col("c.first_name"), col("c.last_name"),
                        col("c.risk_rating"), col("c.country"),
                        col("a.account_type"), col("a.open_date"), col("a.status")
                        )
                )

                # customer_normalized.cache()

                joined_df = (
                fact_trn_df
                        .join(customer_normalized, on="account_id", how="left")
                        .join(broadcast(branch_df), on="branch_id", how="left")
                        .join(broadcast(product_df), on="product_id", how="left")
                        .select(
                        col("customer_id"), col("first_name"), col("last_name"),
                        col("risk_rating"), col("country"), col("account_type"),
                        col("transaction_id"), col("transaction_date"), col("amount"),
                        col("transaction_type"), col("channel"),
                        col("branch_name"), col("region"),
                        col("product_name"), col("product_category")
                        )
                )

                joined_df.writeStream\
                        .format("delta")\
                        .option("checkpointLocation", f"{self.base_checkpoint}/agg_transaction/")\
                        .outputMode("append")\
                        .trigger(availableNow=True)\
                        .toTable(f"{self.catalog}.{self.db_name}.agg_transaction_gold")

# COMMAND ----------

ingestion_start = GoldIngestion()
ingestion_start.gold_fact_transaction()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from dev.financeco_raw_feeds.agg_transaction_gold
# MAGIC -- select * from dev.financeco_raw_feeds.dim_customer
# MAGIC -- order by customer_id
# MAGIC -- select * from dev.financeco_raw_feeds.transactions_raw
# MAGIC