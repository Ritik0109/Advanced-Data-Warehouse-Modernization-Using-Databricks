# Databricks notebook source
# MAGIC %run /Workspace/FinanceCo/00_config

# COMMAND ----------

from pyspark.sql.functions import *

class Upserter:
    def __init__(self, merge_query, temp_view):
        self.merge_query = merge_query
        self.temp_view = temp_view

    def merge_batch(self, batch_df, batch_id):
        batch_df.createOrReplaceTempView(self.temp_view)
        spark.sql(self.merge_query)
        # batch_df._jdf.sparkSession().sql(self.merge_query)

class SilverIngestion:
    def __init__(self):
#Using Config files to get the variables for path and objects
        self.config = Config()
        self.catalog = self.config.catalog
        self.db_name = self.config.db_name
        self.raw_path = self.config.raw_path

### Adding raw path to store Autoloader Schema reference and Checkpoints
        self.schema_ref = self.raw_path + "autoloader-reference/"
        self.base_schema = f"{self.schema_ref}/schema/silver/"
        self.base_checkpoint = f"{self.schema_ref}/checkpoint/silver/"


    def silver_fact_transaction(self):
        # Defining fact table schema
        schema = ("transaction_id int", "account_id int", "product_id int", "branch_id int", "transaction_date date", "amount float", "transaction_type string", "channel string")
        schema_sql = ",\n".join(schema)

        # Creating fact table
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.transactions_fact_silver ({schema_sql}) USING DELTA PARTITIONED BY (transaction_date)""")

        # Reading data from bronze transaction table
        df = spark.readStream\
            .table(f"{self.catalog}.{self.db_name}.transactions_raw")\
            .withColumn("transaction_date",to_timestamp(col("transaction_date"), "yyyy-MM-dd"))\
            .withWatermark("transaction_date", "2 day")\
            .dropDuplicates(["transaction_id"])\
            .filter(col("transaction_id").isNotNull() & col("transaction_date").isNotNull())\
            .drop("_rescued_data")

        # Writing data to silver transaction table
        writer = df.writeStream\
            .format("delta")\
            .option("checkpointLocation", f"{self.base_checkpoint}/transactions/")\
            .outputMode("append")\
            .trigger(availableNow=True)\
            .toTable(f"{self.catalog}.{self.db_name}.transactions_fact_silver")
         
        return writer

    def silver_customer_update(self):
        schema = ("customer_id int", "first_name string", "last_name string", "email string", "phone string", "country string", "risk_rating string", "effective_start_date date", "effective_end_date timestamp", "is_current string")
        schema_sql = ",\n".join(schema)

        # Creating fact table
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.dim_customer ({schema_sql}) USING DELTA """)

        df = spark.readStream\
            .table(f"{self.catalog}.{self.db_name}.customer_raw")\
            .withColumn("effective_start_date",to_date(col("effective_start_date"), "dd-MM-yyyy"))\
            .filter(col("customer_id").isNotNull())\
            .withColumn("event_date", current_timestamp())\
            .drop("_rescued_data")
            
        df.createOrReplaceTempView("dim_customer_temp")
        temp_v = "dim_customer_temp"
        query = f"""
            MERGE INTO {self.catalog}.{self.db_name}.dim_customer t
                USING dim_customer_temp AS s
                ON t.customer_id = s.customer_id AND t.is_current = TRUE
                WHEN MATCHED THEN
                    UPDATE SET *
                WHEN NOT MATCHED THEN
                    INSERT *
            """

        data_upsert = Upserter(query, temp_v)
        writer = df.writeStream\
            .foreachBatch(data_upsert.merge_batch)\
            .option("checkpointLocation", f"{self.base_checkpoint}/customer/")\
            .outputMode("update")\
            .trigger(availableNow=True)\
            .start()
        return writer

    def silver_account_update(self):
        schema = ("account_id int", "customer_id int", "account_type string", "currency string", "open_date date", "status string")
        schema_sql = ",\n".join(schema)

        spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.dim_accounts ({schema_sql}) USING DELTA """)

        df = spark.readStream\
                .table(f"{self.catalog}.{self.db_name}.accounts_raw")\
                .filter(col("account_id").isNotNull())\
                .drop("_rescued_data")

        df.createOrReplaceTempView("dim_accounts_temp")
        temp_v = "dim_accounts_temp"
        query = f"""
        MERGE INTO {self.catalog}.{self.db_name}.dim_accounts t 
        USING dim_accounts_temp s
        ON s.account_id = t.account_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """ 
        data_upsert = Upserter(query, temp_v)
        writer = df.writeStream\
            .foreachBatch(data_upsert.merge_batch)\
            .option("checkpointLocation", f"{self.base_checkpoint}/accounts/")\
            .outputMode("update")\
            .trigger(availableNow=True)\
            .start()
        return writer

    def silver_branch_update(self):
        schema = ("branch_id int", "branch_name string", "region string")
        schema_sql = ",\n".join(schema)
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.dim_branch ({schema_sql}) USING DELTA """)

        df = spark.readStream\
                .table(f"{self.catalog}.{self.db_name}.branch_raw")\
                .filter(col("branch_id").isNotNull())\
                .drop("_rescued_data")

        writer = df.writeStream\
            .format("delta")\
            .outputMode("append")\
            .trigger(availableNow=True)\
            .option("checkpointLocation", f"{self.base_checkpoint}/branch/")\
            .toTable(f"{self.catalog}.{self.db_name}.dim_branch")
        return writer
    
    def silver_product_update(self):
        schema = ("product_id int", "product_name string", "product_category string")
        schema_sql = ",\n".join(schema)
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.dim_product ({schema_sql}) USING DELTA """)

        df = spark.readStream\
                .table(f"{self.catalog}.{self.db_name}.product_raw")\
                .filter(col("product_id").isNotNull())\
                .drop("_rescued_data")

        writer = df.writeStream\
            .format("delta")\
            .outputMode("append")\
            .trigger(availableNow=True)\
            .option("checkpointLocation", f"{self.base_checkpoint}/product/")\
            .toTable(f"{self.catalog}.{self.db_name}.dim_product")
        return writer

    def ingestion_start(self):
        print("Starting silver layer ingestion...")
        self.silver_fact_transaction()
        self.silver_customer_update()
        self.silver_account_update()
        self.silver_branch_update()
        self.silver_product_update()
        print("Completing silver layer ingestion...")

# COMMAND ----------

data_ingestion = SilverIngestion()
data_ingestion.ingestion_start()

# COMMAND ----------

# Running optimize after data is ingested
config = Config()
spark.sql(f"OPTIMIZE {config.catalog}.{config.db_name}.transactions_fact_silver") 