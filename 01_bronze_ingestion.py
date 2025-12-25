# Databricks notebook source
# MAGIC %run /Workspace/FinanceCo/00_config

# COMMAND ----------

class BronzeIngest:
    def __init__(self):
#Using Config files to get the variables for path and objects
        self.config = Config()
        self.catalog = self.config.catalog
        self.db_name = self.config.db_name
        self.raw_path = self.config.raw_path

### Adding raw path to store Autoloader Schema reference and Checkpoints
        self.schema_ref = self.raw_path + "autoloader-reference/"
        self.base_schema = f"{self.schema_ref}/schema/bronze/"
        self.base_checkpoint = f"{self.schema_ref}/checkpoint/bronze/"

#### Checking if the objects exists
    def ddl_objects(self):
        spark.sql(f"create catalog if not exists {self.catalog}")
        spark.sql(f"create schema if not exists {self.catalog}.{self.db_name}")


    def bronzeIngestion(self):
        path_variables = ["transactions", "accounts", "branch", "customer", "product"]
        
        for item in path_variables: 
            #### Reading source files using autoloader
            df = spark.readStream\
                .format("cloudFiles")\
                .option("cloudFiles.format","csv")\
                .option("header", "true")\
                .option("cloudFiles.schemaLocation",f"{self.base_schema}/{item}/")\
                .option("cloudFiles.schemaEvolutionMode", "rescue")\
                .load(f"{self.raw_path}/{item}/")

            #### Writing files to tables        
            query = df.writeStream\
                        .format("delta")\
                        .option("checkpointLocation", f"{self.base_checkpoint}/{item}/")\
                        .outputMode("append")\
                        .trigger(availableNow=True)\
                        .toTable(f"{self.catalog}.{self.db_name}.{item}_raw")
            print(f"Ingestion completed for {item}...")


    def ingestion_start(self):
        self.ddl_objects()
        print('Starting ingesting from landing...')
        self.bronzeIngestion()
        print('Data ingestion complete...')
        # self.validate_ingestion()
        # print("Bronze layer update complete... Starting Silver layer now...")

# COMMAND ----------

intialize_ingestion = BronzeIngest()
intialize_ingestion.ingestion_start()