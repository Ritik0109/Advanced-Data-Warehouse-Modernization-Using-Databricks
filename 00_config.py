# Databricks notebook source
class Config:
    def __init__(self):
        self.raw_path = spark.sql('describe external location `raw-data`').collect()[0][1]
        self.catalog = 'dev'
        self.db_name = 'financeco_raw_feeds'