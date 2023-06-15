# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ###Data Creation Notebook
# MAGIC
# MAGIC This notebook will create two synthetic stock tables: 
# MAGIC 1) stock_demo_data.stock_trades: Second-by-second transaction data with price, volume, and timestamp of trade. 
# MAGIC 2) stock_demo_data.stock_daily_stats: Day-by-day stock data including volume, closing price, low price, and high price. 
# MAGIC
# MAGIC Optional: This notebook also includes an example of streaming data into the Ingest API and materializing that in a Streaming Feature View. 
# MAGIC
# MAGIC ####How to use: 
# MAGIC
# MAGIC The notebook attempts to store this data in a schema "stock_demo_data" within Databricks. The Tecton data source is set up with the demo code: Link and Link
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %pip install dbldatagen

# COMMAND ----------

# DBTITLE 1,Generate second-by-second trade data
from pyspark.sql.types import LongType, IntegerType, StringType
from datetime import datetime
from datetime import date
from datetime import timedelta

import dbldatagen as dg

stock_tickers = [
    "ABC", "DEF", "GHI", "JKLM", "NOP"
]

shuffle_partitions_requested = len(stock_tickers)
device_population = 100000
data_rows = 20 * 1000
partitions_requested = len(stock_tickers)

spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

ticker_weights = [
    2,3,1,1,1
]

yesterdays_date = datetime.today() - timedelta(days=1)
one_year_prior_date = datetime.today() - timedelta(days=365)
yesterdays_date_string = yesterdays_date.strftime("%Y-%m-%d")
one_year_prior_date_string = one_year_prior_date.strftime("%Y-%m-%d")

testDataSpec = (
    dg.DataGenerator(spark, name="device_data_set", rows=data_rows, 
                     partitions=partitions_requested)
    .withIdOutput()
    # we'll use hash of the base field to generate the ids to
    # avoid a simple incrementing sequence
    .withColumn("trade_id", "long", minValue=0x1000000000000, omit=True, baseColumnType="hash")
    .withColumn(
        "trade_identification", "string", format="0x%013x", baseColumn="trade_id"
    )
    .withColumn("SYMBOL", "string", values=stock_tickers, weights=ticker_weights, 
                baseColumn="trade_id")
    .withColumn("QUANTITY", "long", minValue=1, maxValue=100, step=1, random=True)
    .withColumn("PRICE", "long", minValue=100, maxValue=110, step=1, random=True)
    .withColumn("TIMESTAMP", "timestamp", begin=one_year_prior_date_string + " 09:30:00", 
                end=yesterdays_date_string + " 16:00:00", 
                interval="1 second", random=True )
)

dfTestData2 = testDataSpec.build()

# COMMAND ----------

# DBTITLE 1,Create our demo schema
# MAGIC %sql 
# MAGIC CREATE SCHEMA IF NOT EXISTS stock_demo_data;

# COMMAND ----------

# DBTITLE 1,Preview the data
from pyspark.sql.functions import col
display(dfTestData2.orderBy("TIMESTAMP").drop(col("id")))

# COMMAND ----------

# DBTITLE 1,Write to disk
from pyspark.sql.functions import col
(dfTestData2
 .orderBy("TIMESTAMP")
 .drop(col("id"))
 .write.mode('overwrite')
 .saveAsTable("stock_demo_data.stock_trades"))

# COMMAND ----------

# DBTITLE 1,Generate day-by-day stock data
from pyspark.sql.types import LongType, IntegerType, StringType
import dbldatagen as dg

shuffle_partitions_requested = 1
data_rows = 365
partitions_requested = 1

spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

yesterdays_date = datetime.today() - timedelta(days=1)
one_year_prior_date = datetime.today() - timedelta(days=365)
yesterdays_date_string = yesterdays_date.strftime("%Y-%m-%d")
one_year_prior_date_string = one_year_prior_date.strftime("%Y-%m-%d")

symbols = ["ABC", "DEF", "GHI", "JKLM", "NOP"]
array_of_dfs = [] 
for symbol in symbols: 
  stock_tickers = [
      symbol
  ]

  testDataSpec = (
      dg.DataGenerator(spark, name="stock_data", rows=data_rows, 
                      partitions=partitions_requested)
      .withIdOutput()
      .withColumn("SYMBOL", "string", values=stock_tickers)
      .withColumn("VOLUME", "integer", minValue=100000, maxValue=1000000, step=10000, random=True, randomSeed=-1)
      .withColumn("CLOSE", "integer", minValue=100, maxValue=110, step=1, random=True, randomSeed=-1)
      .withColumn(
          "LOW",
          "integer",
          expr="CLOSE - floor(rand()*10)",
          baseColumn="CLOSE",
      )
      .withColumn(
          "HIGH",
          "integer",
          expr="CLOSE + floor(rand() * 10)",
          baseColumn="CLOSE",
      )
      .withColumn("TIMESTAMP", "timestamp", begin= one_year_prior_date_string + " 16:00:00", 
                end=yesterdays_date_string + " 16:00:00", 
                interval="1 day", random=False)
  )
  array_of_dfs.append(testDataSpec.build())

from functools import reduce
from pyspark.sql import DataFrame

master_df = reduce(DataFrame.unionAll, array_of_dfs)

# COMMAND ----------

# DBTITLE 1,Preview daily stock data
from pyspark.sql.functions import col
display(master_df.orderBy("TIMESTAMP", "SYMBOL").drop(col("id")))

# COMMAND ----------

# DBTITLE 1,Write to disk
from pyspark.sql.functions import col
(master_df
 .drop(col("id"))
 .write.mode('overwrite')
 .saveAsTable("stock_demo_data.stock_daily_stats"))

# COMMAND ----------

import tecton

#Replace variables your own Tecton API Service Account Key and Tecton URL
token = dbutils.secrets.get(scope="nicklee", key="STAGING_TECTON_API_KEY") 
tecton_url = dbutils.secrets.get(scope="nicklee", key="STAGING_API_SERVICE")

tecton.set_credentials(token, tecton_url=tecton_url)
tecton.conf.set("TECTON_CLUSTER_NAME", "tecton-staging")
tecton.test_credentials()

#Get your target workspace in Tecton
#Most of this notebook will work with a development workspace, but you must deploy to a live workspace to enable online serving. 
ws = tecton.get_workspace("nicklee-staging-live") 

# COMMAND ----------

# DBTITLE 1,Validate our batch data sources
from tecton import HiveConfig, BatchSource, DatetimePartitionColumn

stock_daily_stats = BatchSource(
    name='stock_daily_stats',
    batch_config=HiveConfig(
        database='stock_demo_data',
        table='stock_daily_stats',
        timestamp_field='TIMESTAMP'
    ),
    owner='nlee@tecton.ai',
    tags={'release': 'production'}
)

stock_daily_stats.validate()

stock_trades = BatchSource(
    name='stock_trades_batch',
    batch_config=HiveConfig(
        database='stock_demo_data',
        table='stock_trades',
        timestamp_field='TIMESTAMP'
    ),
    owner='nlee@tecton.ai',
    tags={'release': 'production'}
)

stock_trades.validate()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ####The below code blocks will only work after you have defined your feature views and run "tecton apply" on your workspace. 

# COMMAND ----------

# DBTITLE 1,Optional: Streaming Data Simulator
#Leave this code running to simulate a streaming data stream with Tecton's Ingest API 
import requests, json, uuid
from datetime import datetime
import time
import random

looping = False #Change to True if you want to kick off a constant stream of data to the Ingest API. Let code run in the background to simulate stream. 
verbose = True

def stream_a_trade(verbose = False): 
  current_timestamp_formatted = datetime.utcnow().strftime("%Y-%m-%d" + "T" + "%H:%M:%S" + "Z") #The expected Zulu Time format for Tecton

  symbol = random.choice(["ABC", "DEF", "GHI", "JKLM", "NOP"])
  data = {
    "workspace_name": "nicklee-staging-live", #replace with your workspace name 
    "dry_run": False,
    "records": {
      "stock_transactions_event_source": [
        {
          "record": {
            "trade_identification": str(uuid.uuid4()),
            "SYMBOL": symbol,
            "QUANTITY": random.randint(1,1000),
            "PRICE": random.randint(100,110), 
            "TIMESTAMP": current_timestamp_formatted
          }
        }
      ]
    }
  }

  ingest_endpoint = 'https://preview.staging.tecton.ai/ingest' #replace this with your correct endpoint https://preview.<your_cluster>.tecton.ai/ingest if you have access to the preview

  r = requests.post(ingest_endpoint, data=json.dumps(data), headers={'Authorization': 'Tecton-key ' + token})  # This will only work for a live workspace
  
  if verbose:
    print(json.dumps(data, indent=4))
    print(r.json())

while looping is True: 
  stream_a_trade(verbose = verbose)
  time.sleep(3) #sleep 3 seconds 

stream_a_trade(verbose=True)

# COMMAND ----------

from pyspark.sql.functions import col, asc, desc 
stock_daily_transactions = ws.get_feature_view("live_trading_stats")

#You can confirm that our Online Store is being updated with streaming features by viewing the freshness 
stock_daily_transactions.summary()

# COMMAND ----------

#Here is a quick check of the Online Store; you can see features are being updated on-the-fly for each symbol as new data is streaming in from Ingest API. 
#Try submitting more data and re-running this code block to see the changes. 

for symbol in ["ABC", "DEF", "GHI", "JKLM", "NOP"]: 
  print(symbol + ": " + str(stock_daily_transactions.get_online_features(join_keys={'SYMBOL':symbol}).to_dict()))
