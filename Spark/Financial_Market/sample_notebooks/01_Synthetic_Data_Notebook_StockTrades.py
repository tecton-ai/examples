# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ###Batch Data Creation Notebook
# MAGIC
# MAGIC This notebook will create two synthetic stock tables: 
# MAGIC 1) stock_demo_data.stock_trades: Second-by-second transaction data with price, volume, and timestamp of trade. 
# MAGIC 2) stock_demo_data.stock_daily_stats: Day-by-day stock data including volume, closing price, low price, and high price. 
# MAGIC
# MAGIC ####How to use: 
# MAGIC
# MAGIC The notebook attempts to store this data in a schema "stock_demo_data" within Databricks. The Tecton data source is set up with the demo code: Link and Link

# COMMAND ----------

# MAGIC %pip install dbldatagen

# COMMAND ----------

# DBTITLE 1,Generate second-by-second trade data
from pyspark.sql.types import LongType, IntegerType, StringType

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
    .withColumn("QUANTITY", "integer", minValue=1, maxValue=100, step=1, random=True)
    .withColumn("PRICE", "integer", minValue=100, maxValue=110, step=1, random=True)
    .withColumn("TIMESTAMP", "timestamp", begin="2019-01-02 09:30:00", 
                end="2019-01-02 16:00:00", 
                interval="1 second", random=True )
)

dfTestData2 = testDataSpec.build()

# COMMAND ----------

# DBTITLE 1,Create our demo schema
# MAGIC %sql 
# MAGIC CREATE SCHEMA IF NOT EXISTS stock_demo_data;

# COMMAND ----------

# DBTITLE 1,Preview the data
display(dfTestData2.orderBy("TIMESTAMP"))

# COMMAND ----------

# DBTITLE 1,Write to disk
dfTestData2.orderBy("TIMESTAMP").write.mode('overwrite').saveAsTable("stock_demo_data.stock_trades")

# COMMAND ----------

# DBTITLE 1,Generate day-by-day stock data
from pyspark.sql.types import LongType, IntegerType, StringType
import dbldatagen as dg

shuffle_partitions_requested = 1
data_rows = 365
partitions_requested = 1

spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

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
      .withColumn("VOLUME", "integer", minValue=100000, maxValue=1000000, step=10000, random=True)
      .withColumn("CLOSE", "integer", minValue=100, maxValue=110, step=1, random=True)
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
      .withColumn("TIMESTAMP", "timestamp", begin="2019-01-01 16:00:00", 
                end="2019-12-31 16:00:00", 
                interval="1 day", random=False)
  )
  array_of_dfs.append(testDataSpec.build())

from functools import reduce
from pyspark.sql import DataFrame

master_df = reduce(DataFrame.unionAll, array_of_dfs)

# COMMAND ----------

# DBTITLE 1,Preview daily stock data
display(master_df)

# COMMAND ----------

# DBTITLE 1,Write to disk
master_df.write.mode('overwrite').saveAsTable("stock_demo_data.stock_daily_stats")

# COMMAND ----------

import tecton

#Replace variables your own Tecton API Service Account Key and Tecton URL
token = dbutils.secrets.get(scope="nicklee", key="TECTON_API_KEY") 
tecton_url = dbutils.secrets.get(scope="nicklee", key="API_SERVICE")

tecton.set_credentials(token)
tecton.conf.set("TECTON_CLUSTER_NAME", "tecton-production")
tecton.test_credentials()

ws = tecton.get_workspace("stock_demo_workspace") 

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


