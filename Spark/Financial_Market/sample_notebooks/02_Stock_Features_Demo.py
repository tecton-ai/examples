# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Before running this notebook, make sure you've created your synthetic data and run "tecton apply" with the demo project to define the data sources, feature views, and feature service. 

# COMMAND ----------

import tecton
from datetime import datetime, timedelta

#Replace variables your own Tecton API Service Account Key and Tecton URL
token = dbutils.secrets.get(scope="nicklee", key="TECTON_API_KEY") 
tecton_url = dbutils.secrets.get(scope="nicklee", key="API_SERVICE")

tecton.set_credentials(token)
tecton.conf.set("TECTON_CLUSTER_NAME", "tecton-production")
tecton.test_credentials()

#Get your target workspace in Tecton
#Most of this notebook will work with a development workspace, but you must deploy to a live workspace to enable online serving. 
ws = tecton.get_workspace("stock_demo_workspace_live") 

# COMMAND ----------

# DBTITLE 1,Inspect stock trades data source
stock_ds = ws.get_data_source("stock_trades_batch")
stock_trades_df = stock_ds.get_dataframe(start_time=datetime(2019, 1, 1), end_time=datetime.now()).to_spark()

#Preview data to make sure we can load it from our data source
display(stock_trades_df)

# COMMAND ----------

# DBTITLE 1,Inspect daily stock statistics data source
stock_daily_ds = ws.get_data_source("stock_daily_stats")
stock_daily_stats = stock_daily_ds.get_dataframe(start_time=datetime(2019, 1, 1), end_time=datetime.now()).to_spark()
display(stock_daily_stats)

# COMMAND ----------

# DBTITLE 1,View our closing price metrics feature view
stock_max_closing_prices = ws.get_feature_view("closing_prices_metrics")
stock_max_closing_prices_fv = stock_max_closing_prices.run(datetime(2019, 1, 1), datetime.now()).to_spark()
display(stock_max_closing_prices_fv)

# COMMAND ----------

# DBTITLE 1,Inspect stock trades feature view
stock_daily_transactions = ws.get_feature_view("daily_transactions_stats")
stock_daily_transactions = stock_daily_transactions.run(datetime(2019, 1, 1), datetime.now()).to_spark()
display(stock_daily_transactions)

# COMMAND ----------

# DBTITLE 1,Build spine for Feature View
from pyspark.sql import functions as F
testds = ws.get_feature_view("todays_closing_price")
test = testds.run(datetime.now() - timedelta(days=180), datetime.now()).to_spark() #grab the last 180 days for simplicity

#Add an hour to the spine timestamp so we get that day's closing price when we query that date (we're using a 1 hr batch interval)
test = test.withColumn('TIMESTAMP_ADJUSTED', F.col("TIMESTAMP") + F.expr('INTERVAL 1 HOURS')) 
display(test.orderBy("SYMBOL", "TIMESTAMP_ADJUSTED"))

# COMMAND ----------

# DBTITLE 1,Test on-demand feature view to compare today's close vs yesterday's close
odfv_daily_returns = ws.get_feature_view("percentage_daily_returns")
daily_returns_result = odfv_daily_returns.get_historical_features(spine = test["SYMBOL", "TIMESTAMP_ADJUSTED"], timestamp_key = "TIMESTAMP_ADJUSTED", from_source=True).to_spark()
display(daily_returns_result)

# COMMAND ----------

# DBTITLE 1,Feature Service data w/ on-demand features and batch features
ws = tecton.get_workspace("stock_demo_workspace_live") 
stock_daily_stats_feature_service = ws.get_feature_service("stock_daily_stats_feature_service")

training_data = stock_daily_stats_feature_service.get_historical_features(
    spine=test["SYMBOL", "TIMESTAMP_ADJUSTED"], timestamp_key="TIMESTAMP_ADJUSTED", from_source=True
).to_spark() 

display(training_data.orderBy("TIMESTAMP_ADJUSTED", "SYMBOL"))

# COMMAND ----------

# DBTITLE 1,After deploying to a live workspace, you can test the real-time feature service
import requests, json

data = {
  "params": {
    "feature_service_name": "stock_daily_stats_feature_service",
    "join_key_map": {
      "SYMBOL": "DEF"
    },
    "workspace_name": "stock_demo_workspace_live", 
    "metadataOptions": {"includeNames": True}, 
  }
}

# This will only work for a live workspace
r = requests.post('https://app.tecton.ai/api/v1/feature-service/get-features', data=json.dumps(data), headers={'Authorization': 'Tecton-key ' + token})

response_body = r.json()
for (a, b) in zip(response_body['result']['features'], response_body['metadata']['features']): 
  print(b['name'] + ": " + str(a))


# COMMAND ----------



# COMMAND ----------


