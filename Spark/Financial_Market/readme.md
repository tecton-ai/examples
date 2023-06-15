# Financial Market Sample Code

This feature repository includes code snippets that show how to build feature pipelines for training and serving, using a fictional financial market use-case (stock market). 

### Getting Started
1. The sample_notebooks folder contains two helper notebooks. One notebook allows you to synthesize fake stock data in a Databricks notebook. You should inspect this notebook, rename the variables to your liking, and register those tables in your hive metastore. Code at the bottom of the notebook will test that the data sources are set up for Tecton access.
2. Once your synthetic data is ready, create a tecton workspace for this demo.
3. Run "tecton apply" in the Financial_Market directory to set up all of the data sources, feature views, and on-demand features. 
4. If all of the above were successful, go back into Databricks and run through the second notebook in sample_notebooks. You'll need to replace the credential variables with your own Tecton credentials. This notebook shows how to access the Tecton features we just created. 

#### v0.7b Streaming Dependency
The Streaming Feature View demonstrates a transformation on PushSource data ingested through Tecton's Ingest API. This capability is only available in [Tecton v0.7b](https://docs.tecton.ai/using-the-ingestion-api#transformations). You can avoid the 0.7b dependency by removing the Streaming Feature View before you run "tecton apply". 
