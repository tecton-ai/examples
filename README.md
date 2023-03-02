# Tecton Feature Examples

This repository contains curated examples of features using Tecton. Use this in your own projects by switching the sample data sources with your own data. 

## [1. Fraud Detection](Fraud)

Build powerful batch, streaming and real-time features to use in fraud detection models.

### [Data sources](Fraud/data_sources.py)

The data required for these features is transactional data. The features are built from historical transactions data (e.g files in s3, refreshed daily) as well as from transaction events streamed through Kinesis.

A sample of the batch data is publicly available in an s3 bucket `s3://tecton.ai.public/tutorials/fraud_demo/transactions/`

**Data preview**

|    | user_id           | transaction_id                   | category      |   amt |   is_fraud | merchant                           |   merch_lat |   merch_long | timestamp           |\n|---:|:------------------|:---------------------------------|:--------------|------:|-----------:|:-----------------------------------|------------:|-------------:|:--------------------|\n|  0 | user_884240387242 | 3eb88afb219c9a10f5130d0b89a13451 | gas_transport | 68.23 |          0 | fraud_Kutch, Hermiston and Farrell |       42.71 |     -78.3386 | 2023-06-20 10:26:41 |