# One Click Data Pipeline for E-commerce Profitability (dbt, Shopify, Facebook, Fedex, GLS and other costs)

## End result of the project

Here are the few dashboards that were built on top of the data model built from this repo. 

** 1. Overview of Revenue and Profits **

** 2. Overview of Impact of different Cost Factors in EUR and % of Revenue **

** 3. Map of Countries with highest Profit Margins after deducting all the costs from Order Revenues **

** 4. Overview of Performance of Paid Marketing and ROI (Profits made from 1 EUR of Marketing Investment) **



## Overview of Repo

This repository is a production dbt pipeline example that models the profitability of an e-commerce business. Data is extracted and loaded to a BigQuery data warehouse by Airbyte. 
The main goal of this repo is to show a production pipeline for a common analytics use case (improving profitability).

![Architecture](./architecture_profitability_usecase.png)

## Data Sources

- **Shopify**: E-commerce platform data (including  Shopify Orders, Shopify Balance Transactions, Shopify Transactions)
- **Facebook Ads**: Paid Marketing  (Facebook Advertising) data
- **Paypal**: Payment processing data (coming from Shopify)
- **FedEx and GLS**: Shipping data (coming from GSheets)
- **Manufacturing Costs**: Cost data from the manufacturing process (coming  from Gsheets)

## Models

Detail the dbt models included in the project and their relationships.

- `staging` models: Models raw data from source systems
- `intermediate` models: Models Revenue, and all costs separately. Join and aggregate data from staging models.
- `final` models: Calculates profitability metrics and other business KPIs and build fact_orders table including all costs at order level

## Data lineage 



## Feedback
We would love to hear your feedback or about your struggles with integrating data! 
Please reach out to us at hi@getdot.ai or DM us in the dbt or Airbyte Slack channels.

## Disclaimer

This repository can be installed to create a "one click pipeline." However, you will need to recreate the sources for cost data because they are custom to the business. The Facebook, Shopify, and Paypal transformations should be plug and play.
