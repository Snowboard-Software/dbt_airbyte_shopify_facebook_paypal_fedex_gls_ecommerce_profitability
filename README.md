# dbt E-commerce Profitability Pipeline

## Overview

This repository is a production dbt pipeline example that models the profitability of an e-commerce business. Data is extracted and loaded to a BigQuery data warehouse by Airbyte. 
The main goal of this repo is to show a production pipeline for a common analytics use case (improving profitability).

![Architecture](./architecture_profitability_usecase.png)

## Data Sources

- **Shopify**: E-commerce platform data
- **Facebook Ads**: Advertising data
- **Paypal**: Payment processing data
- **FedEx and GLS**: Shipping data
- **Manufacturing Costs**: Cost data from the manufacturing process

## Models

Detail the dbt models included in the project and their relationships.

- `staging` models: Intermediate transformations for each data source
- `intermediate` models: Join and aggregate data from staging models
- `final` models: Calculate profitability metrics and other business KPIs


## Feedback
We would love to hear your feedback or about your struggles with integrating data! 
Please reach out to us at hi@getdot.ai or DM us in the dbt or Airbyte Slack channels.

## Disclaimer

This repository can be installed to create a "one click pipeline." However, you will need to recreate the sources for cost data because they are custom to the business. The Facebook, Shopify, and Paypal transformations should be plug and play.
