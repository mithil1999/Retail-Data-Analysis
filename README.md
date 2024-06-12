# Retail-Data-Analysis
Project on Real-time Streaming data consumed from Apache Kafka, computed the KPIs using Apache Spark and stored them in a Data Lake so that data can be furthur consumed for BI analytics.

![image](https://github.com/mithil1999/Retail-Data-Analysis/assets/90143241/34abee76-e04f-42f0-977d-b03f41b5a07f)

Key tasks performed:
1. Reading the sales data from the Kafka server
2. Preprocessing the data to calculate additional derived columns such as total_cost etc
3. Calculating the time-based KPIs and time and country-based KPIs
4. Storing the KPIs (both time-based and time- and country-based) for a 10-minute interval into separate JSON files for further analysis
