# Spark Orders ETL Project

## Overview
This project demonstrates a **modular PySpark ETL pipeline** for processing orders and customer data.  
The pipeline performs the following tasks:

1. Reads orders and customers data from CSV files
2. Filters orders with status `CLOSED`
3. Joins orders with customer data
4. Aggregates the number of closed orders by state
5. Logs progress using Log4j