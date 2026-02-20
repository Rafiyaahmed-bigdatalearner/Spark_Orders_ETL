# Retail Analysis Pipeline

This project demonstrates a PySpark-based data pipeline using **medallion architecture (Bronze → Silver → Gold)** and **Slowly Changing Dimension Type 2 (SCD2)**.

## Project Structure
DataManipulation.py # PySpark functions for filtering, joining, aggregation, SCD2
logger.py # Log4j wrapper for Spark logging
application_main.py # Entry point for pipeline execution
requirements.txt # Python dependencies
README.md # Project documentation
