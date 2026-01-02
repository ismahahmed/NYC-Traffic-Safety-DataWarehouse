# NYC-Traffic-Safety-DataWarehouse

This project designs and implements a PostgreSQL-based data warehouse to analyze traffic safety trends in New York City, with a particular focus on vehicle collisions and speed reducer requests. The goal is to transform raw, disparate datasets into a structured, queryable warehouse that supports meaningful analysis of traffic safety patterns across time and location.

The project demonstrates end-to-end data engineering principles, including data cleaning, dimensional modeling, and ETL pipeline development.

# Motivation

New York City publishes large volumes of open data related to traffic collisions and speed reducer requests. However, these datasets are often difficult to analyze directly due to inconsistent formats, data input errors and lack of relational structure.

This project addresses those challenges by:

- Cleaning and standardizing raw traffic safety data
- Designing a dimensional schema optimized for analytical queries
- Loading the transformed data into a centralized data warehouse

The resulting warehouse enables more efficient analysis of traffic incidents and safety-related requests across NYC boroughs

# Data Sources

The datasets used in this project are publicly available NYC Open Data sources, including:
- Motor Vehicle Collisions
- Speed Reducer Requests

Raw data files are stored in the raw_data/ directory prior to processing

## Dimentional Model 

![image](https://github.com/user-attachments/assets/8283e3a1-c985-4266-9603-76a314d75497)

# ETL Pipeline Overview

- Extract Raw CSV files are ingested from NYC Open Data
- Transform
  - Python scripts clean and normalize the data by:
  - Handling missing and inconsistent values
  - Standardizing location and vehicle attributes
  - Splitting entities into dimension-ready tables
- Load
  - Cleaned data is loaded into PostgreSQL tables

### Project Directory Structure

```
NYC-Traffic-Safety-Insights/
├── raw_data/                     # Raw input data files
├── postgres_files/               # SQL scripts for database schema
├── Clean_Data.py                 # Main ETL script
├── Create_Dim_*                  # Scripts for creating dimension tables
├── Write_To_Postgres.py          # Script for loading data into PostgreSQL
├── vehicle_mapping.csv           # Vehicle type mapping file
├── config.py                     # Configuration file for database connection
```

### **Project Workflow**
1. **Data Cleaning**: Removes duplicates, fills missing values, and standardizes columns.
2. **Dimensional Modeling**: Creates fact and dimension tables for analysis.
3. **ETL Process**: Extracts raw data, transforms it, and loads it into a PostgreSQL database.

![image](https://github.com/user-attachments/assets/f5028959-ec2f-4020-a730-628dd154e187)

---


### **Technologies Used**
- **Python**: Data processing and ETL pipeline.
- **Pandas**: Data manipulation and cleaning.
- **PostgreSQL**: Data warehouse for storing and querying transformed data.
- **SQLAlchemy**: Database connection and operations.


## Example Use Cases

- This data warehouse enables analysis such as:
- Identifying boroughs with the highest collision rates
- Analyzing trends in speed reducer requests over time
- Comparing collision characteristics by vehicle type
- Supporting downstream BI or dashboarding tools

## Future Improvements

- Automate the ETL pipeline with scheduling
- Expand the schema to include additional NYC safety datasets



