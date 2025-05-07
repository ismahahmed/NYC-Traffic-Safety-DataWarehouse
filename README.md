# NYC-Traffic-Safety-DataWarehouse

A data warehouse project for analyzing NYC traffic safety and speed reducer requests. Includes data cleaning, dimensional modeling, and ETL processes to load crash and request data into PostgreSQL for insights on trends, contributing factors, and infrastructure improvements.


## Directory Structure 

NYC-Traffic-Safety-Insights/
├── raw_data/                     # Raw input data files
├── postgres_files/               # SQL scripts for database schema
├── Clean_Data.py                 # Main ETL script
├── Create_Dim_*                  # Scripts for creating dimension tables
├── Write_To_Postgres.py          # Script for loading data into PostgreSQL
├── vehicle_mapping.csv           # Vehicle type mapping file
├── config.py                     # Configuration file for database connection



### **Project Workflow**
1. **Data Cleaning**: Removes duplicates, fills missing values, and standardizes columns.
2. **Dimensional Modeling**: Creates fact and dimension tables for analysis.
3. **ETL Process**: Extracts raw data, transforms it, and loads it into a PostgreSQL database.

---

### **Technologies Used**
- **Python**: Data processing and ETL pipeline.
- **Pandas**: Data manipulation and cleaning.
- **PostgreSQL**: Data warehouse for storing and querying transformed data.
- **SQLAlchemy**: Database connection and operations.



