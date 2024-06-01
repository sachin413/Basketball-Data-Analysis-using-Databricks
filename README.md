# Basketball-Data-Analysis-using-Databricks
## Project Overview
This project demonstrates data engineering tasks using basketball data within a Databricks environment. The main goals are to process raw data, cleanse it, and analyze it using SQL to gain insights into player demographics across different teams.

## Technologies Used
- Databricks Workspace
- PySpark
- SQL

## Notebooks Overview
### 1. Data Processing Notebook
****Purpose:**** Handle initial data processing tasks including reading raw data from a CSV file, performing data cleansing, and writing the cleaned data to a Parquet file.

#### Steps:
#### 1. Reading Data:
Import basketball data from a CSV file.
#### 2. Data Cleansing:
- Rename columns for better readability and consistency.
- Replace blank or null values with appropriate placeholders or default values.
- Add new columns that may be required for further analysis.
#### 3. Writing Data:
- Write the processed data to a Parquet file format for efficient querying and storage.

### 2. SQL Analysis Notebook
****Purpose:**** Create a table from the Parquet file generated in the first notebook and perform SQL queries to extract specific insights.

#### Steps:
#### 1. Creating Table:
- Create a table in Databricks using the Parquet file as the data source.
#### 2. Solving Analytical Problems Using SQL:
- ****Query 1:**** View the most aged players from each team.
- ****Query 2:**** Display players from each team with a height greater than 6 feet.

## Setup and Execution
### Prerequisites
- Databricks environment setup
- Required libraries installed (e.g., pandas, pyspark, etc.)
- Access to the basketball CSV data file

### Steps to Run

#### 1. Data Processing Notebook:
- Upload the basketball CSV data file to Databricks.
- Execute the notebook to perform data cleansing and generate the Parquet file.
- Verify the output Parquet file in the Databricks file system.
#### 2. SQL Analysis Notebook:
- Execute the notebook to create a table from the Parquet file.
- Run the provided SQL queries to extract insights and verify the results.
