# PUC-Eng-Dados-MVP
## MVP de Engenharia de dados PUC
### Objetivo:
mortalidade, efetividade das medidas de prevenção adotadas e eficácia das vacinas contra seus efeitos colaterais. 

Para ser possível diminuí-lo em caso de surgimento de novos cenários como este em um curto período.

**Perguntas**

1. Em termos percentuais, qual foi a taxa de mortalidade da COVID-10 no mundo? 
2. Quantidate de pessoas hospoitalizadas X mortes.
2. Relação da taxa de mortalidade no mundo com as taxas brasileiras.
2. 
3. Houve discrepância significativa na taxa de mortalidade entre o Brasil e os principais países do mundo?
4. Qual foi o grau de efetividade das medidas de segurança propostas pela OMS, com relação à pandemia?
5. Qual foi a efetividade das vacinas com relação à taxa de mortalidade e surgimento de novos casos?
6. Ritmo de vacinação por país.



## Proximos passos




file1.cvs with: Entity,Day, Daily new confirmed cases of COVID-19 per million people as a header
file2.csv: Entity,Day,Daily new confirmed deaths due to COVID-19 per million people (rolling 7-day average, right-aligned)
file3.csv:Entity,Day,COVID-19 doses (daily, 7-day average, per million people)
file4.csv:Entity,Code,Day,Daily hospital occupancy
What would be the best data modeling strategy: Snowflake, Star or flat?


## Analysis of the Data Files

Let's analyze the four COVID-19 data files:

1. **file1.csv**: Daily new confirmed COVID-19 cases per million people
2. **file2.csv**: Daily new confirmed COVID-19 deaths per million people (7-day average)
3. **file3.csv**: Daily COVID-19 vaccination doses administered (7-day average, per million)
4. **file4.csv**: Daily hospital occupancy

Common dimensions across these files:
- Entity (likely countries/regions)
- Day (time dimension)
- Code (appears only in file4, likely country/region code)

## Recommended Data Modeling Strategy: Star Schema

**A Star schema would be the best approach** for this COVID-19 data for the following reasons:

### Why Star Schema Is Best:

1. **Appropriate Level of Normalization**:
   - The data has clear facts (cases, deaths, vaccinations, hospital occupancy) and dimensions (location, time)
   - Star schema provides a good balance between performance and complexity

2. **Query Performance**:
   - COVID-19 data is likely to be queried frequently for reporting and analysis
   - Star schema minimizes joins, improving query performance
   - Aggregations across time and geography will be common

3. **Intuitive Representation**:
   - The relationship between dimensions and facts is straightforward
   - Analysts can easily understand the model for ad-hoc analysis

4. **Flexibility**:
   - New metrics can be added as new fact tables that share the same dimensions

### Why Not Snowflake:

1. **No Complex Hierarchies**: The dimensions (Entity, Day) don't appear to have multiple levels requiring normalization
2. **Performance Overhead**: Snowflake would introduce additional joins without significant benefits
3. **Unnecessary Complexity**: There's no need for the additional normalization that snowflake provides

### Why Not Flat Model:

1. **Data Redundancy**: Would duplicate entity information across all fact records
2. **Limited Scalability**: As more COVID metrics are added, a flat model becomes unwieldy
3. **Difficult Maintenance**: Changes to dimension attributes would require updates across all records

## Proposed Star Schema Model:

```
                ┌───────────────┐
                │  dim_entity   │
                ├───────────────┤
                │ entity_id (PK)│
                │ entity_name   │
                │ entity_code   │
                └───────┬───────┘
                        │
                        │
┌───────────────┐       │       ┌─────────────────────────┐
│    dim_date   │       │       │ fact_covid_cases        │
├───────────────┤       │       ├─────────────────────────┤
│ date_id (PK)  │       │       │ entity_id (FK)          │
│ full_date     │       ├──────►│ date_id (FK)            │
│ day           │       │       │ new_cases_per_million   │
│ month         │       │       └─────────────────────────┘
│ year          │       │
│ day_of_week   │       │       ┌─────────────────────────┐
└───────┬───────┘       │       │ fact_covid_deaths       │
        │               │       ├─────────────────────────┤
        │               ├──────►│ entity_id (FK)          │
        │               │       │ date_id (FK)            │
        │               │       │ new_deaths_per_million  │
        │               │       └─────────────────────────┘
        │               │
        │               │       ┌─────────────────────────┐
        │               │       │ fact_covid_vaccinations │
        │               │       ├─────────────────────────┤
        └───────────────┼──────►│ entity_id (FK)          │
                        │       │ date_id (FK)            │
                        │       │ doses_per_million       │
                        │       └─────────────────────────┘
                        │
                        │       ┌─────────────────────────┐
                        │       │ fact_hospital_occupancy │
                        │       ├─────────────────────────┤
                        └──────►│ entity_id (FK)          │
                                │ date_id (FK)            │
                                │ hospital_occupancy      │
                                └─────────────────────────┘
```

## Processo de ETL:


Step 1: Extract Data in Databricks
1. Set Up Databricks Environment
# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
2. Upload CSV Files to Databricks
In the Databricks workspace, click on "Data" in the left sidebar
Click "Create Table" or "Create New Table"
Select "Upload Files" and upload the four CSV files:
casos_confirmados_por_milhao_de_pessoas.csv
mortes_por_milhao_de_pessoas.csv
doses_de_vacinas_por_milhao_de_pessoas.csv
hospitalizados_numero_total.csv
Alternatively, you can use DBFS (Databricks File System) to store these files:
# If your files are already in cloud storage (S3, Azure Blob, etc.)
# dbutils.fs.cp("s3://your-bucket/path/to/file.csv", "dbfs:/FileStore/covid_data/")

# To check if files are correctly uploaded
display(dbutils.fs.ls("dbfs:/FileStore/covid_data/"))
3. Read the CSV Files (Separating by Future Fact Table)
# Specify the file paths in DBFS
cases_file_path = "/FileStore/covid_data/casos_confirmados_por_milhao_de_pessoas.csv"
deaths_file_path = "/FileStore/covid_data/mortes_por_milhao_de_pessoas.csv"
vaccines_file_path = "/FileStore/covid_data/doses_de_vacinas_por_milhao_de_pessoas.csv"
hospital_file_path = "/FileStore/covid_data/hospitalizados_numero_total.csv"

# Read confirmed cases data (for fact_covid_cases)
df_cases = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(cases_file_path)

# Read deaths data (for fact_covid_deaths)
df_deaths = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(deaths_file_path)

# Read vaccination data (for fact_vaccinations)
df_vaccines = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(vaccines_file_path)

# Read hospitalization data (for fact_hospitalizations)
df_hospital = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(hospital_file_path)
4. Inspect the Data with Separate Fact Tables in Mind
# Display schema and sample data for each dataset (future fact table)
print("Confirmed Cases Schema (future fact_covid_cases):")
df_cases.printSchema()
display(df_cases.limit(5))

print("Deaths Schema (future fact_covid_deaths):")
df_deaths.printSchema()
display(df_deaths.limit(5))

print("Vaccines Schema (future fact_vaccinations):")
df_vaccines.printSchema()
display(df_vaccines.limit(5))

print("Hospitalizations Schema (future fact_hospitalizations):")
df_hospital.printSchema()
display(df_hospital.limit(5))

# Get record counts for each dataset
print("Records per dataset (future fact table):")
print(f"Confirmed Cases: {df_cases.count()}")
print(f"Deaths: {df_deaths.count()}")
print(f"Vaccines: {df_vaccines.count()}")
print(f"Hospitalizations: {df_hospital.count()}")
5. Extract Dimension Data
# Extract unique entity data for dim_location
df_locations_cases = df_cases.select("Entity").distinct()
df_locations_deaths = df_deaths.select("Entity").distinct()
df_locations_vaccines = df_vaccines.select("Entity").distinct()
df_locations_hospital = df_hospital.select("Entity", "Code").distinct()

# Combine all unique locations into one comprehensive dimension table source
df_locations_all = df_locations_cases.unionAll(df_locations_deaths) \
                                    .unionAll(df_locations_vaccines) \
                                    .distinct()

# Join with hospital data to get codes where available
df_locations_dim = df_locations_all.join(df_locations_hospital, on="Entity", how="left")

# Extract date data for dim_date
df_dates_cases = df_cases.select("Day").distinct()
df_dates_deaths = df_deaths.select("Day").distinct()
df_dates_vaccines = df_vaccines.select("Day").distinct()
df_dates_hospital = df_hospital.select("Day").distinct()

# Combine all unique dates for the date dimension
df_dates_all = df_dates_cases.unionAll(df_dates_deaths) \
                            .unionAll(df_dates_vaccines) \
                            .unionAll(df_dates_hospital) \
                            .distinct()
6. Prepare Date Fields for Filtering (Separate for Each Fact)
# Convert Day column to date type for filtering
df_cases = df_cases.withColumn("Day", to_date(col("Day")))
df_deaths = df_deaths.withColumn("Day", to_date(col("Day")))
df_vaccines = df_vaccines.withColumn("Day", to_date(col("Day")))
df_hospital = df_hospital.withColumn("Day", to_date(col("Day")))

# Also convert date for dimension table source
df_dates_all = df_dates_all.withColumn("Day", to_date(col("Day")))

# Check if the conversion was successful
print("Sample dates after conversion (cases fact source):")
display(df_cases.select("Entity", "Day").limit(5))
7. Rename Columns for Each Future Fact Table
# Rename columns to match future fact table structure

# For fact_covid_cases
df_cases = df_cases.withColumnRenamed(
    "Daily new confirmed cases of COVID-19 per million people (rolling 7-day average, right-aligned)", 
    "cases_per_million"
)

# For fact_covid_deaths
df_deaths = df_deaths.withColumnRenamed(
    "Daily new confirmed deaths due to COVID-19 per million people (rolling 7-day average, right-aligned)", 
    "deaths_per_million"
)

# For fact_vaccinations
df_vaccines = df_vaccines.withColumnRenamed(
    "COVID-19 doses (daily, 7-day average, per million people)", 
    "doses_per_million"
)

# For fact_hospitalizations
df_hospital = df_hospital.withColumnRenamed(
    "Daily hospital occupancy", 
    "hospital_occupancy"
)
8. Register Temporary Views (Separately for Each Fact Source)
# Create temporary views to enable SQL queries
df_cases.createOrReplaceTempView("covid_cases_source")
df_deaths.createOrReplaceTempView("covid_deaths_source")
df_vaccines.createOrReplaceTempView("covid_vaccines_source")
df_hospital.createOrReplaceTempView("covid_hospital_source")

# Create temporary views for dimension sources
df_locations_dim.createOrReplaceTempView("dim_location_source")
df_dates_all.createOrReplaceTempView("dim_date_source")

# Verify with a simple SQL query
display(spark.sql("SELECT Entity, Day, cases_per_million FROM covid_cases_source LIMIT 10"))
9. Save Extracted Data as Delta Tables (Separate for Each Fact)
# Create a database for raw data if it doesn't exist
spark.sql("CREATE DATABASE IF NOT EXISTS covid_data_raw")

# Save
1. **Create dimension tables**:
   - `dim_entity`: Unique entities with their codes
   - `dim_date`: Calendar dimension with date hierarchies

2. **Create fact tables**:
   - `fact_covid_cases`: Daily cases per million
   - `fact_covid_deaths`: Daily deaths per million
   - `fact_covid_vaccinations`: Daily vaccination doses
   - `fact_hospital_occupancy`: Hospital occupancy data

3. **Load dimension tables first**, then fact tables with appropriate key relationships

## Benefits of This Approach:

1. **Consistency**: Unified dimensions across all fact tables
2. **Performance**: Efficient querying through minimized joins
3. **Extensibility**: Easy to add new COVID metrics as additional fact tables
4. **Analysis**: Supports both detailed and aggregated analysis
5. **Storage efficiency**: Dimension data stored only once

This star schema balances simplicity with the analytical needs of COVID-19 data tracking and reporting, making it the optimal choice for this dataset.
User: provide me the steps to create this model in DataBricks,
Model: # Creating a Star Schema for COVID-19 Data in Databricks

Below are the step-by-step instructions to implement the recommended star schema for your COVID-19 datasets in Databricks.



## Step 2: Transform
 # Detailed Steps for the Transform Phase in Databricks (Using Separate Fact Tables)

After completing the extraction phase, the transformation phase involves preparing the data for the star schema model. Here's a comprehensive approach to the transformation step using Databricks:

## Step 2: Transform Data in Databricks

### 1. Filter Data to Match Date Range Requirements

```python
# Define date range constants (March 2020 to December 2021)
from datetime import datetime
start_date = datetime(2020, 3, 1)
end_date = datetime(2021, 12, 31)

# Filter each dataset to include only data within the specified date range
df_cases_filtered = df_cases.filter((col("Day") >= start_date) & (col("Day") <= end_date))
df_deaths_filtered = df_deaths.filter((col("Day") >= start_date) & (col("Day") <= end_date))
df_vaccines_filtered = df_vaccines.filter((col("Day") >= start_date) & (col("Day") <= end_date))
df_hospital_filtered = df_hospital.filter((col("Day") >= start_date) & (col("Day") <= end_date))

# Check record counts after filtering to ensure data exists
print("Records after date filtering:")
print(f"Confirmed Cases: {df_cases_filtered.count()}")
print(f"Deaths: {df_deaths_filtered.count()}")
print(f"Vaccines: {df_vaccines_filtered.count()}")
print(f"Hospitalizations: {df_hospital_filtered.count()}")
```

### 2. Create the Date Dimension Table

```python
from pyspark.sql.functions import year, month, dayofmonth, quarter, dayofweek, weekofyear, date_format

# Create the date dimension from all unique dates in the filtered datasets
df_dates_all_filtered = df_dates_all.filter((col("Day") >= start_date) & (col("Day") <= end_date))

# Generate all date attributes
df_dim_date = df_dates_all_filtered.select(
    col("Day").alias("full_date"),
    dayofmonth("Day").alias("day"),
    month("Day").alias("month"),
    year("Day").alias("year"),
    quarter("Day").alias("quarter"),
    dayofweek("Day").alias("day_of_week"),
    weekofyear("Day").alias("week_of_year"),
    date_format("Day", "EEEE").alias("day_name"),
    date_format("Day", "MMMM").alias("month_name")
)

# Add a date_id column as a surrogate key
from pyspark.sql.functions import monotonically_increasing_id, row_number
from pyspark.sql.window import Window

window_spec = Window.orderBy("full_date")
df_dim_date = df_dim_date.withColumn("date_id", row_number().over(window_spec))

# Display the date dimension
print("Date dimension sample:")
display(df_dim_date.limit(10))
```

### 3. Create the Location Dimension Table

```python
# Combine location information from all datasets
# Note: Hospital data has the Code column which isn't present in other datasets
df_locations_combined = df_cases_filtered.select("Entity").distinct() \
    .join(df_hospital_filtered.select("Entity", "Code").distinct(), on="Entity", how="left")

# Add a location_id as a surrogate key
window_spec = Window.orderBy("Entity")
df_dim_location = df_locations_combined.withColumn("location_id", row_number().over(window_spec))

# Add a continent column based on country mapping (simplified example)
from pyspark.sql.functions import when, lit

# This is a simplified example - in a real scenario, you'd have a more comprehensive mapping
continent_mapping = [
    (["United States", "Canada", "Mexico"], "North America"),
    (["Brazil", "Argentina", "Colombia"], "South America"),
    (["United Kingdom", "France", "Germany", "Italy", "Spain"], "Europe"),
    (["China", "Japan", "India", "South Korea"], "Asia"),
    (["Australia", "New Zealand"], "Oceania"),
    (["South Africa", "Nigeria", "Egypt"], "Africa")
]

# Build the continent mapping expression
continent_expr = None
for countries, continent in continent_mapping:
    for country in countries:
        country_expr = (col("Entity") == country)
        if continent_expr is None:
            continent_expr = when(country_expr, lit(continent))
        else:
            continent_expr = continent_expr.when(country_expr, lit(continent))

# Apply the continent mapping with a default value
df_dim_location = df_dim_location.withColumn(
    "continent", 
    continent_expr.otherwise(lit("Unknown"))
)

# Handle null values in Code column
df_dim_location = df_dim_location.withColumn(
    "location_code",
    when(col("Code").isNull(), lit("UNK")).otherwise(col("Code"))
)

# Reorder and rename columns
df_dim_location = df_dim_location.select(
    "location_id", 
    col("Entity").alias("location_name"), 
    "location_code",
    "continent"
)

# Display the location dimension
print("Location dimension sample:")
display(df_dim_location.limit(10))
```

### 4. Transform Fact Tables (Cases, Deaths, Vaccines, Hospitalizations)

```python
# Function to transform each fact table by joining with dimension tables
def create_fact_table(source_df, metric_name, metric_column):
    # Join with dimensions to get the surrogate keys
    fact_df = source_df.join(
        df_dim_location,
        source_df["Entity"] == df_dim_location["location_name"],
        "inner"
    ).join(
        df_dim_date,
        source_df["Day"] == df_dim_date["full_date"],
        "inner"
    )
    
    # Select only the needed columns
    fact_df = fact_df.select(
        "date_id",
        "location_id",
        col(metric_column).alias(metric_column)
    )
    
    # Handle missing values in the metric column
    fact_df = fact_df.withColumn(
        metric_column,
        when(col(metric_column).isNull(), 0).otherwise(col(metric_column))
    )
    
    return fact_df

# Create each fact table
fact_covid_cases = create_fact_table(df_cases_filtered, "Cases", "cases_per_million")
fact_covid_deaths = create_fact_table(df_deaths_filtered, "Deaths", "deaths_per_million")
fact_vaccinations = create_fact_table(df_vaccines_filtered, "Vaccines", "doses_per_million")

# Hospital fact table needs special handling due to the Code column
fact_hospitalizations = df_hospital_filtered.join(
    df_dim_location,
    (df_hospital_filtered["Entity"] == df_dim_location["location_name"]) &
    (df_hospital_filtered["Code"] == df_dim_location["location_code"]),
    "inner"
).join(
    df_dim_date,
    df_hospital_filtered["Day"] == df_dim_date["full_date"],
    "inner"
)

fact_hospitalizations = fact_hospitalizations.select(
    "date_id",
    "location_id",
    col("hospital_occupancy").alias("hospital_occupancy")
)

fact_hospitalizations = fact_hospitalizations.withColumn(
    "hospital_occupancy",
    when(col("hospital_occupancy").isNull(), 0).otherwise(col("hospital_occupancy"))
)

# Display samples of each fact table
print("COVID Cases Fact Table Sample:")
display(fact_covid_cases.limit(5))

print("COVID Deaths Fact Table Sample:")
display(fact_covid_deaths.limit(5))

print("Vaccinations Fact Table Sample:")
display(fact_vaccinations.limit(5))

print("Hospitalizations Fact Table Sample:")
display(fact_hospitalizations.limit(5))
```

### 5. Data Quality Checks

```python
# Check for missing dimension keys in fact tables
def check_missing_keys(



# LOAD
# Detailed Steps for the Load Phase in Databricks (Using Separate Fact Tables)

After completing the extraction and transformation phases, the loading phase involves saving the transformed data into the target star schema. Here's a comprehensive approach to the loading step using Databricks:

## Step 3: Load Data in Databricks

### 1. Create Database and Schema

```python
# Create a new database for the star schema model
spark.sql("CREATE DATABASE IF NOT EXISTS covid_star_schema")
spark.sql("USE covid_star_schema")
```

### 2. Create Optimized Tables with Delta Lake

```python
# Set up configurations for optimized Delta tables
spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
```

### 3. Load Dimension Tables

```python
# Load the date dimension table
df_dim_date.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("comment", "Date dimension for COVID-19 data") \
    .saveAsTable("covid_star_schema.dim_date")

# Create a Z-order index on the date dimension for faster queries
spark.sql("""
    OPTIMIZE covid_star_schema.dim_date
    ZORDER BY (date_id, full_date)
""")

# Load the location dimension table
df_dim_location.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("comment", "Location dimension for COVID-19 data") \
    .saveAsTable("covid_star_schema.dim_location")

# Create a Z-order index on the location dimension
spark.sql("""
    OPTIMIZE covid_star_schema.dim_location
    ZORDER BY (location_id, location_name)
""")

# Verify dimension tables were loaded correctly
print("Date dimension record count:", spark.table("covid_star_schema.dim_date").count())
print("Location dimension record count:", spark.table("covid_star_schema.dim_location").count())
```

### 4. Load Fact Tables

```python
# Load the fact tables

# 1. COVID Cases Fact Table
fact_covid_cases.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("comment", "Fact table for COVID-19 cases per million") \
    .saveAsTable("covid_star_schema.fact_covid_cases")

# 2. COVID Deaths Fact Table
fact_covid_deaths.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("comment", "Fact table for COVID-19 deaths per million") \
    .saveAsTable("covid_star_schema.fact_covid_deaths")

# 3. Vaccinations Fact Table
fact_vaccinations.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("comment", "Fact table for COVID-19 vaccination doses per million") \
    .saveAsTable("covid_star_schema.fact_vaccinations")

# 4. Hospitalizations Fact Table
fact_hospitalizations.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("comment", "Fact table for COVID-19 hospital occupancy") \
    .saveAsTable("covid_star_schema.fact_hospitalizations")

# Verify fact tables were loaded correctly
print("COVID cases fact table record count:", spark.table("covid_star_schema.fact_covid_cases").count())
print("COVID deaths fact table record count:", spark.table("covid_star_schema.fact_covid_deaths").count())
print("Vaccinations fact table record count:", spark.table("covid_star_schema.fact_vaccinations").count())
print("Hospitalizations fact table record count:", spark.table("covid_star_schema.fact_hospitalizations").count())
```

### 5. Create Partitions and Optimize Fact Tables

```python
# Optimize each fact table with appropriate partitioning and Z-ordering

# 1. Optimize COVID Cases Fact Table
spark.sql("""
    OPTIMIZE covid_star_schema.fact_covid_cases
    ZORDER BY (date_id, location_id)
""")

# 2. Optimize COVID Deaths Fact Table
spark.sql("""
    OPTIMIZE covid_star_schema.fact_covid_deaths
    ZORDER BY (date_id, location_id)
""")

# 3. Optimize Vaccinations Fact Table
spark.sql("""
    OPTIMIZE covid_star_schema.fact_vaccinations
    ZORDER BY (date_id, location_id)
""")

# 4. Optimize Hospitalizations Fact Table
spark.sql("""
    OPTIMIZE covid_star_schema.fact_hospitalizations
    ZORDER BY (date_id, location_id)
""")
```

### 6. Define Table Properties and Constraints

```python
# Add constraints to ensure data integrity

# Add constraints to dim_date
spark.sql("""
    ALTER TABLE covid_star_schema.dim_date
    ADD CONSTRAINT pk_dim_date PRIMARY KEY (date_id)
""")

# Add constraints to dim_location
spark.sql("""
    ALTER TABLE covid_star_schema.dim_location
    ADD CONSTRAINT pk_dim_location PRIMARY KEY (location_id)
""")

# Add foreign key constraints to fact tables
# Note: In Delta Lake, foreign key constraints are not enforced but serve as documentation

# Fact COVID Cases
spark.sql("""
    ALTER TABLE covid_star_schema.fact_covid_cases
    ADD CONSTRAINT fk_fact_covid_cases_date FOREIGN KEY (date_id)
    REFERENCES covid_star_schema.dim_date(date_id)
""")

spark.sql("""
    ALTER TABLE covid_star_schema.fact_covid_cases
    ADD CONSTRAINT fk_fact_covid_cases_location FOREIGN KEY (location_id)
    REFERENCES covid_star_schema.dim_location(location_id)
""")

# Apply similar constraints to other fact tables
# (For brevity, not showing all similar constraint definitions)
```

### 7. Create Views for Common Analytical Queries

```python
# Create views that simplify common analytical queries

# 1. COVID-19 Cases View
spark.sql("""
    CREATE OR REPLACE VIEW covid_star_schema.vw_covid_cases AS
    SELECT 
        d.full_date,
        d.year,
        d.month,
        d.month_name,
        l.location_name,
        l.continent,
        c.cases_per_million
    FROM covid_star_schema.fact_covid_cases c
    JOIN covid_star_schema.dim_date d ON c.date_id = d.date_id
    JOIN covid_star_schema.dim_location l ON c.location_id = l.location_id
""")

# 2. COVID-19 Deaths View
spark.sql("""
    CREATE OR REPLACE VIEW covid_star_schema.vw_covid_deaths AS
    SELECT 
        d.full_date,
        d.year,
        d.month,
        d.month_name,
        l.location_name,
        l.continent,
        c.deaths_per_million
    FROM covid_star_schema.fact_covid_deaths c
    JOIN covid_star_schema.dim_date d ON c.date_id = d.date_id
    JOIN covid_star_schema.dim_location l ON c.location_id = l.location_id
""")

# 3. COVID-19 Comprehensive View (combining all metrics)
spark.sql("""
    CREATE OR REPLACE VIEW covid_star_schema.vw_covid_comprehensive AS
    SELECT 
        d.full_date,
        d.year,
        d.month,
        d.month_name,
        l.location_name,
        l.continent,
        c.cases_per_million
