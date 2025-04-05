# PUC-Eng-Dados-MVP
## MVP de Engenharia de dados PUC
### Objetivo:
De janeiro de 2020 até dezembro de 2021 causou uma revolução mundial devido a pandemia de COVID-19, em pouco tempo, vimos mudanças drásticas na mudança de hábito, medo, morte e instabilidade econômica. O que antes era considerado normal, em um piscar de olhos se tornou perigoso, vivíamos a era do "novo normal". 
Este tema tão intrigante, devido a estes e outros fatores, dispertou a minha curiosidade por isso o elegi para, pelo menos tentar responder com dados, algumas perguntas que pairaram na minha mente neste período. 


Além de tentar me ajudar com estas dúvidas o trabalho tem como obejtivo enteder como ficou a distribuição dos casos reportados (Brasil e mundo), distribuição da taxa mortalidade (Brasil e mundo), eficácia das vacinas contra seus efeitos colaterais. 

Há também a pretenção utópica de quem sabe, ajudar a entender melhor o cenário vivído recentemente,
**Perguntas**

1. Em termos percentuais, qual foi a taxa de mortalidade da COVID-10 no mundo?
2. Esse número flutou ao ser comparado com os índices brasileiros?
3. O número de casos teve variação entre os países?
4. Variação entre o Brasil e o mundo sobre a quantidate de pessoas hospoitalizadas X mortes.
5. Relação da taxa de mortalidade no mundo com as taxas brasileiras.
6. Qual foi a efetividade das vacinas com relação à taxa de mortalidade e surgimento de novos casos? 
7. Ritmo de vacinação por país.
8. Em relação à periodos anteriores à COVID-19, qual o percentual de mortalidade acrescido?

### Coleta:
Os dados utilizados para a análise foram coletados do site https://ourworldindata.org/, que já na pendemia da COVID-19 os disponibilizava de forma pública e gratuita. O site é fruto de um projeto da parceria entre a Universidsde de Oxford, responsável pelas pesquisas  e uma ONG chamada Global Change Data Lab que é responsável pela manutenção do site.
O período utilizado para realizar o MVP foi de março de 2020, período em que foi decretado o "Lock Down" até a dezembro de 2021 quando a maioria das pessoas no mundo já haviam recebido as dias doses das vacinas e a pandemida estava controlada.
Os arquivos baixados foram os seguintes:
- daily-new-confirmed-covid-19-cases-per-million-people
- daily-new-confirmed-covid-19-deaths-per-million-people
- number-of-covid-19-patients-in-hospital-per-million-people
- daily-covid-19-vaccine-doses-administered-per-million-people

## Modelagem

## Analysis of the Data Files
Baseado na características dos dados coletados, o esquema **estrela** escolhido para a modelagem, pelas seguintes razões:


1. **Nível de normanlização apropriado**:

2. **Performance das Queries**:
  
3. **Intuitive Representation**:

4. **Flexibility**:

**Modelo Proposto:**
COVID-19 Esquuema
                          ┌───────────────┐
                          │ Location      | 
                          │ Deimansion    | 
                          ├───────────────┤
                          │ LocationID PK │
                          │ EntityName    │
                          │   │
                          └──────┬──────-─┘
                                 │
                                 │ 
           ┌───────────────────┬─┴──┬────────────────────┐
           │                   │    │                    │
           │                   │    │                    │
┌──────────▼─────────┐         │    │          ┌─────────▼──────────┐
│    CovidFacts      │         │    │          │    TimeDimension   │
├────────────────────┤         │    │          ├────────────────────┤
│ MetricID       PK  │◄────────┘    └─────────►│ DateID         PK  │
│ LocationID     FK  │                         │ Day                │
│ DateID         FK  │                         │ Year               │
│ NewCasesPerMillion │                         │ Month              │
│ NewDeathsPerMillion│                         └────────────────────┘
│ VaccineDosesPerMil │
│ HospitalOccupancyPM│
└────────────────────┘

1. **No Complex Hierarchies**: The dimensions (Entity, Day) don't appear to have multiple levels requiring normalization
2. **Performance Overhead**: Snowflake would introduce additional joins without significant benefits
3. **Unnecessary Complexity**: There's no need for the additional normalization that snowflake provides



# ETL Process for COVID-19 Data Model

## Phase 1: Extraction

1. **Source Identification**:
   - `casos_confirmados_por_milhao_de_pessoas.csv`: Daily COVID-19 cases per million
   - `mortes_por_milhao_de_pessoas.csv`: Daily COVID-19 deaths per million
   - `doses_de_vacinas_por_milhao_de_pessoas.csv`: Daily vaccine doses per million
   - `hospitalizados_numero_total.csv`: Daily hospital occupancy per million

2. **Data Extraction**:
   - Read each CSV file using appropriate data processing tools (Python/Pandas, SQL SSIS, etc.)
   - Validate file structure and encoding
   - Extract header information to confirm data columns
   - Create raw data staging tables for each source file

User: based on the files:
casos_confirmados_por_milhao_de_pessoas.csv that has the heading as "Entity,Day,"Daily new confirmed cases of COVID-19 per million people (rolling 7-day average, right-aligned)"
mortes_por_milhao_de_pessoas.csv, heading as Entity,Day,"Daily new confirmed deaths due to COVID-19 per million people (rolling 7-day average, right-aligned)"
doses_de_vacinas_por_milhao_de_pessoas.csv, heading as "Entity,Day,"COVID-19 doses (daily, 7-day average, per million people)"
hospitalizados_numero_total.csv heading "Entity,Day,Daily hospital occupancy per million"
What wouil be the best modeling schema?
Model: # Modeling Schema Recommendation

Based on the four CSV files provided, I can recommend a modeling schema that would effectively organize this COVID-19 related data.

## Entity-Relationship Model

### Core Entities:

1. **Location**
   - LocationID (PK)
   - EntityName (from the "Entity" column in all files)
   - Population (derived or added, useful for per-million calculations)
   - Region/Continent (optional, for geographical grouping)

2. **TimeData**
   - DateID (PK)
   - Day (from the "Day" column in all files)
   - Year (derived)
   - Month (derived)
   - Week (derived)

3. **CovidMetrics** (Fact table)
   - MetricID (PK)
   - LocationID (FK)
   - DateID (FK)
   - NewCasesPerMillion (from casos_confirmados_por_milhao_de_pessoas.csv)
   - NewDeathsPerMillion (from mortes_por_milhao_de_pessoas.csv)
   - VaccineDosesPerMillion (from doses_de_vacinas_por_milhao_de_pessoas.csv)
   - HospitalOccupancyPerMillion (from hospitalizados_numero_total.csv)

## Schema Benefits:

1. **Dimensional Model**: This star schema allows for efficient querying across multiple metrics.
2. **Reduced Redundancy**: Entity information is stored once in the Location table.
3. **Time Series Analysis**: The TimeData dimension enables easy time-based querying.
4. **Data Integration**: All metrics are consolidated in one fact table with consistent keys.
5. **Scalability**: Easy to add new metrics without changing the schema structure.

## Implementation Notes:

- Each record in the fact table represents a unique combination of location, date, and the associated COVID-19 metrics.
- The schema handles the "rolling 7-day average" and "per million people" aspects implicit in the data.
- Missing values can be handled with NULL entries in the fact table.
- Consider adding data quality constraints for the metrics (e.g., non-negative values).

This schema would allow for efficient querying across locations and time periods, enabling analysis of relationships between cases, deaths, vaccinations, and hospitalizations.
User: remove Region/Continent  from location and wee from TimeData
Model: # Revised Modeling Schema Recommendation

Based on your feedback, I've revised the schema by removing Region/Continent from Location and Week from TimeData.

## Entity-Relationship Model

### Core Entities:

1. **Location**
   - LocationID (PK)
   - EntityName (from the "Entity" column in all files)
   - Population (derived or added, useful for per-million calculations)

2. **TimeData**
   - DateID (PK)
   - Day (from the "Day" column in all files)
   - Year (derived)
   - Month (derived)

3. **CovidMetrics** (Fact table)
   - MetricID (PK)
   - LocationID (FK)
   - DateID (FK)
   - NewCasesPerMillion (from casos_confirmados_por_milhao_de_pessoas.csv)
   - NewDeathsPerMillion (from mortes_por_milhao_de_pessoas.csv)
   - VaccineDosesPerMillion (from doses_de_vacinas_por_milhao_de_pessoas.csv)
   - HospitalOccupancyPerMillion (from hospitalizados_numero_total.csv)

## Schema Benefits:

1. **Streamlined Design**: The revised schema is more focused with only essential attributes.
2. **Dimensional Model**: Maintains the star schema for efficient querying.
3. **Reduced Redundancy**: Entity information is stored once in the Location table.
4. **Time Series Analysis**: The simplified TimeData dimension still enables effective date-based querying.
5. **Data Integration**: All metrics remain consolidated in one fact table with consistent keys.

## Implementation Notes:

- Each record in the fact table represents a unique combination of location, date, and the associated COVID-19 metrics.
- The schema handles the "rolling 7-day average" and "per million people" aspects implicit in the data.
- Missing values can be handled with NULL entries in the fact table.
- Consider adding data quality constraints for the metrics (e.g., non-negative values).

This revised schema maintains the analytical capabilities while focusing only on the necessary attributes you've specified.
User: Drar me the schema
Model: # COVID-19 Data Model Schema

┌─────────────────────┐           ┌───────────────────────────┐
│   Date_Dimension    │           │       COVID_Facts         │
├─────────────────────┤           ├───────────────────────────┤
│ date_key (PK)       │◄──────────┤ date_key (FK)             │
│ day (integer)       │           │ location_key (FK)         │──────────┐
│ month (integer)     │           │ deaths_per_million        │          │
│ year (integer)      │           │ cases_per_million         │          │
└─────────────────────┘           │ hospitalizations_per_mil  │          │
                                  │ vaccine_doses_per_million │          │
                                  └───────────────────────────┘          │
                                                                         │
                                                                         │
                                  ┌───────────────────────────┐          │
                                  │    Location_Dimension     │          │
                                  ├───────────────────────────┤          │
                                  │ location_key (PK)         │◄─────────┘
                                  │ entity_name (varchar)     │
                                  └───────────────────────────┘


```
Date_Dimension
-----------------
date_key (PK)
full_date (date)
day (integer)
month (integer)
year (integer)


Location_Dimension
-----------------
location_key (PK)
entity_name (varchar)


Metrics_Dimension
-----------------
metric_key (PK)
metric_name (varchar) - deaths, cases, hospitalizations, vaccines
metric_description (varchar)
unit_of_measure (varchar) - "per million"
calculation_method (varchar) - "7-day rolling average"


## Relationships:

1. One **Location** can have many **CovidMetrics** records (one-to-many)
2. One **TimeData** entry can be associated with many **CovidMetrics** records (one-to-many)
3. Each **CovidMetrics** record is associated with exactly one **Location** and one **TimeData** (many-to-one)

This star schema design centers around the fact table (CovidMetrics) which contains all the measured values, while the dimension tables (Location and TimeData) provide the contextual information for analysis.
User: Ok, now provide me the ETL phases for that schema
Model: # ETL Process for COVID-19 Data Model

## Phase 1: Extraction

1. **Source Identification**:
   - `casos_confirmados_por_milhao_de_pessoas.csv`: Daily COVID-19 cases per million
   - `mortes_por_milhao_de_pessoas.csv`: Daily COVID-19 deaths per million
   - `doses_de_vacinas_por_milhao_de_pessoas.csv`: Daily vaccine doses per million
   - `hospitalizados_numero_total.csv`: Daily hospital occupancy per million

2. **Data Extraction**:
   - Read each CSV file using appropriate data processing tools (Python/Pandas, SQL SSIS, etc.)
   - Validate file structure and encoding
   - Extract header information to confirm data columns
   - Create raw data staging tables for each source file

## Phase 2: Transformation

1. **Data Cleansing**:
   - Handle missing values in all metrics
   - Remove duplicate entries if present
   - Standardize date formats across all files
   - Validate data types (numeric for metrics, date for Day column)

2. **Data Enrichment and Dimensional Preparation**:
   - **Location Dimension**: 
     - Extract unique Entity values from all files
     - Assign unique LocationID to each Entity
     - Obtain Population data from external source if needed for calculations

   - **TimeData Dimension**:
     - Extract unique Day values from all files
     - Create DateID as primary key
     - Derive Year and Month from Day values

   - **Metrics Transformation**:
     - Align data from all sources based on Entity and Day
     - Validate metric values (check for negative values, outliers)
     - Ensure consistent units (per million) across all metrics

3. **Data Integration**:
   - Join data from all four sources based on Entity and Day
   - Handle scenarios where metrics may be available for some locations/dates but not others

## Phase 3: Loading

1. **Dimensional Loading**:
   - Load Location dimension table first
   - Load TimeData dimension table
   - Verify referential integrity

2. **Fact Table Loading**:
   - Generate MetricID values
   - Map Entity to LocationID foreign keys
   - Map Day to DateID foreign keys
   - Load transformed metrics data into CovidMetrics fact table
   - Implement error handling for failed loads

3. **Data Validation**:
   - Verify row counts match between source and destination
   - Perform sample validation of transformed data against source
   - Check for orphaned records or referential integrity issues

## Phase 4: Orchestration & Scheduling

1. **Process Workflow**:
   - Create dependencies between dimension and fact loads
   - Implement logging and error notification
   - Design retry logic for failed steps

2. **Scheduling**:
   - Set up daily/weekly refresh schedule based on data update frequency
   - Configure incremental loading strategy for new data

## Phase 5: Maintenance & Monitoring

1. **Performance Monitoring**:
   - Track ETL job duration and resource utilization
   - Monitor data growth over time
   - Set up alerts for abnormal patterns or failed loads

2. **Data Quality Monitoring**:
   - Implement data quality checks (completeness, accuracy, consistency)
   - Create dashboards for data quality metrics

This ETL process ensures that data from the four CSV sources is properly integrated into the dimensional model, allowing for efficient querying and analysis of COVID-19 metrics across locations and time periods.
User: Using Databricks, provide me the detailed technical steps for Phase 1
Model: # Phase 1: Extraction in Databricks - Detailed Technical Steps

## 1. Environment Setup

```python
# Create a new Databricks notebook

# Set up necessary imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime
```

## 2. Define Schema and File Paths

```python
# Define schema for all CSV files
cases_schema = StructType([
    StructField("Entity", StringType(), False),
    StructField("Day", DateType(), False),
    StructField("Daily new confirmed cases of COVID-19 per million people (rolling 7-day average, right-aligned)", DoubleType(), True)
])

deaths_schema = StructType([
    StructField("Entity", StringType(), False),
    StructField("Day", DateType(), False),
    StructField("Daily new confirmed deaths due to COVID-19 per million people (rolling 7-day average, right-aligned)", DoubleType(), True)
])

vaccines_schema = StructType([
    StructField("Entity", StringType(), False),
    StructField("Day", DateType(), False),
    StructField("COVID-19 doses (daily, 7-day average, per million people)", DoubleType(), True)
])

hospital_schema = StructType([
    StructField("Entity", StringType(), False),
    StructField("Day", DateType(), False),
    StructField("Daily hospital occupancy per million", DoubleType(), True)
])

# Define file paths (adjust as needed based on your Databricks file storage)
dbfs_mount_point = "/mnt/covid_data/"
cases_file = dbfs_mount_point + "casos_confirmados_por_milhao_de_pessoas.csv"
deaths_file = dbfs_mount_point + "mortes_por_milhao_de_pessoas.csv"
vaccines_file = dbfs_mount_point + "doses_de_vacinas_por_milhao_de_pessoas.csv"
hospital_file = dbfs_mount_point + "hospitalizados_numero_total.csv"
```

## 3. Mount Storage (if using external storage like Azure Blob, S3, etc.)

```python
# Example for Azure Blob Storage
# Replace with your specific storage details
configs = {
  "fs.azure.account.key.<storage-account-name>.blob.core.windows.net": "<storage-account-key>"
}

# Mount storage if not already mounted
if not any(mount.mountPoint == dbfs_mount_point for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
      source = "wasbs://<container-name>@<storage-account-name>.blob.core.windows.net",
      mount_point = dbfs_mount_point,
      extra_configs = configs
    )
```

## 4. Extract and Validate Data from Source Files

```python
# Read COVID cases data
try:
    df_cases = spark.read.format("csv") \
                    .option("header", "true") \
                    .option("delimiter", ",") \
                    .schema(cases_schema) \
                    .load(cases_file)
    
    print(f"Successfully loaded cases data with {df_cases.count()} rows")
    print("Cases data sample:")
    display(df_cases.limit(5))
except Exception as e:
    print(f"Error loading cases data: {str(e)}")

# Read COVID deaths data
try:
    df_deaths = spark.read.format("csv") \
                     .option("header", "true") \
                     .option("delimiter", ",") \
                     .schema(deaths_schema) \
                     .load(deaths_file)
    
    print(f"Successfully loaded deaths data with {df_deaths.count()} rows")
    print("Deaths data sample:")
    display(df_deaths.limit(5))
except Exception as e:
    print(f"Error loading deaths data: {str(e)}")

# Read COVID vaccines data
try:
    df_vaccines = spark.read.format("csv") \
                       .option("header", "true") \
                       .option("delimiter", ",") \
                       .schema(vaccines_schema) \
                       .load(vaccines_file)
    
    print(f"Successfully loaded vaccines data with {df_vaccines.count()} rows")
    print("Vaccines data sample:")
    display(df_vaccines.limit(5))
except Exception as e:
    print(f"Error loading vaccines data: {str(e)}")

# Read COVID hospital data
try:
    df_hospital = spark.read.format("csv") \
                       .option("header", "true") \
                       .option("delimiter", ",") \
                       .schema(hospital_schema) \
                       .load(hospital_file)
    
    print(f"Successfully loaded hospital data with {df_hospital.count()} rows")
    print("Hospital data sample:")
    display(df_hospital.limit(5))
except Exception as e:
    print(f"Error loading hospital data: {str(e)}")
```

## 5. Profile Data for Structure and Quality Issues

```python
# Function to analyze dataframe
def analyze_dataframe(df, name):
    print(f"\n--- Analysis for {name} ---")
    print(f"Total rows: {df.count()}")
    print(f"Total columns: {len(df.columns)}")
    print("\nColumn statistics:")
    
    for col in df.columns:
        null_count = df.filter(df[col].isNull()).count()
        null_percent = (null_count / df.count()) * 100
        distinct_count = df.select(col).distinct().count()
        
        print(f"\nColumn: {col}")
        print(f"  - Data type: {df.schema[col].dataType}")
        print(f"  - Null count: {null_count} ({null_percent:.2f}%)")
        print(f"  - Distinct values: {distinct_count}")
        
        # If numeric column, show basic statistics
        if isinstance(df.schema[col].dataType, (IntegerType, DoubleType, FloatType, LongType)):
            stats = df.select(
                min(col).alias("min"),
                max(col).alias("max"),
                avg(col).alias("avg"),
                stddev(col).alias("stddev")
            ).collect()[0]
            
            print(f"  - Min: {stats['min']}")
            print(f"  - Max: {stats['max']}")
            print(f"  - Avg: {stats['avg']}")
            print(f"  - StdDev: {stats['stddev']}")

# Run analysis on all dataframes
analyze_dataframe(df_cases, "COVID Cases")
analyze_dataframe(df_deaths, "COVID Deaths")
analyze_dataframe(df_vaccines, "COVID Vaccines")
analyze_dataframe(df_hospital, "COVID Hospital")
```

## 6. Rename Columns for Consistency and Readability

```python
# Rename columns to make them more manageable
df_cases = df_cases \
    .withColumnRenamed("Daily new confirmed cases of COVID-19 per million people (rolling 7-day average, right-aligned)", "NewCasesPerMillion")

df_deaths = df_deaths \
    .withColumnRenamed("Daily new confirmed deaths due to COVID-19 per million people (rolling 7-day average, right-aligned)", "NewDeathsPerMillion")

df_vaccines = df_vaccines \
    .withColumnRenamed("COVID-19 doses (daily, 7-day average, per million people)", "VaccineDosesPerMillion")

df_hospital = df_hospital \
    .withColumnRenamed("Daily hospital occupancy per million", "HospitalOccupancyPerMillion")
```

## 7. Save Extracted Data to Delta Tables for Staging

```python
# Create a database for staging if it doesn't exist
spark.sql("CREATE DATABASE IF NOT EXISTS covid_staging")

# Write data to Delta tables in staging area
df_cases.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("covid_staging.cases_raw")

df_deaths.write \
    .format("delta") \

## Phase 2: Transformation

1. **Data Cleansing**:
   - Handle missing values in all metrics
   - Remove duplicate entries if present
   - Standardize date formats across all files
   - Validate data types (numeric for metrics, date for Day column)

2. **Data Enrichment and Dimensional Preparation**:
   - **Location Dimension**: 
     - Extract unique Entity values from all files
     - Assign unique LocationID to each Entity
     - Obtain Population data from external source if needed for calculations

   - **TimeData Dimension**:
     - Extract unique Day values from all files
     - Create DateID as primary key
     - Derive Year and Month from Day values

   - **Metrics Transformation**:
     - Align data from all sources based on Entity and Day
     - Validate metric values (check for negative values, outliers)
     - Ensure consistent units (per million) across all metrics

3. **Data Integration**:
   - Join data from all four sources based on Entity and Day
   - Handle scenarios where metrics may be available for some locations/dates but not others

User: based on the files:
casos_confirmados_por_milhao_de_pessoas.csv that has the heading as "Entity,Day,"Daily new confirmed cases of COVID-19 per million people (rolling 7-day average, right-aligned)"
mortes_por_milhao_de_pessoas.csv, heading as Entity,Day,"Daily new confirmed deaths due to COVID-19 per million people (rolling 7-day average, right-aligned)"
doses_de_vacinas_por_milhao_de_pessoas.csv, heading as "Entity,Day,"COVID-19 doses (daily, 7-day average, per million people)"
hospitalizados_numero_total.csv heading "Entity,Day,Daily hospital occupancy per million"
What wouil be the best modeling schema?
Model: # Modeling Schema Recommendation

Based on the four CSV files provided, I can recommend a modeling schema that would effectively organize this COVID-19 related data.

## Entity-Relationship Model

### Core Entities:

1. **Location**
   - LocationID (PK)
   - EntityName (from the "Entity" column in all files)
   - Population (derived or added, useful for per-million calculations)
   - Region/Continent (optional, for geographical grouping)

2. **TimeData**
   - DateID (PK)
   - Day (from the "Day" column in all files)
   - Year (derived)
   - Month (derived)
   - Week (derived)

3. **CovidMetrics** (Fact table)
   - MetricID (PK)
   - LocationID (FK)
   - DateID (FK)
   - NewCasesPerMillion (from casos_confirmados_por_milhao_de_pessoas.csv)
   - NewDeathsPerMillion (from mortes_por_milhao_de_pessoas.csv)
   - VaccineDosesPerMillion (from doses_de_vacinas_por_milhao_de_pessoas.csv)
   - HospitalOccupancyPerMillion (from hospitalizados_numero_total.csv)

## Schema Benefits:

1. **Dimensional Model**: This star schema allows for efficient querying across multiple metrics.
2. **Reduced Redundancy**: Entity information is stored once in the Location table.
3. **Time Series Analysis**: The TimeData dimension enables easy time-based querying.
4. **Data Integration**: All metrics are consolidated in one fact table with consistent keys.
5. **Scalability**: Easy to add new metrics without changing the schema structure.

## Implementation Notes:

- Each record in the fact table represents a unique combination of location, date, and the associated COVID-19 metrics.
- The schema handles the "rolling 7-day average" and "per million people" aspects implicit in the data.
- Missing values can be handled with NULL entries in the fact table.
- Consider adding data quality constraints for the metrics (e.g., non-negative values).

This schema would allow for efficient querying across locations and time periods, enabling analysis of relationships between cases, deaths, vaccinations, and hospitalizations.
User: remove Region/Continent  from location and wee from TimeData
Model: # Revised Modeling Schema Recommendation

Based on your feedback, I've revised the schema by removing Region/Continent from Location and Week from TimeData.

## Entity-Relationship Model

### Core Entities:

1. **Location**
   - LocationID (PK)
   - EntityName (from the "Entity" column in all files)
   - Population (derived or added, useful for per-million calculations)

2. **TimeData**
   - DateID (PK)
   - Day (from the "Day" column in all files)
   - Year (derived)
   - Month (derived)

3. **CovidMetrics** (Fact table)
   - MetricID (PK)
   - LocationID (FK)
   - DateID (FK)
   - NewCasesPerMillion (from casos_confirmados_por_milhao_de_pessoas.csv)
   - NewDeathsPerMillion (from mortes_por_milhao_de_pessoas.csv)
   - VaccineDosesPerMillion (from doses_de_vacinas_por_milhao_de_pessoas.csv)
   - HospitalOccupancyPerMillion (from hospitalizados_numero_total.csv)

## Schema Benefits:

1. **Streamlined Design**: The revised schema is more focused with only essential attributes.
2. **Dimensional Model**: Maintains the star schema for efficient querying.
3. **Reduced Redundancy**: Entity information is stored once in the Location table.
4. **Time Series Analysis**: The simplified TimeData dimension still enables effective date-based querying.
5. **Data Integration**: All metrics remain consolidated in one fact table with consistent keys.

## Implementation Notes:

- Each record in the fact table represents a unique combination of location, date, and the associated COVID-19 metrics.
- The schema handles the "rolling 7-day average" and "per million people" aspects implicit in the data.
- Missing values can be handled with NULL entries in the fact table.
- Consider adding data quality constraints for the metrics (e.g., non-negative values).

This revised schema maintains the analytical capabilities while focusing only on the necessary attributes you've specified.
User: Drar me the schema
Model: # COVID-19 Data Model Schema

```
                          ┌───────────────┐
                          │   Location    │
                          ├───────────────┤
                          │ LocationID PK │
                          │ EntityName    │
                          │ Population    │
                          └───────┬───────┘
                                  │
                                  │
           ┌────────────────────┬─┴──┬────────────────────┐
           │                    │    │                    │
           │                    │    │                    │
┌──────────▼─────────┐         │    │          ┌─────────▼──────────┐
│    CovidMetrics    │         │    │          │      TimeData      │
├────────────────────┤         │    │          ├────────────────────┤
│ MetricID       PK  │◄────────┘    └─────────►│ DateID         PK  │
│ LocationID     FK  │                         │ Day                │
│ DateID         FK  │                         │ Year               │
│ NewCasesPerMillion │                         │ Month              │
│ NewDeathsPerMillion│                         └────────────────────┘
│ VaccineDosesPerMil │
│ HospitalOccupancyPM│
└────────────────────┘
```

## Relationships:

1. One **Location** can have many **CovidMetrics** records (one-to-many)
2. One **TimeData** entry can be associated with many **CovidMetrics** records (one-to-many)
3. Each **CovidMetrics** record is associated with exactly one **Location** and one **TimeData** (many-to-one)

This star schema design centers around the fact table (CovidMetrics) which contains all the measured values, while the dimension tables (Location and TimeData) provide the contextual information for analysis.
User: Ok, now provide me the ETL phases for that schema
Model: # ETL Process for COVID-19 Data Model

## Phase 1: Extraction

1. **Source Identification**:
   - `casos_confirmados_por_milhao_de_pessoas.csv`: Daily COVID-19 cases per million
   - `mortes_por_milhao_de_pessoas.csv`: Daily COVID-19 deaths per million
   - `doses_de_vacinas_por_milhao_de_pessoas.csv`: Daily vaccine doses per million
   - `hospitalizados_numero_total.csv`: Daily hospital occupancy per million

2. **Data Extraction**:
   - Read each CSV file using appropriate data processing tools (Python/Pandas, SQL SSIS, etc.)
   - Validate file structure and encoding
   - Extract header information to confirm data columns
   - Create raw data staging tables for each source file

## Phase 2: Transformation

1. **Data Cleansing**:
   - Handle missing values in all metrics
   - Remove duplicate entries if present
   - Standardize date formats across all files
   - Validate data types (numeric for metrics, date for Day column)

2. **Data Enrichment and Dimensional Preparation**:
   - **Location Dimension**: 
     - Extract unique Entity values from all files
     - Assign unique LocationID to each Entity
     - Obtain Population data from external source if needed for calculations

   - **TimeData Dimension**:
     - Extract unique Day values from all files
     - Create DateID as primary key
     - Derive Year and Month from Day values

   - **Metrics Transformation**:
     - Align data from all sources based on Entity and Day
     - Validate metric values (check for negative values, outliers)
     - Ensure consistent units (per million) across all metrics

3. **Data Integration**:
   - Join data from all four sources based on Entity and Day
   - Handle scenarios where metrics may be available for some locations/dates but not others

## Phase 3: Loading

1. **Dimensional Loading**:
   - Load Location dimension table first
   - Load TimeData dimension table
   - Verify referential integrity

2. **Fact Table Loading**:
   - Generate MetricID values
   - Map Entity to LocationID foreign keys
   - Map Day to DateID foreign keys
   - Load transformed metrics data into CovidMetrics fact table
   - Implement error handling for failed loads

3. **Data Validation**:
   - Verify row counts match between source and destination
   - Perform sample validation of transformed data against source
   - Check for orphaned records or referential integrity issues

## Phase 4: Orchestration & Scheduling

1. **Process Workflow**:
   - Create dependencies between dimension and fact loads
   - Implement logging and error notification
   - Design retry logic for failed steps

2. **Scheduling**:
   - Set up daily/weekly refresh schedule based on data update frequency
   - Configure incremental loading strategy for new data

## Phase 5: Maintenance & Monitoring

1. **Performance Monitoring**:
   - Track ETL job duration and resource utilization
   - Monitor data growth over time
   - Set up alerts for abnormal patterns or failed loads

2. **Data Quality Monitoring**:
   - Implement data quality checks (completeness, accuracy, consistency)
   - Create dashboards for data quality metrics

This ETL process ensures that data from the four CSV sources is properly integrated into the dimensional model, allowing for efficient querying and analysis of COVID-19 metrics across locations and time periods.
User: Using Databricks, provide me the detailed technical steps for Phase 1
Model: # Phase 1: Extraction in Databricks - Detailed Technical Steps

## 1. Environment Setup

```python
# Create a new Databricks notebook

# Set up necessary imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime
```

## 2. Define Schema and File Paths

```python
# Define schema for all CSV files
cases_schema = StructType([
    StructField("Entity", StringType(), False),
    StructField("Day", DateType(), False),
    StructField("Daily new confirmed cases of COVID-19 per million people (rolling 7-day average, right-aligned)", DoubleType(), True)
])

deaths_schema = StructType([
    StructField("Entity", StringType(), False),
    StructField("Day", DateType(), False),
    StructField("Daily new confirmed deaths due to COVID-19 per million people (rolling 7-day average, right-aligned)", DoubleType(), True)
])

vaccines_schema = StructType([
    StructField("Entity", StringType(), False),
    StructField("Day", DateType(), False),
    StructField("COVID-19 doses (daily, 7-day average, per million people)", DoubleType(), True)
])

hospital_schema = StructType([
    StructField("Entity", StringType(), False),
    StructField("Day", DateType(), False),
    StructField("Daily hospital occupancy per million", DoubleType(), True)
])

# Define file paths (adjust as needed based on your Databricks file storage)
dbfs_mount_point = "/mnt/covid_data/"
cases_file = dbfs_mount_point + "casos_confirmados_por_milhao_de_pessoas.csv"
deaths_file = dbfs_mount_point + "mortes_por_milhao_de_pessoas.csv"
vaccines_file = dbfs_mount_point + "doses_de_vacinas_por_milhao_de_pessoas.csv"
hospital_file = dbfs_mount_point + "hospitalizados_numero_total.csv"
```

## 3. Mount Storage (if using external storage like Azure Blob, S3, etc.)

```python
# Example for Azure Blob Storage
# Replace with your specific storage details
configs = {
  "fs.azure.account.key.<storage-account-name>.blob.core.windows.net": "<storage-account-key>"
}

# Mount storage if not already mounted
if not any(mount.mountPoint == dbfs_mount_point for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
      source = "wasbs://<container-name>@<storage-account-name>.blob.core.windows.net",
      mount_point = dbfs_mount_point,
      extra_configs = configs
    )
```

## 4. Extract and Validate Data from Source Files

```python
# Read COVID cases data
try:
    df_cases = spark.read.format("csv") \
                    .option("header", "true") \
                    .option("delimiter", ",") \
                    .schema(cases_schema) \
                    .load(cases_file)
    
    print(f"Successfully loaded cases data with {df_cases.count()} rows")
    print("Cases data sample:")
    display(df_cases.limit(5))
except Exception as e:
    print(f"Error loading cases data: {str(e)}")

# Read COVID deaths data
try:
    df_deaths = spark.read.format("csv") \
                     .option("header", "true") \
                     .option("delimiter", ",") \
                     .schema(deaths_schema) \
                     .load(deaths_file)
    
    print(f"Successfully loaded deaths data with {df_deaths.count()} rows")
    print("Deaths data sample:")
    display(df_deaths.limit(5))
except Exception as e:
    print(f"Error loading deaths data: {str(e)}")

# Read COVID vaccines data
try:
    df_vaccines = spark.read.format("csv") \
                       .option("header", "true") \
                       .option("delimiter", ",") \
                       .schema(vaccines_schema) \
                       .load(vaccines_file)
    
    print(f"Successfully loaded vaccines data with {df_vaccines.count()} rows")
    print("Vaccines data sample:")
    display(df_vaccines.limit(5))
except Exception as e:
    print(f"Error loading vaccines data: {str(e)}")

# Read COVID hospital data
try:
    df_hospital = spark.read.format("csv") \
                       .option("header", "true") \
                       .option("delimiter", ",") \
                       .schema(hospital_schema) \
                       .load(hospital_file)
    
    print(f"Successfully loaded hospital data with {df_hospital.count()} rows")
    print("Hospital data sample:")
    display(df_hospital.limit(5))
except Exception as e:
    print(f"Error loading hospital data: {str(e)}")
```

## 5. Profile Data for Structure and Quality Issues

```python
# Function to analyze dataframe
def analyze_dataframe(df, name):
    print(f"\n--- Analysis for {name} ---")
    print(f"Total rows: {df.count()}")
    print(f"Total columns: {len(df.columns)}")
    print("\nColumn statistics:")
    
    for col in df.columns:
        null_count = df.filter(df[col].isNull()).count()
        null_percent = (null_count / df.count()) * 100
        distinct_count = df.select(col).distinct().count()
        
        print(f"\nColumn: {col}")
        print(f"  - Data type: {df.schema[col].dataType}")
        print(f"  - Null count: {null_count} ({null_percent:.2f}%)")
        print(f"  - Distinct values: {distinct_count}")
        
        # If numeric column, show basic statistics
        if isinstance(df.schema[col].dataType, (IntegerType, DoubleType, FloatType, LongType)):
            stats = df.select(
                min(col).alias("min"),
                max(col).alias("max"),
                avg(col).alias("avg"),
                stddev(col).alias("stddev")
            ).collect()[0]
            
            print(f"  - Min: {stats['min']}")
            print(f"  - Max: {stats['max']}")
            print(f"  - Avg: {stats['avg']}")
            print(f"  - StdDev: {stats['stddev']}")

# Run analysis on all dataframes
analyze_dataframe(df_cases, "COVID Cases")
analyze_dataframe(df_deaths, "COVID Deaths")
analyze_dataframe(df_vaccines, "COVID Vaccines")
analyze_dataframe(df_hospital, "COVID Hospital")
```

## 6. Rename Columns for Consistency and Readability

```python
# Rename columns to make them more manageable
df_cases = df_cases \
    .withColumnRenamed("Daily new confirmed cases of COVID-19 per million people (rolling 7-day average, right-aligned)", "NewCasesPerMillion")

df_deaths = df_deaths \
    .withColumnRenamed("Daily new confirmed deaths due to COVID-19 per million people (rolling 7-day average, right-aligned)", "NewDeathsPerMillion")

df_vaccines = df_vaccines \
    .withColumnRenamed("COVID-19 doses (daily, 7-day average, per million people)", "VaccineDosesPerMillion")

df_hospital = df_hospital \
    .withColumnRenamed("Daily hospital occupancy per million", "HospitalOccupancyPerMillion")
```

## 7. Save Extracted Data to Delta Tables for Staging

```python
# Create a database for staging if it doesn't exist
spark.sql("CREATE DATABASE IF NOT EXISTS covid_staging")

# Write data to Delta tables in staging area
df_cases.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("covid_staging.cases_raw")

df_deaths.write \
    .format("delta") \


## Phase 3: Loading

1. **Dimensional Loading**:
   - Load Location dimension table first
   - Load TimeData dimension table
   - Verify referential integrity

2. **Fact Table Loading**:
   - Generate MetricID values
   - Map Entity to LocationID foreign keys
   - Map Day to DateID foreign keys
   - Load transformed metrics data into CovidMetrics fact table
   - Implement error handling for failed loads

3. **Data Validation**:
   - Verify row counts match between source and destination
   - Perform sample validation of transformed data against source
   - Check for orphaned records or referential integrity issues

## Phase 4: Orchestration & Scheduling

1. **Process Workflow**:
   - Create dependencies between dimension and fact loads
   - Implement logging and error notification
   - Design retry logic for failed steps

2. **Scheduling**:
   - Set up daily/weekly refresh schedule based on data update frequency
   - Configure incremental loading strategy for new data

## Phase 5: Maintenance & Monitoring

1. **Performance Monitoring**:
   - Track ETL job duration and resource utilization
   - Monitor data growth over time
   - Set up alerts for abnormal patterns or failed loads

2. **Data Quality Monitoring**:
   - Implement data quality checks (completeness, accuracy, consistency)
   - Create dashboards for data quality metrics

This ETL process ensures that data from the four CSV sources is properly integrated into the dimensional model, allowing for efficient querying and analysis of COVID-19 metrics across locations and time periods.


# Phase 3: Loading in Databricks - Detailed Technical Steps

## 1. Create Target Database and Tables

```python
# Create target database for final data model
spark.sql("CREATE DATABASE IF NOT EXISTS covid_data")

# Create schema for Location dimension
spark.sql("""
CREATE TABLE IF NOT EXISTS covid_data.Location (
    LocationID BIGINT,
    EntityName STRING,
    Population DOUBLE,
    PRIMARY KEY (LocationID)
) USING DELTA
""")

# Create schema for TimeData dimension
spark.sql("""
CREATE TABLE IF NOT EXISTS covid_data.TimeData (
    DateID BIGINT,
    Day DATE,
    Year INT,
    Month INT,
    PRIMARY KEY (DateID)
) USING DELTA
""")

# Create schema for CovidMetrics fact table
spark.sql("""
CREATE TABLE IF NOT EXISTS covid_data.CovidMetrics (
    MetricID BIGINT,
    LocationID BIGINT,
    DateID BIGINT,
    NewCasesPerMillion DOUBLE,
    NewDeathsPerMillion DOUBLE,
    VaccineDosesPerMillion DOUBLE,
    HospitalOccupancyPerMillion DOUBLE,
    PRIMARY KEY (MetricID),
    FOREIGN KEY (LocationID) REFERENCES covid_data.Location(LocationID),
    FOREIGN KEY (DateID) REFERENCES covid_data.TimeData(DateID)
) USING DELTA
""")
```

## 2. Prepare Loading Operations with Validation

```python
# Function to validate dimension data before loading
def validate_dimension(df, name, id_column):
    # Check for unique IDs
    total_rows = df.count()
    unique_ids = df.select(id_column).distinct().count()
    
    if total_rows != unique_ids:
        print(f"WARNING: {name} dimension has duplicate IDs. Found {unique_ids} unique IDs out of {total_rows} rows.")
        # Optional: handle duplicates
        df = df.dropDuplicates([id_column])
    else:
        print(f"✓ {name} dimension has all unique IDs.")
    
    # Check for null values in key columns
    null_ids = df.filter(col(id_column).isNull()).count()
    if null_ids > 0:
        print(f"WARNING: {name} dimension has {null_ids} NULL ID values.")
        # Optional: filter nulls
        df = df.filter(col(id_column).isNotNull())
    else:
        print(f"✓ {name} dimension has no NULL IDs.")
    
    return df

# Function to validate fact table before loading
def validate_fact_table(df_fact, df_location, df_time):
    # Check for data volume
    fact_count = df_fact.count()
    print(f"Fact table has {fact_count} rows.")
    
    # Check for orphaned foreign keys (LocationIDs not in Location dimension)
    fact_location_ids = df_fact.select("LocationID").distinct()
    orphaned_locations = fact_location_ids.join(
        df_location.select("LocationID"),
        "LocationID",
        "left_anti"
    ).count()
    
    if orphaned_locations > 0:
        print(f"WARNING: {orphaned_locations} LocationIDs in fact table do not exist in Location dimension.")
    else:
        print("✓ All LocationIDs in fact table exist in Location dimension.")
    
    # Check for orphaned foreign keys (DateIDs not in TimeData dimension)
    fact_date_ids = df_fact.select("DateID").distinct()
    orphaned_dates = fact_date_ids.join(
        df_time.select("DateID"),
        "DateID",
        "left_anti"
    ).count()
    
    if orphaned_dates > 0:
        print(f"WARNING: {orphaned_dates} DateIDs in fact table do not exist in TimeData dimension.")
    else:
        print("✓ All DateIDs in fact table exist in TimeData dimension.")
    
    return df_fact
```

## 3. Load Dimension Tables

```python
# Validate and load Location dimension
df_location = validate_dimension(df_location, "Location", "LocationID")
df_location.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("covid_data.Location")

print(f"Successfully loaded {df_location.count()} records into Location dimension table.")

# Validate and load TimeData dimension
df_time = validate_dimension(df_time, "TimeData", "DateID")
df_time.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("covid_data.TimeData")

print(f"Successfully loaded {df_time.count()} records into TimeData dimension table.")
```

## 4. Load Fact Table with Error Handling

```python
# Validate fact table against dimensions
df_fact = validate_fact_table(df_fact, df_location, df_time)

# Begin a transaction to ensure atomicity
spark.sql("START TRANSACTION")

try:
    # Load the fact table
    df_fact.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("covid_data.CovidMetrics")
    
    # Commit the transaction if successful
    spark.sql("COMMIT")
    print(f"Successfully loaded {df_fact.count()} records into CovidMetrics fact table.")
except Exception as e:
    # Rollback the transaction if there's an error
    spark.sql("ROLLBACK")
    print(f"ERROR: Failed to load CovidMetrics fact table: {str(e)}")
    raise e
```

## 5. Create Table Optimizations

```python
# Set up Z-ordering for performance optimization
spark.sql("OPTIMIZE covid_data.CovidMetrics ZORDER BY (LocationID, DateID)")
spark.sql("OPTIMIZE covid_data.TimeData ZORDER BY (Day)")

# Create helpful views for common queries
spark.sql("""
CREATE OR REPLACE VIEW covid_data.covid_trends AS
SELECT 
    l.EntityName,
    t.Day,
    t.Year,
    t.Month,
    m.NewCasesPerMillion,
    m.NewDeathsPerMillion,
    m.VaccineDosesPerMillion,
    m.HospitalOccupancyPerMillion
FROM covid_data.CovidMetrics m
JOIN covid_data.Location l ON m.LocationID = l.LocationID
JOIN covid_data.TimeData t ON m.DateID = t.DateID
""")
```

## 6. Verify Data Integrity and Calculate Metrics

```python
# Collect statistics on the loaded data
def report_table_stats():
    # Count records in each table
    location_count = spark.table("covid_data.Location").count()
    time_count = spark.table("covid_data.TimeData").count()
    metrics_count = spark.table("covid_data.CovidMetrics").count()
    
    print("\