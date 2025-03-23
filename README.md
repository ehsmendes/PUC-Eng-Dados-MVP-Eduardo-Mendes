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



User: Based on the following data:
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

## Implementation Steps:

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

## Step 1: Set Up the Databricks Environment

```python
# Create a new database for our COVID data model
spark.sql("CREATE DATABASE IF NOT EXISTS covid_data_model")

# Use the new database
spark.sql("USE covid_data_model")
```

## Step 2: Upload Your CSV Files to Databricks

1. **Navigate to your Databricks workspace**
2. **Find the file upload option** (this varies by interface version):
   - Look for "Data" or "Tables" in the sidebar, or
   - Use the workspace file browser and navigate to FileStore

3. **Upload your four CSV files** to `/FileStore/covid_data/`

## Step 3: Read the Source CSV Files

```python
# Read all the source files
cases_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/FileStore/covid_data/file1.csv")
deaths_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/FileStore/covid_data/file2.csv")
vaccines_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/FileStore/covid_data/file3.csv")
hospital_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/FileStore/covid_data/file4.csv")

# Display sample data
display(cases_df.limit(5))
display(deaths_df.limit(5))
display(vaccines_df.limit(5))
display(hospital_df.limit(5))
```

## Step 4: Create the Dimension Tables

### Create Entity Dimension

```python
# Create a unified entity dimension from all sources
from pyspark.sql.functions import col, lit, monotonically_increasing_id

# Collect all unique entities
entities_from_cases = cases_df.select("Entity").distinct()
entities_from_deaths = deaths_df.select("Entity").distinct()
entities_from_vaccines = vaccines_df.select("Entity").distinct()
entities_from_hospital = hospital_df.select("Entity").distinct()

# Union all entities and remove duplicates
all_entities = entities_from_cases.union(entities_from_deaths).union(entities_from_vaccines).union(entities_from_hospital).distinct()

# Join with hospital data to get codes (which only appear in file4)
entity_codes = hospital_df.select("Entity", "Code").distinct()
dim_entity = all_entities.join(entity_codes, on="Entity", how="left")

# Add entity_id
dim_entity = dim_entity.withColumn("entity_id", monotonically_increasing_id())

# Create the dimension table
dim_entity.write.format("delta").mode("overwrite").saveAsTable("covid_data_model.dim_entity")

# Display the entity dimension
display(spark.table("covid_data_model.dim_entity"))
```

### Create Date Dimension

```python
from pyspark.sql.functions import to_date, year, month, dayofmonth, date_format, dayofweek

# Collect all unique dates
dates_from_cases = cases_df.select("Day").distinct()
dates_from_deaths = deaths_df.select("Day").distinct()
dates_from_vaccines = vaccines_df.select("Day").distinct()
dates_from_hospital = hospital_df.select("Day").distinct()

# Union all dates and remove duplicates
all_dates = dates_from_cases.union(dates_from_deaths).union(dates_from_vaccines).union(dates_from_hospital).distinct()

# Assume Day is in YYYY-MM-DD format
date_df = all_dates.withColumn("full_date", to_date(col("Day")))

# Create date hierarchies
dim_date = date_df \
    .withColumn("date_id", date_format(col("full_date"), "yyyyMMdd").cast("int")) \
    .withColumn("day", dayofmonth(col("full_date"))) \
    .withColumn("month", month(col("full_date"))) \
    .withColumn("year", year(col("full_date"))) \
    .withColumn("day_of_week", date_format(col("full_date"), "EEEE"))

# Create the dimension table
dim_date.write.format("delta").mode("overwrite").saveAsTable("covid_data_model.dim_date")

# Display the date dimension
display(spark.table("covid_data_model.dim_date"))
```

## Step 5: Create the Fact Tables

### Create COVID Cases Fact Table

```python
# Join cases data with dimension tables
fact_covid_cases = cases_df \
    .join(spark.table("covid_data_model.dim_entity"), on="Entity", how="inner") \
    .join(spark.table("covid_data_model.dim_date"), cases_df["Day"] == spark.table("covid_data_model.dim_date")["Day"], how="inner") \
    .select(
        col("entity_id"),
        col("date_id"),
        col("Daily new confirmed cases of COVID-19 per million people").alias("new_cases_per_million")
    )

# Create the fact table
fact_covid_cases.write.format("delta").mode("overwrite").saveAsTable("covid_data_model.fact_covid_cases")

# Display the fact table
display(spark.table("covid_data_model.fact_covid_cases").limit(10))
```

### Create COVID Deaths Fact Table

```python
# Join deaths data with dimension tables
fact_covid_deaths = deaths_df \
    .join(spark.table("covid_data_model.dim_entity"), on="Entity", how="inner") \
    .join(spark.table("covid_data_model.dim_date"), deaths_df["Day"] == spark.table("covid_data_model.dim_date")["Day"], how="inner") \
    .select(
        col("entity_id"),
        col("date_id"),
        col("Daily new confirmed deaths due to COVID-19 per million people (rolling 7-day average, right-aligned)").alias("new_deaths_per_million")
    )

# Create the fact table
fact_covid_deaths.write.format("delta").mode("overwrite").saveAsTable("covid_data_model.fact_covid_deaths")

# Display the fact table
display(spark.table("covid_data_model.fact_covid_deaths").limit(10))
```

### Create COVID Vaccinations Fact Table

```python
# Join vaccinations data with dimension tables
fact_covid_vaccinations = vaccines_df \
    .join(spark.table("covid_data_model.dim_entity"), on="Entity", how="inner") \
    .join(spark.table("covid_data_model.dim_date"), vaccines_df["Day"] == spark.table("covid_data_model.dim_date")["Day"], how="inner") \
    .select(
        col("entity_id"),
        col("date_id"),
        col("COVID-19 doses (daily, 7-day average, per million people)").alias("doses_per_million")
    )

# Create the fact table
fact_covid_vaccinations.write.format("delta").mode("overwrite").saveAsTable("covid_data_model.fact_covid_vaccinations")

# Display the fact table
display(spark.table("covid_data_model.fact_covid_vaccinations").limit(10))
```

### Create Hospital Occupancy Fact Table

```python
# Join hospital data with dimension tables
fact_hospital_occupancy = hospital_df \
    .join(spark.table("covid_data_model.dim_entity"), on="Entity", how="inner") \
    .join(spark.table("covid_data_model.dim_date"), hospital_df["Day"] == spark.table("covid_data_model.dim_date")["Day"], how="inner") \
    .select(
        col("entity_id"),
        col("date_id"),
        col("Daily hospital occupancy").alias("hospital_occupancy")
    )

# Create the fact table
fact_hospital_occupancy.write.format("delta").mode("overwrite").saveAsTable("covid_data_model.fact_hospital_occupancy")

#