## Project Overview

This project is about credit card fraud analysis.
The goal of the project is to understand how fraud transactions behave and what patterns can be found in the data.
I use a real credit card transaction dataset and build a simple data pipeline in AWS.
The data is stored in S3, processed with AWS Glue, and analyzed using SQL in Athena.

During the project, the data is enriched with additional information such as:
	•	local transaction time and night transactions,
	•	public holidays,
	•	basic sanctions screening for merchants and customers.

The main focus of the project is not to build a perfect fraud model, but to show how data can be prepared, enriched, and analyzed in a cloud environment to support fraud detection decisions.

**Business Context and Motivation**

Nowadays, credit card fraud and other types of banking fraud are one of the main security problems for financial institutions.
With the development of online banking and full digitalization of financial services, hacking a bank account or stealing card data can lead to a complete loss of money for a customer.

Fraud prevention is important not only for banks, but also for their clients. Customers want to feel safe and trust their bank, while banks want to protect their systems and reputation.

**Why Fraud Prevention Is Important for Banks**

Fraud is a serious problem for banks for several reasons:

- Banks lose clients if their systems are not secure and fraud cases happen too often.
- Banks are under strong pressure from regulators and government institutions.
- Banks receive many customer complaints, and in some cases they have to refund stolen money.
- Fraud also leads to unpaid credits and direct financial losses, which is not in the bank’s interest.
- One of the core functions of a bank is to keep customers’ money safe.

Because of this, fraud prevention is not optional for banks — it is a necessary part of their business.

**Why This Project Is Valuable for Banks**

Most fraud cases are not random. They usually follow certain schemes and patterns.
If these patterns are identified, banks can prevent future losses more effectively.

However, fraud patterns are often not obvious. Some of them may appear in variables or features where fraud is not expected at first glance. This is where data analytics becomes important. The goal of this project is to use transaction data to explore fraud patterns and risk indicators. Based on this analysis, banks can later build or improve rule-based anti-fraud systems that block specific fraudulent behaviors. It is also important to note that fraud schemes change over time. Because of this, fraud analytics should be a continuous process. Anti-fraud rules and models should be reviewed and updated regularly, for example once per year.

This project shows how a data analyst can help banks understand fraud behavior, support decision-making, and improve financial security.

**Stakeholders**

The main stakeholders of this project are:

• Bank customers, who want their money and personal data to be protected.  
• Banks and financial institutions, who want to reduce fraud losses and keep customer trust.  
• Risk and fraud departments, who use analytical results to design fraud prevention rules.  
• Regulators, who expect banks to follow security and compliance standards.

## Data Sources and APIs

**1. Transaction dataset (main data source)**

The main data source of this project is a transaction-level dataset containing credit card transactions.
It includes information such as transaction time, amount, merchant, customer details, geographic data, and fraud labels.

Business purpose:
This dataset represents real transaction behavior and allows the bank to analyze fraud patterns and risky activities.

Technical purpose:
It is used as the base layer of the data pipeline and is enriched with additional features.

**2. Sanctions API (sanctions.network)**

The sanctions API is used to check whether merchants or individuals appear in public sanctions lists.

Business purpose:
Transactions involving sanctioned persons or organizations represent a higher compliance and fraud risk for banks.

Technical purpose:
The API is used to enrich transaction data with sanctions-related indicators, which can later be analyzed or used in fraud rules.

**3. Public Holidays API (Nager.Date)**

This API provides official public holidays for the United States.

Business purpose:
Fraud activity often changes during holidays because of higher transaction volumes and reduced monitoring.

Technical purpose:
The holiday information is used to create features such as is_holiday and holiday_name for fraud analysis.

**4. Time Zone API (TimeAPI.io)**

The time zone API is used to determine the local time zone based on transaction latitude and longitude.

Business purpose:
Transactions happening at unusual local hours (for example at night) may indicate fraudulent behavior.

Technical purpose:
The API is used to convert transaction time into local time and create features such as local_hour and is_night_transaction.

## Data Pipeline Architecture

This project is built as a step-by-step analytical data pipeline designed for fraud analysis.
The pipeline transforms raw transaction data into enriched, analysis-ready datasets using AWS services.

### Step 1. Raw Data Ingestion (Amazon S3)

The project starts with a raw credit card transactions dataset stored in Amazon S3.
This dataset contains basic transaction information such as transaction time, amount, merchant, customer details, location, and fraud label.

S3 is used as a data lake because it is scalable, cost-efficient, and suitable for storing large volumes of raw data.

### Step 2. Data Enrichment and Feature Engineering (AWS Glue – Spark Jobs)

The core enrichment logic is implemented using AWS Glue Spark jobs.

In this step, the raw CSV data is transformed and enriched with additional analytical features:
	•	Time-based features
	•	Transaction timestamp parsing
	•	Year and month extraction (used for partitioning)
	•	Local transaction hour
	•	Night-time transaction flag (early morning hours)
	•	Geographical enrichment
	•	State-based time zone mapping
	•	Local transaction time approximation
	•	Calendar enrichment
	•	Public holiday detection
	•	Holiday name
	•	Holiday flag
	•	Fraud-related placeholders
	•	Columns prepared for sanctions-related indicators
	•	These columns allow easy extension of the pipeline

The enriched data is written back to Amazon S3 in Parquet format, partitioned by year and month.
This significantly improves query performance and reduces scanning costs in later analysis.

### Step 3. Sanctions Entity Preparation (AWS Glue – Spark Job)

To analyze sanctions-related risks, a separate Glue job extracts entities from the enriched dataset:
	•	Merchant names
	•	Customer full names (first name + last name)

These entities are deduplicated and stored as a separate reference dataset in S3.
This dataset represents potential entities that can be screened against sanctions lists.

This design allows sanctions screening to be performed independently from the main transaction pipeline and reused later if new sanctions sources are added.

### Step 4. Metadata Management (AWS Glue Crawler)

AWS Glue Crawlers are used to automatically detect schemas and register tables in the Glue Data Catalog.

Separate crawlers are created for:
	•	Raw transaction data
	•	Enriched transaction data
	•	Sanctions-related reference datasets

This enables SQL-based analysis using Amazon Athena without manual schema management.

### Step 5. Analytical Layer (Amazon Athena)

Amazon Athena is used as the main analytical engine.

Using SQL queries, the enriched dataset is analyzed to produce business-relevant insights such as:
	•	Fraud distribution by time of day
	•	Fraud during holidays vs non-holidays
	•	Fraud by state and merchant category
	•	Sanctions-related entity statistics

Athena allows fast, serverless querying directly on S3, which fits well with the project’s analytical goals.

### Optional / Future Step: Visualization Layer (Amazon QuickSight)

The pipeline is designed to support visualization in Amazon QuickSight.

However, QuickSight was not available due to account permission limitations.
Despite this, the dataset structure and KPIs are fully compatible with BI tools and can be visualized in the future without changing the pipeline.

### Notes on AWS Lambda

An AWS Lambda function was initially implemented to test lightweight enrichment and API-based logic.

In the final architecture, Lambda was not used for full-scale processing because:
	•	The dataset is large
	•	Batch Spark processing in AWS Glue is more efficient and scalable

Nevertheless, Lambda can be reused in the future for:
	•	Incremental enrichment
	•	Real-time or near real-time screening
	•	API-based fraud checks

### Summary

The final pipeline follows a clear and scalable structure:

S3 (Raw Data) → AWS Glue (Enrichment & Feature Engineering) → S3 (Enriched Data) → Glue Catalog → Athena (Analytics)

This architecture is cost-efficient, scalable, and suitable for real-world fraud analytics use cases.

## Key Performance Indicators (KPIs)

The following KPIs are used to evaluate fraud patterns and business risk.
They are based on both the original dataset and the enriched features.

### 1. Fraud rate

Percentage of transactions labeled as fraud.
This KPI gives a high-level view of how serious the fraud problem is in the dataset.

### 2. Fraud transactions by state

Number and share of fraudulent transactions per US state.
Helps identify geographic regions with higher fraud concentration.

### 3. Fraud transactions by merchant category

Fraud count grouped by merchant category (e.g. online shopping, personal care, health).
Useful for detecting high-risk business sectors.

### 4. Night-time fraud ratio

Share of fraudulent transactions that occur at night.
Night transactions often show higher fraud risk and can be used for rule-based detection.

### 5. Holiday vs non-holiday fraud

Comparison of fraud activity during holidays versus regular days.
Important for understanding seasonal and behavioral effects.

### 6. Average transaction amount for fraud vs non-fraud

Comparison of average amounts between fraudulent and normal transactions.
Helps detect abnormal transaction sizes.

### 7. Fraud frequency by customer

Number of fraud cases per customer.
Can indicate compromised accounts or repeated fraud attempts.

### 8. Fraud frequency by merchant

Number of fraud cases linked to specific merchants.
Useful for merchant risk monitoring and investigations.

## Implementation 

### 1. Creating Amazon S3 Data Lake

At the first step of the project, an Amazon S3 bucket was created to store all project data.
S3 is used as the main storage layer because it is scalable, cheap, and well integrated with other AWS services such as AWS Glue and Athena.

The bucket is structured as a simple data lake with separate folders for different stages of data processing.
This approach makes the pipeline clear and easy to extend in the future.
<img width="1220" height="398" alt="image" src="https://github.com/user-attachments/assets/cbec8be8-1499-4fe3-a346-c1746d2df510" />

#### S3 Bucket Structure

The following folder structure was used inside the bucket:
- raw/ (where we downloaded the original dataset)
- enriched/ (where we keep the enriched data)
- reference/ (where information about sanction hits is stored)
- analytics/ (where the final data for analisis, dashboards and ml will be stored)
- athena-results/ (where we keep the results of athena querries)

#### Uploading Raw Data

The original credit card transactions dataset was uploaded to the raw/ folder in CSV format.
This dataset is never modified directly and is used as the source for all further processing steps.

<img width="1438" height="496" alt="image" src="https://github.com/user-attachments/assets/a4d11794-9759-4e8a-9f48-a91180e084de" />

### Lambda Function: Initial Enrichment Attempt and Limitations

At an early stage of the project, an AWS Lambda function was used to test data enrichment logic on raw transaction data stored in Amazon S3.
The goal of this step was to validate that external data sources and enrichment logic (such as time-based features and sanctions screening) could be applied correctly to the dataset.

The Lambda function successfully demonstrated that:
	•	Data can be read from S3
	•	Enrichment logic works correctly on a small subset of rows
	•	Enriched results can be written back to S3

This step was mainly used as a proof of concept and a functional smoke test.
<img width="863" height="675" alt="image" src="https://github.com/user-attachments/assets/2448989f-ad8d-4df8-92f1-9e6276388a87" />

#### Performance Issues and Timeout Errors

However, the full dataset contains hundreds of thousands of rows.
When the Lambda function was executed on larger inputs, several limitations became clear:
	•	Lambda execution time was exceeded, even after increasing the timeout limit
	•	External API calls significantly increased execution time
	•	Processing the dataset row-by-row inside Lambda was not scalable
	•	Memory and execution constraints made full enrichment impractical

Even after tuning parameters (such as limiting the number of processed rows and reducing API calls), Lambda was not suitable for batch processing at this scale.

This behavior is expected, as AWS Lambda is designed for short-lived, event-driven workloads, not large analytical batch jobs.

#### Smoke Test Strategy

To avoid blocking the project due to Lambda limitations, a smoke test approach was applied.

Instead of processing the full dataset, the Lambda function was executed on:
	•	A very small sample of rows
	•	Limited number of enrichment operations

The smoke test confirmed that:
	•	The enrichment logic itself is correct
	•	Data flow between S3 and Lambda works as expected
	•	Output files are created correctly in the target S3 location

After this validation, Lambda was intentionally excluded from the main production pipeline.
The AWS Lambda function used for data enrichment is available here:
[Lambda enrichment code](docs/code/glue/lambda_enrichment.py)

Based on the observed limitations, the project pipeline was redesigned.
For full-scale enrichment and transformation of the dataset, AWS Glue (Spark-based processing) was chosen instead of Lambda.
Lambda remains documented in the project as an initial experimentation and validation step, while AWS Glue is used for all production-scale data processing.

### ETL Job: Transaction Enrichment (Time, Timezone, Holidays)

At this stage of the project, an AWS Glue ETL job was created to enrich raw transaction data with additional analytical features.  
The main goal of this job was to transform raw CSV transaction data into a structured and enriched dataset suitable for analytical queries in Amazon Athena.

#### Purpose of the ETL Job

This ETL job performs the following tasks:
- Parses transaction timestamps into a proper datetime format
- Extracts date-related features (year, month)
- Adds timezone information based on the US state
- Creates time-based risk indicators (night transactions)
- Enriches data with US public holidays
- Converts the dataset into a partitioned Parquet format for efficient querying


#### Input and Output

- **Input data:** raw transaction CSV files stored in Amazon S3  
- **Output data:** enriched transaction data stored in Amazon S3 in Parquet format, partitioned by year and month

**S3 paths are passed as job parameters:**
- `RAW_S3_PATH` – path to raw CSV data
- `OUT_S3_PATH` – path where enriched Parquet files are written

<img width="1438" height="744" alt="image" src="https://github.com/user-attachments/assets/83925ebb-1ba8-4cbf-accd-0cefb687b618" />


#### Key Transformations

#### Timestamp Processing
Transaction timestamps are parsed and converted into structured time fields:
- `trans_date`
- `trans_year`
- `trans_month`

#### Timezone Enrichment
Each transaction is assigned a timezone based on the US state code.  
This allows correct interpretation of transaction time in local context.

Derived fields:
- `timezone`
- `local_hour`
- `is_night_transaction` (transactions between 00:00 and 05:00)

#### Holiday Enrichment
US public holidays for years 2019–2021 are mapped directly inside the ETL job.  
Two new fields are added:
- `holiday_name`
- `is_holiday`

This enables analysis of fraud behavior during holiday periods.

#### Output Format

The enriched dataset is written in **Parquet format** and partitioned by:
- `trans_year`
- `trans_month`

This structure significantly improves query performance in Amazon Athena.

<img width="1217" height="314" alt="image" src="https://github.com/user-attachments/assets/5826ac05-a57e-4397-bb01-c2eff8d0bc7d" />

#### Glue ETL Job Code

Below is the core AWS Glue (PySpark) script used for this enrichment process:

```python
import sys
import json
import urllib.request
import urllib.parse
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F
from pyspark.sql import types as T

args = getResolvedOptions(
    sys.argv,
    ["RAW_S3_PATH", "OUT_S3_PATH", "MAX_TZ_CALLS", "HOLIDAY_COUNTRY"]
)

raw_path = args["RAW_S3_PATH"]
out_path = args["OUT_S3_PATH"]
max_tz_calls = int(args["MAX_TZ_CALLS"])
holiday_country = args["HOLIDAY_COUNTRY"]

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("escape", "\"")
    .option("multiLine", "false")
    .load(raw_path)
)

df = df.withColumn("trans_ts", F.to_timestamp(F.col("trans_date_trans_time"), "yyyy-MM-dd HH:mm:ss"))
df = df.withColumn("trans_date", F.to_date(F.col("trans_ts")))
df = df.withColumn("trans_year", F.year(F.col("trans_ts")))
df = df.withColumn("trans_month", F.month(F.col("trans_ts")))

df = df.withColumn("latitude_d", F.col("lat").cast("double"))
df = df.withColumn("longitude_d", F.col("long").cast("double"))

years = [r["y"] for r in df.select(F.col("trans_year").alias("y")).where(F.col("y").isNotNull()).distinct().collect()]

def http_get_json(url: str, timeout_s: int = 10):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout_s) as r:
        return json.loads(r.read().decode("utf-8"))

holiday_map = {}
for y in years:
    try:
        url = f"https://date.nager.at/api/v3/publicholidays/{int(y)}/{holiday_country}"
        data = http_get_json(url, timeout_s=20)
        if isinstance(data, list):
            for item in data:
                d = item.get("date")
                name = item.get("name") or item.get("localName")
                if d and name:
                    holiday_map[d] = str(name)
    except Exception:
        pass

b_holiday_map = sc.broadcast(holiday_map)

tz_schema = T.StructType([
    T.StructField("lat", T.DoubleType(), True),
    T.StructField("lon", T.DoubleType(), True),
    T.StructField("timezone_api", T.StringType(), True),
    T.StructField("utc_offset", T.StringType(), True)
])

def timeapi_timezone(lat: float, lon: float):
    try:
        qs = urllib.parse.urlencode({"latitude": lat, "longitude": lon})
        url = f"https://timeapi.io/api/TimeZone/coordinate?{qs}"
        data = http_get_json(url, timeout_s=10)
        tz = data.get("timeZone") if isinstance(data, dict) else None
        off = data.get("utcOffset") if isinstance(data, dict) else None
        return (tz, off)
    except Exception:
        return (None, None)

def tz_partition(rows):
    cache = {}
    calls = 0
    for row in rows:
        lat = row["lat"]
        lon = row["lon"]
        if lat is None or lon is None:
            yield (lat, lon, None, None)
            continue
        key = (round(float(lat), 5), round(float(lon), 5))
        if key in cache:
            tz, off = cache[key]
            yield (lat, lon, tz, off)
            continue
        if max_tz_calls > 0 and calls >= max_tz_calls:
            yield (lat, lon, None, None)
            continue
        tz, off = timeapi_timezone(key[0], key[1])
        cache[key] = (tz, off)
        calls += 1
        yield (lat, lon, tz, off)

coords = (
    df.select(F.col("latitude_d").alias("lat"), F.col("longitude_d").alias("lon"))
      .where(F.col("lat").isNotNull() & F.col("lon").isNotNull())
      .dropDuplicates()
)

tz_rdd = coords.rdd.mapPartitions(tz_partition)
tz_df = spark.createDataFrame(tz_rdd, schema=tz_schema)

df = df.join(
    tz_df,
    (df.latitude_d == tz_df.lat) & (df.longitude_d == tz_df.lon),
    "left"
).drop("lat", "lon")

@F.udf(returnType=T.StringType())
def holiday_name_udf(d):
    if d is None:
        return None
    return b_holiday_map.value.get(str(d), None)

df = df.withColumn("holiday_name", holiday_name_udf(F.date_format(F.col("trans_date"), "yyyy-MM-dd")))
df = df.withColumn("is_holiday", F.when(F.col("holiday_name").isNotNull(), F.lit(1)).otherwise(F.lit(0)))

df = df.withColumn("local_hour", F.hour(F.col("trans_ts")))
df = df.withColumn(
    "is_night_transaction",
    F.when((F.col("local_hour") >= 0) & (F.col("local_hour") <= 5), F.lit(1)).otherwise(F.lit(0))
)

df = df.withColumn("sanctions_hit_merchant", F.lit(None).cast(T.IntegerType()))
df = df.withColumn("sanctions_hit_customer", F.lit(None).cast(T.IntegerType()))

df_out = df.drop("trans_ts")

(
    df_out.write.mode("overwrite")
    .format("parquet")
    .partitionBy("trans_year", "trans_month")
    .save(out_path)
)
```
### Glue Job 2 – Build Entities to Screen

This job extracts **unique merchants and persons** from the enriched dataset.  
It prepares a clean list of entities that can later be checked against sanctions APIs.

<img width="582" height="510" alt="image" src="https://github.com/user-attachments/assets/7804a289-b1bc-4539-a842-f82a031ea63c" />


#### Code
```python
import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F

def _parse_args(argv):
    d = {}
    i = 0
    while i < len(argv):
        a = argv[i]
        if a.startswith("--"):
            k = a[2:]
            v = None
            if i + 1 < len(argv) and not argv[i + 1].startswith("--"):
                v = argv[i + 1]
                i += 1
            d[k] = v
        i += 1
    return d

args = _parse_args(sys.argv)
in_path = args.get("IN_PATH")
out_path = args.get("OUT_PATH")

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

df = spark.read.parquet(in_path)

merchants = (
    df.select(F.col("merchant").cast("string").alias("entity"))
    .where(F.col("entity").isNotNull() & (F.length(F.col("entity")) > 0))
    .dropDuplicates()
    .withColumn("entity_type", F.lit("merchant"))
)

persons = (
    df.select(F.concat_ws(" ", F.col("first").cast("string"), F.col("last").cast("string")).alias("entity"))
    .where(F.col("entity").isNotNull() & (F.length(F.col("entity")) > 3))
    .dropDuplicates()
    .withColumn("entity_type", F.lit("person"))
)

entities = merchants.unionByName(persons).dropDuplicates(["entity", "entity_type"])

entities.write.mode("overwrite").format("parquet").save(out_path)
```
<img width="1438" height="518" alt="image" src="https://github.com/user-attachments/assets/3fec1eb8-be3b-4579-a840-dcf051136b4a" />

### Glue Job 3 — Sanctions screening via external API

This Glue job checks prepared entities against the sanctions.network API.
For each entity, the job determines:
- whether there is a sanctions hit
- number of matches
- sources of sanctions
- top match identifier

To control execution time and costs, the job supports a MAX_ENTITIES parameter.

<img width="403" height="128" alt="image" src="https://github.com/user-attachments/assets/7388c11c-952c-46f3-8c35-d0a1a2e02ac4" />

```python

# Glue Job 3 — screen_entities.py

import sys
import json
import urllib.parse
import urllib.request
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import types as T

args = getResolvedOptions(sys.argv, ["IN_PATH", "OUT_PATH", "MAX_ENTITIES"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

in_path = args["IN_PATH"]
out_path = args["OUT_PATH"]
max_entities = int(args["MAX_ENTITIES"])

df = spark.read.parquet(in_path).select("entity", "entity_type").dropDuplicates()

if max_entities > 0:
    df = df.limit(max_entities)

schema = T.StructType([
    T.StructField("entity", T.StringType(), True),
    T.StructField("entity_type", T.StringType(), True),
    T.StructField("sanctions_hit", T.IntegerType(), True),
    T.StructField("sanctions_matches", T.IntegerType(), True),
    T.StructField("sanctions_sources", T.StringType(), True),
    T.StructField("top_match_id", T.StringType(), True)
])

def call_api(name):
    try:
        q = urllib.parse.quote(name)
        url = f"https://api.sanctions.network/rpc/search_sanctions?name={q}"
        with urllib.request.urlopen(url, timeout=8) as r:
            data = json.loads(r.read().decode("utf-8"))

        if not data:
            return (0, 0, None, None)

        sources = sorted({item.get("source") for item in data if item.get("source")})
        top_id = data[0].get("id")
        return (1, len(data), "|".join(sources), top_id)
    except Exception:
        return (0, 0, None, None)

def enrich_partition(rows):
    for row in rows:
        name = row["entity"]
        if not name or len(name.strip()) < 4:
            yield (name, row["entity_type"], 0, 0, None, None)
            continue
        hit, matches, sources, top_id = call_api(name)
        yield (name, row["entity_type"], hit, matches, sources, top_id)

rdd = df.rdd.mapPartitions(enrich_partition)
out_df = spark.createDataFrame(rdd, schema)

out_df.write.mode("overwrite").format("parquet").save(out_path)
```

### Glue Job 4 — Join sanctions data with enriched transactions

This Glue job joins sanctions screening results back to the enriched transactions dataset.
Sanctions flags are applied to:
- merchant name
- person name (first + last)

The final dataset contains a single sanctions indicator per transaction.
```python
# Glue Job 4 — join_sanctions.py

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

enriched_path = "s3://lsda-fraud-data-hqg0ch/enriched/full/"
sanctions_path = "s3://lsda-fraud-data-hqg0ch/reference/sanctions/screened/"
out_path = "s3://lsda-fraud-data-hqg0ch/enriched/final/"

df = spark.read.parquet(enriched_path)
san = spark.read.parquet(sanctions_path)

df = df.withColumn("person_name", F.concat_ws(" ", F.col("first"), F.col("last")))

san_merch = san.filter(F.col("entity_type") == "merchant") \
    .select(
        F.col("entity").alias("merchant_entity"),
        F.col("sanctions_hit").alias("merchant_sanctions_hit")
    )

san_person = san.filter(F.col("entity_type") == "person") \
    .select(
        F.col("entity").alias("person_entity"),
        F.col("sanctions_hit").alias("person_sanctions_hit")
    )

df = df.join(san_merch, df.merchant == san_merch.merchant_entity, "left")
df = df.join(san_person, df.person_name == san_person.person_entity, "left")

df = df.withColumn(
    "sanctions_hit",
    F.when(
        (F.col("merchant_sanctions_hit") == 1) | (F.col("person_sanctions_hit") == 1),
        F.lit(1)
    ).otherwise(F.lit(0))
)

df = df.drop(
    "merchant_entity",
    "person_entity",
    "merchant_sanctions_hit",
    "person_sanctions_hit"
)

df.write.mode("overwrite").format("parquet").save(out_path)
```
<img width="1438" height="737" alt="image" src="https://github.com/user-attachments/assets/1a265c47-7965-4345-80c0-72911dccda93" />



### Data Catalog and Table Creation (AWS Glue Crawler)

After the ETL job finished and enriched data was written to Amazon S3 in Parquet format, the next step was to make this data queryable using SQL.  
For this purpose, AWS Glue Crawler was used to automatically create table metadata in the Glue Data Catalog.

#### Purpose of the crawler
The crawler scans the enriched S3 data and:
- detects schema (columns and data types),
- creates a table in the Glue Data Catalog,
- allows querying the data later in Amazon Athena.

This step is necessary to move from raw files in S3 to structured analytical queries.

#### Crawler configuration

The crawler was created with the following settings:

- **Data source**: Amazon S3  
- **S3 path**:  s3:///enriched/final/
- **Data format**: Parquet
- **Crawler output**: Glue Data Catalog
- **Database name**: `fraud_db`
- **Table name**: generated automatically by Glue
- **Crawler schedule**: Run on demand

#### Running the crawler

After configuration, the crawler was started manually.  
When the crawler finished successfully, a new table appeared in the Glue Data Catalog under the `fraud_db` database.

<img width="1291" height="330" alt="image" src="https://github.com/user-attachments/assets/1d870929-51fb-4da8-a8af-e9bbe06f7cc5" />


#### Resulting table

As a result of the crawler execution:
- A table with enriched transaction data was created
- All columns from the ETL job (timestamps, timezone, holiday flags, night transactions, etc.) were available
- Partition columns `trans_year` and `trans_month` were detected automatically

<img width="1439" height="761" alt="image" src="https://github.com/user-attachments/assets/dab89aa0-e3b7-4399-a777-a5ec6c85fb87" />

### Data Analysis with Amazon Athena

After building the final enriched dataset, all analytical queries were executed using **Amazon Athena**.
Athena allows running SQL queries directly on data stored in Amazon S3 without loading it into a separate database.

The table used for analysis is `final_final`, which contains:
- original transaction data
- time-based features (local hour, night flag)
- holiday indicators
- geographic information
- fraud label (`is_fraud`)

Below are the key KPIs calculated during the analysis phase.


### Athena KPI Queries (final table: `final_final`)

Below are the KPI queries I used in Athena and short interpretations based on the results.


#### KPI 1 — Night vs Day transactions (volume)

~~~sql
SELECT
  is_night_transaction,
  COUNT(*) AS transactions_cnt
FROM final_final
GROUP BY is_night_transaction;
~~~

**Result (from screenshot):**
- `is_night_transaction = 0` → **446,774**
- `is_night_transaction = 1` → **108,945**

**Interpretation:** most transactions happen outside night hours, but night is still a big share of activity.

**Screenshot:** 
<img width="1242" height="139" alt="image" src="https://github.com/user-attachments/assets/a5330416-d4cb-446a-b9cf-a5c3bc2425c5" />


#### KPI 2 — Night vs Day fraud rate

~~~sql
SELECT
  is_night_transaction,
  COUNT(*) AS total_cnt,
  SUM(CAST(is_fraud AS INTEGER)) AS fraud_cnt,
  CAST(SUM(CAST(is_fraud AS INTEGER)) AS DOUBLE) / COUNT(*) AS fraud_rate
FROM final_final
GROUP BY is_night_transaction;
~~~

**Result (from screenshot):**
- Day (`0`): total **446,774**, fraud **1,391**, fraud_rate **0.003113**
- Night (`1`): total **108,945**, fraud **754**, fraud_rate **0.006921**

**Interpretation:** night transactions have ~**2.2x higher** fraud rate than day transactions (0.0069 vs 0.0031). This is a strong signal for risk scoring.

**Screenshot:** 
<img width="1246" height="136" alt="image" src="https://github.com/user-attachments/assets/c79001f8-26e1-49cc-8f07-ed4c27c1c5aa" />


#### KPI 3 — Holiday vs Non-holiday fraud rate

~~~sql
SELECT
  is_holiday,
  COUNT(*) AS total_cnt,
  SUM(CAST(is_fraud AS INTEGER)) AS fraud_cnt,
  CAST(SUM(CAST(is_fraud AS INTEGER)) AS DOUBLE) / COUNT(*) AS fraud_rate
FROM final_final
GROUP BY is_holiday;
~~~

**Result (from screenshot):**
- Holiday (`1`): total **15,644**, fraud **65**, fraud_rate **0.004155**
- Non-holiday (`0`): total **540,075**, fraud **2,080**, fraud_rate **0.003851**

**Interpretation:** holiday fraud rate is slightly higher than normal days (0.00415 vs 0.00385). The effect exists, but it is not as strong as the night-time effect.

**Screenshot:** 
<img width="1250" height="128" alt="image" src="https://github.com/user-attachments/assets/14bc4ed0-7051-48eb-a483-a7d9607e0e31" />


#### KPI 4 — Top states by fraud rate (min volume included)

~~~sql
SELECT
  state,
  COUNT(*) AS total_cnt,
  SUM(CAST(is_fraud AS INTEGER)) AS fraud_cnt,
  CAST(SUM(CAST(is_fraud AS INTEGER)) AS DOUBLE) / COUNT(*) AS fraud_rate
FROM final_final
GROUP BY state
ORDER BY fraud_rate DESC
LIMIT 10;
~~~

**Result (from screenshot, top 10 by fraud_rate):**
- AK: 0.01661 (14 / 843)
- CT: 0.01221 (40 / 3,277)
- ID: 0.00884 (22 / 2,490)
- HI: 0.00826 (9 / 1,090)
- MT: 0.00732 (37 / 5,052)
- DC: 0.00659 (10 / 1,517)
- IN: 0.00627 (75 / 11,959)
- OR: 0.00615 (48 / 7,811)
- MS: 0.00611 (54 / 8,833)
- VA: 0.005997 (75 / 12,506)

**Interpretation:** some states show higher fraud rates, but several have small total volume (example: AK), so we should not over-interpret. For reporting, it’s better to add a minimum volume filter (e.g., `HAVING COUNT(*) >= 5000`) to avoid noise.

**Screenshot:** 
<img width="1244" height="292" alt="image" src="https://github.com/user-attachments/assets/626399ff-0a44-4e08-9974-f03481c18062" />

#### KPI 5 — Top categories by fraud rate

~~~sql
SELECT
  category,
  COUNT(*) AS total_cnt,
  SUM(CAST(is_fraud AS INTEGER)) AS fraud_cnt,
  CAST(SUM(CAST(is_fraud AS INTEGER)) AS DOUBLE) / COUNT(*) AS fraud_rate
FROM final_final
GROUP BY category
ORDER BY fraud_rate DESC
LIMIT 10;
~~~

**Result (from screenshot, top 10 by fraud_rate):**
- shopping_net: 0.01211 (506 / 41,779)
- misc_net: 0.009756 (267 / 27,367)
- grocery_pos: 0.009228 (485 / 52,553)
- shopping_pos: 0.004278 (213 / 49,791)
- gas_transport: 0.002732 (154 / 56,370)
- travel: 0.002293 (40 / 17,449)
- grocery_net: 0.002111 (41 / 19,426)
- misc_pos: 0.002082 (72 / 34,574)
- personal_care: 0.001780 (70 / 39,327)
- entertainment: 0.001471 (59 / 40,104)

**Interpretation:** fraud is concentrated in a few categories (especially `shopping_net`, `misc_net`, `grocery_pos`). This is useful for rule creation and monitoring dashboards.

**Screenshot:** 
<img width="1245" height="294" alt="image" src="https://github.com/user-attachments/assets/d0fcd5a8-c46c-49dd-b908-5cea9cfbb7a9" />

#### KPI 6 — Fraud rate by local hour (timezone-based)

~~~sql
SELECT
  local_hour,
  COUNT(*) AS total_cnt,
  SUM(CAST(is_fraud AS INTEGER)) AS fraud_cnt,
  CAST(SUM(CAST(is_fraud AS INTEGER)) AS DOUBLE) / COUNT(*) AS fraud_rate
FROM final_final
GROUP BY local_hour
ORDER BY local_hour;
~~~

**Interpretation (based on your screenshot snippet):**
- late hours (22–23) show very high fraud_rate (~0.018–0.019)
- early hours (0–3) also have elevated fraud rates (~0.009–0.010)
This supports the earlier “night risk” KPI and adds more detail for hour-based rules.

**Screenshot:** 


<img width="1241" height="565" alt="image" src="https://github.com/user-attachments/assets/d3f84000-a4b5-4934-b700-00998f62bfd4" />


### KPI 7 — Fraud rate by amount bucket (FIXED query)

~~~sql
SELECT
  CASE
    WHEN CAST(amt AS DOUBLE) < 10 THEN '<10'
    WHEN CAST(amt AS DOUBLE) < 50 THEN '10-50'
    WHEN CAST(amt AS DOUBLE) < 100 THEN '50-100'
    WHEN CAST(amt AS DOUBLE) < 500 THEN '100-500'
    ELSE '500+'
  END AS amt_bucket,
  COUNT(*) AS total_cnt,
  SUM(CAST(is_fraud AS INTEGER)) AS fraud_cnt,
  CAST(SUM(CAST(is_fraud AS INTEGER)) AS DOUBLE) / COUNT(*) AS fraud_rate
FROM final_final
GROUP BY 1
ORDER BY fraud_rate DESC;
~~~

**Result (from screenshot):**
- 500+: fraud_rate **0.17045** (1,036 / 6,078)
- 100–500: **0.006697** (629 / 93,923)
- 10–50: **0.002176** (315 / 144,742)
- <10: **0.001020** (147 / 144,141)
- 50–100: **0.000108** (18 / 166,835)

**Interpretation:** the highest amount bucket (`500+`) has extremely high fraud rate (~17%). That is a very strong signal and should be highlighted as a key risk pattern. It also suggests we should double-check if the dataset has synthetic behavior or if “high amount” really correlates with fraud in this sample.

**Screenshot:** 
<img width="1251" height="179" alt="image" src="https://github.com/user-attachments/assets/8fb40a0c-ab2e-46b3-9ae4-f51606927f4b" />


### KPI 8 — Fraud rate by gender

~~~sql
SELECT
  gender,
  COUNT(*) AS total_cnt,
  SUM(CAST(is_fraud AS INTEGER)) AS fraud_cnt,
  CAST(SUM(CAST(is_fraud AS INTEGER)) AS DOUBLE) / COUNT(*) AS fraud_rate
FROM final_final
GROUP BY gender;
~~~

**Result (from screenshot):**
- F: fraud_rate **0.003818** (1,164 / 304,886)
- M: fraud_rate **0.003911** (981 / 250,833)

**Interpretation:** fraud rates are very close by gender in this dataset (difference is small), so gender is not a strong risk separator here.

**Screenshot:** 
<img width="1235" height="132" alt="image" src="https://github.com/user-attachments/assets/b045fc5e-c94f-445c-8c1f-f5a6dcbe011a" />

### Overall Analysis and Interpretation (Athena Results)

Based on the SQL analysis performed in Amazon Athena on the final enriched dataset, several important fraud-related patterns were identified. These patterns directly support the business goals of the project: **reducing fraud losses, improving risk detection, and supporting data-driven antifraud rules**.

#### Night vs Day Transactions
Fraud rate during night hours is **significantly higher** than during daytime.  
This confirms a well-known fraud pattern: fraudulent transactions are more likely to occur when users are less active and monitoring is weaker.  
**Business implication:** night-time transactions should have stricter controls or higher risk scores.

#### Holidays vs Non-Holidays
Transactions made on holidays show a **slightly higher fraud rate** compared to regular days.  
This can be explained by increased online activity and reduced operational monitoring during holidays.  
**Business implication:** banks can introduce temporary risk adjustments for holidays.

#### Fraud by Transaction Amount (Query 7)
The strongest signal was found in transaction amount buckets:

- Transactions above **$500** have an extremely high fraud rate compared to all other buckets.
- Low and mid-range transactions show much lower fraud probability.

**Business implication:** large transactions should trigger stronger verification mechanisms or real-time blocking rules.

#### Fraud by Merchant Category
Certain categories such as **online shopping and network-based merchants** show much higher fraud rates than others.  
This reflects higher exposure to card-not-present fraud.  
**Business implication:** category-based risk rules can be implemented.

#### Fraud by State
Some states show higher fraud rates than the average, even with lower total transaction volumes.  
This suggests **regional risk differences**.  
**Business implication:** regional fraud coefficients can be added to scoring models.

#### Fraud by Hour
Fraud activity peaks during late-night and early-morning hours.  
This supports the earlier night-vs-day conclusion and provides **hour-level granularity** for rule design.

#### Fraud by Gender
Fraud rates between genders are very similar, indicating **no strong demographic bias** in this dataset.  
**Business implication:** gender should not be used as a primary fraud indicator.



### Relation to Business Goals

This project directly addresses the initial business objectives:

- Identify hidden fraud patterns in transaction data  
- Provide explainable KPIs for antifraud teams  
- Support the design of rule-based and ML-based fraud prevention systems  
- Reduce financial losses and improve customer trust  

The extracted KPIs can be transformed into **production-ready antifraud rules** or used as features in machine learning models.


### Visualization and QuickSight (Future Work)

Amazon QuickSight was planned as the main visualization layer for this project.  
Due to account permission limitations, QuickSight dashboards could not be created.

**Planned dashboards (future work):**
- Fraud rate by hour and day
- Fraud rate by amount bucket
- Fraud rate by merchant category
- Fraud heatmap by state

These dashboards would allow non-technical stakeholders to monitor fraud trends in real time.


### Final Result

As a result, a complete end-to-end analytical pipeline was built:

- Raw data ingestion
- Feature enrichment (time, holidays, geography)
- Sanctions screening (as an extension)
- Analytical KPIs in Athena
- Clear link between technical results and business value

The project demonstrates how **data analytics can directly support fraud prevention strategy in financial institutions**.

