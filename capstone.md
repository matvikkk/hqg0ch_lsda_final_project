
## The Goal of the Project

The goal of this capstone project is to design and implement an end-to-end data
analytics pipeline on AWS for analyzing global fishing activity.
The project focuses on ingesting large, heterogeneous datasets, transforming
and standardizing the data, cataloging it using AWS Glue, and enabling
SQL-based analysis through Amazon Athena.

Specifically, the project demonstrates how raw CSV datasets related to global,
high seas, and exclusive economic zone (EEZ) fishing can be converted into an
optimized columnar format (Parquet), stored in Amazon S3, and unified into a
single analytical table.
Using AWS Glue crawlers and the Glue Data Catalog, the metadata is automatically
generated and kept up to date as new data is added to the dataset.

In the final stage, Amazon Athena is used to perform analytical queries and
create reusable views that support economic and geographic analysis of fishing
activity.
The project evaluates fishing value and catch volumes across countries,
regions, and time periods, illustrating how AWS serverless analytics services
can be used to build scalable, cost-effective data analysis solutions without
managing underlying infrastructure.



### IAM role
The IAM role **CapstoneGlueRole** was verified in AWS IAM.
The role is trusted by the AWS Glue service and provides the required permissions to access Amazon S3, AWS Glue Data Catalog, and Amazon Athena.
No changes were made to the role.

<img width="717" height="480" alt="image" src="https://github.com/user-attachments/assets/364ab03b-58ea-45bb-aa53-81426f55b9fd" />


The Cloud9 environment was configured to use Secure Shell (SSH) connections, as required by the Capstone Lab instructions.


<img width="713" height="622" alt="image" src="https://github.com/user-attachments/assets/bba0421d-9a5d-4055-9a11-ee52ac75c03c" />



<img width="577" height="135" alt="image" src="https://github.com/user-attachments/assets/08b4c880-5d68-4c2e-83df-e9bff0f96c31" />


### Amazon S3 buckets
Two S3 buckets were created in the **us-east-1** region:
- **data-source-091003** — used to store raw and processed datasets
- **query-results-091003** — used to store Amazon Athena query results
All other bucket settings were left as default.
<img width="716" height="295" alt="image" src="https://github.com/user-attachments/assets/dce4651c-e517-44f1-a2aa-68fc17d577bc" />



<img width="581" height="157" alt="image" src="https://github.com/user-attachments/assets/119b6b27-3cc2-497c-b1b3-a72638523bf0" />




### Dataset inspection
The main dataset **SAU-GLOBAL-1-v48-0.csv** was inspected using Linux command-line tools.

The `head` command was used to review the column headers and sample records.
Each row includes:
- Year of fishing activity
- Fishing country (`fishing_entity`)
- Tonnes of fish caught
- Landed value in 2010 USD (`landed_value`)

The dataset contains **561,675 rows**, representing global high seas fishing activity from 1950 to 2018.

### CSV to Parquet conversion
The dataset `SAU-GLOBAL-1-v48-0.csv` was converted to Parquet format using pandas in the Cloud9 environment.
Parquet was chosen due to its columnar storage format, which improves performance and efficiency for analytical queries in AWS Glue and Amazon Athena.

### Upload to Amazon S3
The Parquet file `SAU-GLOBAL-1-v48-0.parquet` was uploaded to the
**data-source-091003** S3 bucket using the AWS CLI.
<img width="583" height="88" alt="image" src="https://github.com/user-attachments/assets/f9c54383-54e5-4d66-b924-1ce52bdbed0b" />

<img width="586" height="670" alt="image" src="https://github.com/user-attachments/assets/2c46de05-ec62-42e3-88ff-c461ff2fa401" />

### Inspection of SAU-HighSeas dataset
The file **SAU-HighSeas-71-v48-0.csv** was inspected using the `head` command
to review the column headers and sample records.
This dataset represents fishing activity in the global high seas.

<img width="592" height="238" alt="image" src="https://github.com/user-attachments/assets/24ce299e-1117-481c-80dd-3173332599bf" />

### SAU-HighSeas dataset processing
The file `SAU-HighSeas-71-v48-0.csv` was converted to Parquet format using pandas
and uploaded to the `data-source-091003` S3 bucket.
This dataset contains fishing activity data from the global high seas.

<img width="596" height="218" alt="image" src="https://github.com/user-attachments/assets/9fd58787-dfbb-4fd5-9567-843109ae7ef9" />

### Glue database
Created an AWS Glue database named **fishdb** to store Data Catalog tables for the fish datasets.
<img width="588" height="100" alt="image" src="https://github.com/user-attachments/assets/a98bfced-e998-4945-a52c-cdd455546994" />

### Glue crawler
Created a Glue crawler named **fishcrawler**.
- Data source: `s3://data-source-091003/`
- IAM role: **CapstoneGlueRole**
- Target database: **fishdb**
- Schedule: **On demand**
<img width="713" height="494" alt="image" src="https://github.com/user-attachments/assets/b95ed19a-ae41-4091-a026-2bbcff4eb8ba" />

### Glue table verification
After running the AWS Glue crawler **fishcrawler**, a table named
`data_source_091003` was successfully created in the **fishdb** Glue database.

The table classification was identified as **Parquet**, and the data location
was confirmed as `s3://data-source-091003/`.
<img width="954" height="651" alt="image" src="https://github.com/user-attachments/assets/8fbcf165-91c1-441e-81d1-8d386609fc5d" />
The inferred schema includes the expected columns such as:
- year
- fishing_entity
- landed_value
- area_name
- fishing_sector
- gear_type
and other descriptive fields.

This confirms that the crawler correctly processed the Parquet files
and populated the AWS Glue Data Catalog.

### Athena validation of table contents
To verify that the table correctly categorized the data, SQL queries were
executed in Amazon Athena against individual columns of the table
`fishdb.data_source_091003`.

The following query was used to validate the `area_name` column:

```sql
SELECT DISTINCT area_name
FROM fishdb.data_source_091003;

```
The results returned both the value “Pacific, Western Central” and NULL,
confirming that the dataset correctly combines records from:
	•	the High Seas dataset (with defined area names)
	•	the Global dataset (without area names)

This validated that the table structure and data ingestion were successful
and suitable for analytical queries.




### Fiji Pacific Western Central analysis
An Amazon Athena query was executed to calculate the annual value (in USD)
of fish caught by Fiji in the Pacific, Western Central high seas area since 2001.

The query grouped results by year and formatted the landed_value column
to display currency values in a reader-friendly format.

### Challenge query
A challenge query was created to calculate the total value of fish caught
by Fiji across all high seas areas since 2001.
The results were grouped by year and stored as a view named `challenge`
in the fishdb database.
<img width="1242" height="317" alt="image" src="https://github.com/user-attachments/assets/10d29aa6-361d-4996-9742-f92dbf392fee" />







### EEZ data transformation

<img width="829" height="543" alt="image" src="https://github.com/user-attachments/assets/544627fc-cb1e-45c8-b4cc-e4b05520c32e" />


The file `SAU-EEZ-242-v48-0.csv` contained column names that did not fully match
the existing dataset. The following transformations were applied using pandas:

- `fish_name` → `common_name`
- `country` → `fishing_entity`

After renaming the columns, the file was converted to Parquet format and
uploaded to the `data-source-091003` S3 bucket.

### Glue metadata update
The AWS Glue crawler was rerun to update the table metadata.
The table `fishdb.data_source_091003` was updated successfully to include
EEZ data.

<img width="1440" height="501" alt="image" src="https://github.com/user-attachments/assets/bcf21273-4c47-40ac-8c52-b74a753a3b4a" />


### Verification after adding EEZ data
After adding the EEZ dataset and rerunning the AWS Glue crawler, the table
`fishdb.data_source_091003` was updated successfully.

The following Athena query was used to verify the `area_name` column:

```sql
SELECT DISTINCT area_name
FROM fishdb.data_source_091003;
```
The query returned three distinct results:
	•	NULL (open seas / global records)
	•	“Pacific, Western Central” (high seas records)
	•	“Fiji” (exclusive economic zone records)

This confirms that the EEZ data was correctly integrated into the dataset
and that the table metadata was updated as expected.

<img width="950" height="523" alt="image" src="https://github.com/user-attachments/assets/4d24e0d5-98ea-48cc-864e-dcdb4f4ce59a" />


### Fiji catch consistency check
Three Athena queries were executed to calculate the annual landed value (USD)
of fish caught by Fiji:
- Open seas catch (`area_name IS NULL`)
- Fiji EEZ catch (`area_name LIKE '%Fiji%'`)
- Combined EEZ and open seas catch

For each year, the sum of the Open Seas and EEZ values matched the combined
EEZ and Open Seas value (within rounding precision).
This confirms that the EEZ data was successfully integrated and that the
dataset and Glue metadata are consistent.

For example, in **2001**:
- Open seas catch value was approximately **65.49 million USD**
- Fiji EEZ catch value was approximately **62.49 million USD**
- Combined EEZ and open seas catch value was approximately **127.98 million USD**

The sum of the Open Seas and EEZ values matches the combined value
(within rounding precision).

This pattern is consistent across all analyzed years, confirming that the
EEZ data was successfully integrated and that the dataset and AWS Glue
metadata are internally consistent and reliable for analysis.

<img width="950" height="649" alt="image" src="https://github.com/user-attachments/assets/c38fe26d-791d-4961-8939-3ef541f43950" />

<img width="950" height="665" alt="image" src="https://github.com/user-attachments/assets/5cb774fd-2da9-4b2d-981d-25b4113bc4a5" />

<img width="949" height="654" alt="image" src="https://github.com/user-attachments/assets/04984936-c910-4d41-b638-948f450ce214" />

### Analytical view: MackerelsCatch
A view named `MackerelsCatch` was created in Athena to analyze mackerel catches
after 2014, aggregated by year, location, and country.

The view was validated using a preview query and then used for analysis:

<img width="948" height="681" alt="image" src="https://github.com/user-attachments/assets/72172daa-7822-4470-92d4-501f249da62f" />

### MackerelsCatch analysis
Queries executed on the `MackerelsCatch` view show that mackerel catch volumes
vary significantly by year and country.

The analysis of maximum annual catch indicates that countries such as the USA
and Fiji recorded the highest mackerel catches in certain years after 2014.
This demonstrates that the view correctly aggregates and ranks catch volumes
by country and year.

A country-specific query for China shows that mackerel fishing activity
occurred across multiple areas, including the Pacific, Western Central region
and the Fiji EEZ. The highest catch volumes for China were observed in
2017–2018, highlighting both temporal and geographical variation in fishing
activity.

<img width="954" height="670" alt="image" src="https://github.com/user-attachments/assets/545c083d-4e71-4b24-af60-aeec6bed02d6" />

<img width="947" height="669" alt="image" src="https://github.com/user-attachments/assets/7fbf345e-4c5c-44e5-9a31-acd18873ea8c" />

### Custom query 1: Top fishing countries by landed value
This query identifies the top countries by total landed value (USD) of fish
since 2010. The results highlight which countries gain the highest economic
benefit from fishing activities in recent years.

<img width="948" height="672" alt="image" src="https://github.com/user-attachments/assets/f17775e5-06ed-42d1-a54c-b3fffad6c4b1" />

The results show that Indonesia dominates global fishing in terms of total
landed value since 2010, followed by China, Japan, and the USA.
This highlights a strong concentration of economic fishing activity in the
Asia-Pacific region.

### Custom query 2: Open seas vs EEZ comparison
This query compares the total landed value of fish caught in open seas versus
exclusive economic zones (EEZs) since 2010. The results illustrate how fishing
value is distributed between international waters and national zones.
<img width="949" height="673" alt="image" src="https://github.com/user-attachments/assets/d61bbd49-be28-47b5-b042-4ff6e3c00d92" />

The comparison shows that fishing in open seas generates significantly higher
total landed value than fishing within EEZs.
This underlines the economic importance of international waters and the need
for coordinated global fisheries management.

### Custom query 3: Most valuable fish species
This query identifies the fish species (by common name) with the highest total
landed value. The results provide insight into which species contribute most
to the economic value of global fishing.

<img width="953" height="673" alt="image" src="https://github.com/user-attachments/assets/89d4dc1f-e236-4843-a3a8-fc34b9a1618f" />

The most valuable fish species by landed value are dominated by tuna species,
particularly skipjack, yellowfin, and bigeye tuna.
This indicates a high economic dependency on a limited number of commercially
important species.

### Custom query 4: Global fishing value over time
This query analyzes the total annual landed value of global fishing since 2000.

<img width="947" height="683" alt="image" src="https://github.com/user-attachments/assets/f3454599-d46e-4096-87df-78d8d58cb2a7" />

The results show that global fishing value remained consistently high,
generally ranging between approximately 2.2 and 2.7 trillion USD per year.
A peak is observed around 2007–2009, followed by a temporary decline around
2010–2011, and a subsequent recovery in later years.

Overall, the trend indicates that while global fishing value fluctuates over
time, it remains economically significant, reflecting sustained global demand
and the importance of fisheries to the world economy.


## Submission
After submitting my work i recieved 25/25

<img width="1440" height="853" alt="image" src="https://github.com/user-attachments/assets/95917b64-9ba4-47d2-bd50-14537e5e1a5a" />

# Code Appendix — AWS Capstone Project Lab

This appendix contains all commands and code executed during the lab,
including Cloud9 bash commands, Python scripts, and Amazon Athena SQL queries.



## 1. Download source data (Cloud9)

~~~bash
wget https://aws-tc-largeobjects.s3.us-west-2.amazonaws.com/CUR-TF-200-ACDENG-1-91570/lab-capstone/s3/SAU-GLOBAL-1-v48-0.csv
wget https://aws-tc-largeobjects.s3.us-west-2.amazonaws.com/CUR-TF-200-ACDENG-1-91570/lab-capstone/s3/SAU-HighSeas-71-v48-0.csv
wget https://aws-tc-largeobjects.s3.us-west-2.amazonaws.com/CUR-TF-200-ACDENG-1-91570/lab-capstone/s3/SAU-EEZ-242-v48-0.csv

ls
head -6 SAU-GLOBAL-1-v48-0.csv
wc -l SAU-GLOBAL-1-v48-0.csv
~~~



## 2. Install dependencies (Cloud9)

~~~bash
sudo pip3 install pandas pyarrow fastparquet
sudo pip3 install "python-dateutil==2.8.2" --upgrade
aws --version
python3 -c "import dateutil; print(dateutil.__version__)"
~~~



## 3. Convert GLOBAL CSV to Parquet

~~~python
import pandas as pd

df = pd.read_csv('SAU-GLOBAL-1-v48-0.csv')
df.to_parquet('SAU-GLOBAL-1-v48-0.parquet')
~~~

~~~bash
aws s3 cp SAU-GLOBAL-1-v48-0.parquet s3://data-source-091003/
~~~



## 4. Convert High Seas CSV to Parquet

~~~python
import pandas as pd

df = pd.read_csv('SAU-HighSeas-71-v48-0.csv')
df.to_parquet('SAU-HighSeas-71-v48-0.parquet')
~~~

~~~bash
aws s3 cp SAU-HighSeas-71-v48-0.parquet s3://data-source-091003/
~~~



## 5. Athena — Validate table structure

~~~sql
SELECT DISTINCT area_name
FROM fishdb.data_source_091003;
~~~



## 6. Athena — Fiji, Pacific Western Central 

~~~sql
SELECT
  year,
  fishing_entity AS Country,
  CAST(CAST(SUM(landed_value) AS DOUBLE) AS DECIMAL(38,2)) AS ValuePacificWCSeasCatch
FROM fishdb.data_source_091003
WHERE area_name LIKE '%Pacific%'
  AND fishing_entity = 'Fiji'
  AND year > 2001
GROUP BY year, fishing_entity
ORDER BY year;
~~~



## 7. Athena — Challenge view 

~~~sql
CREATE OR REPLACE VIEW challenge AS
SELECT
  year,
  fishing_entity AS Country,
  CAST(CAST(SUM(landed_value) AS DOUBLE) AS DECIMAL(38,2)) AS ValueAllHighSeasCatch
FROM fishdb.data_source_091003
WHERE fishing_entity = 'Fiji'
  AND year >= 2001
  AND area_name IS NULL
GROUP BY year, fishing_entity
ORDER BY year;
~~~

~~~sql
SELECT COUNT(*) FROM challenge;
SELECT * FROM challenge LIMIT 5;
~~~



## 8. EEZ transformation 

~~~bash
cp SAU-EEZ-242-v48-0.csv SAU-EEZ-242-v48-0-old.csv
~~~

~~~python
import pandas as pd

df = pd.read_csv('SAU-EEZ-242-v48-0-old.csv')

df.rename(
    columns={
        "fish_name": "common_name",
        "country": "fishing_entity"
    },
    inplace=True
)

df.to_csv('SAU-EEZ-242-v48-0.csv', header=True, index=False)
df.to_parquet('SAU-EEZ-242-v48-0.parquet')
~~~

~~~bash
aws s3 cp SAU-EEZ-242-v48-0.parquet s3://data-source-091003/
~~~



## 9. Athena — Verify EEZ integration

~~~sql
SELECT DISTINCT area_name
FROM fishdb.data_source_091003;

SELECT DISTINCT area_type
FROM fishdb.data_source_091003
ORDER BY area_type;
~~~



## 10. Athena — Fiji Open Seas vs EEZ checks

~~~sql
SELECT
  year,
  fishing_entity AS Country,
  CAST(CAST(SUM(landed_value) AS DOUBLE) AS DECIMAL(38,2)) AS ValueOpenSeasCatch
FROM fishdb.data_source_091003
WHERE area_name IS NULL
  AND fishing_entity = 'Fiji'
  AND year > 2000
GROUP BY year, fishing_entity
ORDER BY year;
~~~

~~~sql
SELECT
  year,
  fishing_entity AS Country,
  CAST(CAST(SUM(landed_value) AS DOUBLE) AS DECIMAL(38,2)) AS ValueEEZCatch
FROM fishdb.data_source_091003
WHERE area_name LIKE '%Fiji%'
  AND fishing_entity = 'Fiji'
  AND year > 2000
GROUP BY year, fishing_entity
ORDER BY year;
~~~

~~~sql
SELECT
  year,
  fishing_entity AS Country,
  CAST(CAST(SUM(landed_value) AS DOUBLE) AS DECIMAL(38,2)) AS ValueEEZAndOpenSeasCatch
FROM fishdb.data_source_091003
WHERE (area_name LIKE '%Fiji%' OR area_name IS NULL)
  AND fishing_entity = 'Fiji'
  AND year > 2000
GROUP BY year, fishing_entity
ORDER BY year;
~~~



## 11. Athena — Create MackerelsCatch view

~~~sql
CREATE OR REPLACE VIEW MackerelsCatch AS
SELECT
  year,
  area_name AS WhereCaught,
  fishing_entity AS Country,
  SUM(tonnes) AS TotalWeight
FROM fishdb.data_source_091003
WHERE common_name LIKE '%Mackerels%'
  AND year > 2014
GROUP BY year, area_name, fishing_entity, tonnes
ORDER BY tonnes DESC;
~~~



## 12. Athena — Queries on MackerelsCatch

~~~sql
SELECT
  year,
  Country,
  MAX(TotalWeight) AS Weight
FROM fishdb.mackerelscatch
GROUP BY year, Country
ORDER BY year, Weight DESC;
~~~

~~~sql
SELECT *
FROM fishdb.mackerelscatch
WHERE Country = 'China';
~~~

## 13. Custom Query 1 — Top countries by landed value (since 2010)

~~~sql
SELECT
  fishing_entity AS Country,
  CAST(CAST(SUM(landed_value) AS DOUBLE) AS DECIMAL(38,2)) AS TotalValueUSD
FROM fishdb.data_source_091003
WHERE year >= 2010
GROUP BY fishing_entity
ORDER BY TotalValueUSD DESC
LIMIT 10;
~~~


## 14. Custom Query 2 — Open Seas vs EEZ comparison

~~~sql
SELECT
  CASE
    WHEN area_name IS NULL THEN 'Open Seas'
    ELSE 'EEZ'
  END AS AreaType,
  CAST(CAST(SUM(landed_value) AS DOUBLE) AS DECIMAL(38,2)) AS TotalValueUSD
FROM fishdb.data_source_091003
WHERE year >= 2010
GROUP BY
  CASE
    WHEN area_name IS NULL THEN 'Open Seas'
    ELSE 'EEZ'
  END;
~~~


## 15. Custom Query 3 — Most valuable fish species

~~~sql
SELECT
  common_name,
  CAST(CAST(SUM(landed_value) AS DOUBLE) AS DECIMAL(38,2)) AS TotalValueUSD
FROM fishdb.data_source_091003
WHERE common_name IS NOT NULL
GROUP BY common_name
ORDER BY TotalValueUSD DESC
LIMIT 10;
~~~

## 16. Custom Query 4 — Global fishing value over time

~~~sql
SELECT
  year,
  CAST(CAST(SUM(landed_value) AS DOUBLE) AS DECIMAL(38,2)) AS TotalValueUSD
FROM fishdb.data_source_091003
WHERE year >= 2000
GROUP BY year
ORDER BY year;
~~~
