Project Overview
This project is about credit card fraud analysis.
The goal of the project is to understand how fraud transactions behave and what patterns can be found in the data.
I use a real credit card transaction dataset and build a simple data pipeline in AWS.
The data is stored in S3, processed with AWS Glue, and analyzed using SQL in Athena.

During the project, the data is enriched with additional information such as:
	•	local transaction time and night transactions,
	•	public holidays,
	•	basic sanctions screening for merchants and customers.

The main focus of the project is not to build a perfect fraud model, but to show how data can be prepared, enriched, and analyzed in a cloud environment to support fraud detection decisions.
