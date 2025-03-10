# AWS-S3-to-PostgreSQL-ETL-with-FastAPI
This project automates the process of:

Extracting product and category data from AWS S3
Performing an OUTER JOIN to merge datasets
Saving the joined dataset back to S3
Filtering Electronics category and storing in S3
Loading both joined and filtered datasets into PostgreSQL
Providing an API endpoint via FastAPI to trigger data loading

Features
✅ Automated data pipeline with AWS S3 and PostgreSQL
✅ Auto-detects schema and creates database tables dynamically
✅ Uses SQLAlchemy for database interactions
✅ FastAPI for API-driven ETL execution

Prerequisites
Python installed
PostgreSQL running
AWS credentials for S3 access
