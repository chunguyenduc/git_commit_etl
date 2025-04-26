# ETL Pipeline: GitHub Commits to PostgreSQL

## Overview

A massive shoutout to my bro [vuthanhhai2302](https://github.com/vuthanhhai2302) whose brilliant Python ETL pipeline inspired this Golang version. You can check out his work right [here](https://github.com/vuthanhhai2302/etl_pipelines_for_git_commit_data)

The code will run using a main script and all the processing function will be wrapped in the `processor` folder.

This ETL pipeline extracts commit data from the GitHub API, saves the raw data to file storage partitioned by month, converts the raw data into a list of commit models, and then loads the validated data into a PostgreSQL database. The process also includes post-load validations to ensure data integrity.

## Prerequisites
- **Go 1.22+**
- Environment variables set (`GITHUB_TOKEN` for API authentication)
- Docker compose
- Install dependencies
```bash
docker compose up -d
```

## Pipeline Components

- **Extractor:** Fetches commit data asynchronously from the GitHub API and aggregates it by month, save the aggregated commit data of each month to corresponding file
- **Transformer:** Loads and converts the file storage data into a list of validated commit model instances, push validated commits to channel
- **Loader:** Listen to Transformer data channel, perform batch inserts to destination data

## Pipeline Flow
Notes: the code will ingest and load to local storage and then load to destination database. the main reason is if we have trouble loading to the destination database, we can re run the failed task (if we are using a ochestrator).

1. **Configuration & Environment Setup:**  
The pipeline run date is determined using the current date. Config is loaded from `config.yaml`, and environment variables (e.g., `GITHUB_TOKEN`) are read for API authentication

2. **Data Extraction:**  
   The `Extractor` collects commit data from GitHub using asynchronous API calls. It aggregates commits by month for the past six months

3. **Saving to File Storage:**  
   The `Extractor` also writes the aggregated data into files (organized by year and month), returning a list of file paths

4. **Data Transformation:**  
   The `Transformer` loads the commit data from the files and converts it into a channel of commit model instances (`Commit`) for downstream processing

5. **Data Loading:**  
   A connection to PostgreSQL is established. Existing records for the current pipeline run date are deleted, and the new commit data is batch-inserted into the target table.

6. **Post-Load Validation:**  
   The pipeline verifies that the number of rows loaded into PostgreSQL matches the expected count from the file storage. If there is a mismatch, an error is logged and raised.

7. **Cleanup:**  
   The PostgreSQL connection is closed and a success log message is produced if all validations pass.

## SQL for queries
you can find the queries from folder sql.