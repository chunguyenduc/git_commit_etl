# ETL Pipeline: GitHub Commits to PostgreSQL

## Overview

A massive shoutout to my bro [vuthanhhai2302](https://github.com/vuthanhhai2302) whose brilliant Python ETL pipeline inspired this Golang version

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


## SQL for queries
you can find the queries from folder sql.