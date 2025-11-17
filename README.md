# Advocate data analyser
This repository contains my implementation of the Duel candidate take-home task.
The goal was to design and build a small but robust data-processing pipeline capable of ingesting ~10,000 JSON files of 
mixed quality, normalising inconsistent fields, validating the resulting data, storing it in a queryable form, 
and exposing useful insights through a simple API.

It is intentionally lightweight but structured as a real production service would evolve. I chose Python because I have spent
most of my time recently coding in it! But I'd be confident to write this solution in Typescript / Node.js as well.

Thank you for reviewing my submission!

## Prerequisites

- Python 3.14.0 or later
- UV 0.8.5 or later
- MongoDB (local Docker or standalone install)

## Setup for development

Create virtual environment and install dependencies:
```
uv sync --all-extras
```
To install the dev group dependencies specifically:
```
uv sync --group dev
```
To install the main dependencies without the dev dependencies:
```
uv sync --no-dev
```
Add new dependencies:
```
uv add dependency>=version
```
To add dev dependencies:
```
uv add --dev dependency>=version
```
Update dependencies (this will only update the lock file, pyproject.toml requires updating manually):
```
uv lock --upgrade
```

## Run tests

Ensure the dev dependencies are installed with:
```
uv sync --group dev
```
To run the tests with pytest:
```
uv run pytest
```
To run tests with coverage with pytest:
```
uv run pytest --cov-report term-missing --cov
```
To run specific tests, use `-k` and to include test logs in console use `--log-cli-level=10`
```
uv run pytest --log-cli-level=10 -k <test-name-search-term> 
```

## Running the app
Ensure MongoDB is running:
```
docker compose  up
```
Start the ingester (place the raw data in a folder such as `data/raw`)
```
uv run ingest-data --ingest-dir data\raw -v
```
Start the api (available at http://localhost:8000). See `\scripts\api.postman_collection.json` for sample requests.
```
uv run start-api
```

## Methodology
1. Discover a possible data schema using AI-generated script:
```
uv run script/generate_schema.py
```
2. Populate Pydantic models based on the inferred schema, using `extra="allow"` to safely accept and inspect unknown fields during exploration.
3. Write unit tests for the models, test against clean and malformed data.
4. Write cleaner functions based on analysis of the data.
5. Build an ingestion pipeline that normalises raw input, validates fields via Pydantic, stores the cleaned representation in MongoDB, and captures malformed records for inspection.
6. Expose the data through a simple API (using basic auth). Endpoints include:
    - GET `/users/{user_id}` - Returns a fully normalised advocate record, including all programs, tasks, and engagement metrics.
    - GET `/metrics/top-advocates?metric=conversions` - Ranks advocates by total sales attributed across all advocacy programs.
    - GET `/metrics/top-advocates?metric=engagement` - Ranks advocates by total engagement generated (likes + comments + shares).
    - GET `/metrics/brands/performance` - Aggregates tasks, engagement, reach, and attributed sales for each brand across the entire dataset.
    - GET `/metrics/outliers?metric=sales` - Identifies advocates whose sales impact exceeds the population mean by a configurable standard deviation threshold.
    - GET `/metrics/outliers?metric=engagement` - Identifies advocates whose engagement output is significantly higher than typical activity levels.

## Data cleaning & validation
The main data quality issues involved inconsistent date formats, malformed URLs, irregular social media handles, and mixed types (strings vs integers).
The pipeline resolves these deterministically so the stored dataset is fully normalised.

A design choice I made was to use a pydantic validator to normalise the social media handles. This could have been done in `cleaning_utils.py` 
for consistency, but I decided to keep it within the model itself to showcase the utility of pydantic validators for normalisation.

## Future improvements

### Front end dashboard
This would showcase the data in a simple dashboard, written in Angular or React, detailing:
- Real-time insights
- Program performance summaries
- Advocate profiles

### CI / CD
- Add CI pipeline (linting, tests, security checks)

### Security 
- Add HTTPS to the API (LetsEncrypt)

### Testing
- Complete unit testing for complete line coverage.
- Expand test suite including integration tests

### Data model:
- Further investigate the results in `data/raw_invalid_json/` and write rules to repair json files (if there are any!)
- Further investigate the results of the files in `data/raw_failed_validation/` and tweak validation rules
- Add schema-versioning system for data evolution
- Ingest raw data into Elasticsearch, use Kibana for visualisation and discovery (check all fields are captured in the pydantic models)

### Analytics
- Produce API queries that generate useful insights about specific advocates (e.g. top advocates by engagement and sales etc for a brand)
- Extend current `/metrics/outliers?metric=sales` and `/metrics/outliers?metric=engagement` to do this for advocates by brand
- Likewise produce queries that identify other outliers (e.g. advocates with high engagement but low sales)

### Optimisations
- Run auto-profiling on ingestion to benchmark slow fields

### Logging
- Add robust logging (and logging to file) and metrics instrumentation
- Add monitoring (e.g. Prometheus)

### Pipeline scaling (beyond MVP)
If this pipeline were deployed in a production environment processing millions of records, it could be evolved into a 
fully distributed ingestion and analytics system. Key areas for extension include:

- Move from local batch ingestion to a horizontally scalable streaming architecture (Kafka / Kinesis), enabling near-real-time processing, enrichment, and validation.
- Introduce a dead-letter queue (DLQ) for malformed or unprocessable records, enabling operators to inspect and reprocess failures without interrupting the main pipeline.
- Schedule ETL jobs, periodic recomputation, and downstream model updates using tools like Airflow
- Place FastAPI behind a load balancer to improve performance and scalability with autoscaling 

