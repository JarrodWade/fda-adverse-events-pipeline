# FDA Adverse Events Pipeline

## Project Overview

This project implements an ELT (Extract, Load, Transform) pipeline for processing FDA adverse event data related to statin drugs. The pipeline fetches data from the FDA API, processes it, stores it in Amazon S3, and then loads it into Snowflake. It's designed to be run as an Airflow DAG using Astronomer Cosmos.

## Pipeline Steps

1. Fetch adverse event data for specified statin drugs from the FDA API.
2. Save processed data as CSV files and upload to Amazon S3 bucket.
3. Load CSV files from S3 bucket into Snowflake.
4. Create dbt project and models to transform Snowflake data.

## Technology Stack

- Apache Airflow (workflow orchestration)
- Astronomer Cosmos (managed Apache Airflow, easy to use with dbt)
- Python 3.x (extraction)
- Amazon S3 (raw data)
- Snowflake (warehouse)
- dbt (data transformation)

## Setup and Installation

1. Clone this repository:
   ```
   git clone https://github.com/your-username/your-repo-name.git
   cd your-repo-name
   ```

2. Set up a virtual environment (if preferred):
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
   ```

3. Install the required packages. Key pieces are astronomer-cosmos and dbt-core (or dbt-snowflake, dbt-postgres, etc, depending on the warehouse you choose).

https://docs.getdbt.com/docs/core/connect-data-platform/snowflake-setup

https://astronomer.github.io/astronomer-cosmos/getting_started/open-source.html

4. Spin up your Airflow environment using Astronomer Cosmos managed Apache Airflow using 'astro dev start' in the command line. This will create a Docker container with Airflow.

5. To access Airflow, go to 'localhost:8080' in your web browser.

6. Set up an S3 bucket in AWS and configure the Airflow connection, including AWS credentials for S3 access.

7. (If using Snowflake) Set up Snowflake connection in Airflow.

8. (If using Snowflake) Configure Snowflake user, permissions, and any warehouse, database, and schema you would like to use to house this project's data. 

9. (If using Snowflake) Configure an S3 Storage Integration and Stage for your Snowflake account to access data in S3. This will also likely require you to manage IAM roles and policies on both Snowflake and AWS.

## Running the Pipeline

The pipeline is designed to be run as an Airflow DAG. Once your Airflow environment is set up:

1. Make sure the `fda_adverse_events_dag.py` file is in your Airflow DAGs directory.
2. Make sure the dbt folder is in the Airflow DAGs directory.
3. The DAG should appear in the Airflow UI.
4. Trigger the DAG manually or wait for it to run based on the scheduled interval.

## Configuration

- Modify the list of drugs in `fda_tasks.py` if needed. In this example, we are using statins.
- Adjust the date range for data fetching in `fda_tasks.py` or allow the DAG to run on a schedule.
- Update the S3 bucket name in `fda_tasks.py` to the name of the bucket you created in your AWS account.

## Future Enhancements

- Apply additional data quality checks to data loaded into Snowflake via dbt models.
- Integrate results with data visualization tools like Grafana, Tableau, dbt Studio, etc.

## Contact

Jarrod Wade - jarrod.wadej@gmail.com