# FDA Adverse Events Pipeline

## Project Overview

This project implements an ELT (Extract, Load, Transform) pipeline for processing FDA adverse event data related to statin drugs. The pipeline fetches data from the FDA API, processes it, stores it in Amazon S3, and then loads it into Snowflake for long term housing and further analysis. It's designed to be run as an Airflow DAG using Astronomer Cosmos.

Data Source: 
```https://open.fda.gov```


<img width="210" alt="Screenshot 2024-09-18 at 7 47 42 PM" src="https://github.com/user-attachments/assets/5048958f-5bb1-4775-b83d-3e4d95d809e9">


## Technology Stack

- Apache Airflow (workflow orchestration)
- Astronomer Cosmos (manages Apache Airflow, easy to use with dbt)
- Python 3.12 (extraction)
- Amazon S3 (raw data load)
- Snowflake (warehousing)
- dbt (data transformation)

```
FDA_ADVERSE_EVENTS_PIPELINE/
│
├── dags/
│ ├── dbt/
│ │ ├── dbt_packages/
│ │ ├── macros/
│ │ │ ├── calculate_age_in_years.sql
│ │ │ ├── determine_age_group.sql
│ │ │ ├── get_outcome_values.sql
│ │ │ ├── outcome_mapping.sql
│ │ │ ├── unique_combination.sql
│ │ │ └── weight_conversions.sql
│ │ ├── models/
│ │ │ ├── marts/
│ │ │ │ ├── age_group_analysis.sql
│ │ │ │ ├── dim_date.sql
│ │ │ │ ├── dim_drug.sql
│ │ │ │ ├── dim_patient.sql
│ │ │ │ ├── drug_summary.sql
│ │ │ │ ├── fct_adverse_events.sql
│ │ │ │ ├── reaction_analysis.sql
│ │ │ │ ├── seriousness_analysis.sql
│ │ │ │ └── time_based_analysis.sql
│ │ │ └── staging/
│ │ │ └── stg_adverse_events.sql
│ │ ├── tests/
│ │ │ ├── ensure_weight_conversion_accurate.sql
│ │ │ └── test_is_valid_date.sql
│ │ ├── schema.yml
│ │ └── sources.yml
│ └── tasks/
│  └── fda_tasks.py
├── .gitignore
├── Dockerfile
├── README.md
└── requirements.txt
```

## Pipeline Steps / Overview

1. Fetch adverse event data for specified statin drugs from the FDA API.
2. Save processed data as CSV files and upload to Amazon S3 bucket for raw data storage.
3. Load CSV files from S3 bucket into Snowflake.
4. Create dbt project and models to transform Snowflake data.

## Slightly More Detailed Overview (with pictures!!!)

The primary extraction and load tasks are defined in `dags/tasks/fda_tasks.py` -- 


<img width="857" alt="Screenshot 2024-09-18 at 7 53 42 PM" src="https://github.com/user-attachments/assets/52a38559-1f40-4dfe-828f-885a4b63cf9b">

____________________________________
____________________________________

-- Extract and Load to S3 Tasks are then called and orchestrated in `dags/fda_adverse_events_dag.py` 



<img width="617" alt="Screenshot 2024-09-18 at 7 57 53 PM" src="https://github.com/user-attachments/assets/c2ca1f89-cdc7-42a5-8b1c-831ebefd372d">



<img width="937" alt="Screenshot 2024-09-18 at 8 28 35 PM" src="https://github.com/user-attachments/assets/81efa1e2-32e4-4d54-9473-8f6dbce87b37">

____________________________________
____________________________________


-- Snowflake COPY INTO command from S3 Stage: 



<img width="460" alt="Screenshot 2024-09-18 at 8 04 33 PM" src="https://github.com/user-attachments/assets/4858f17b-17c4-48ac-bc9c-85a19e0fc73e">

____________________________________
____________________________________

-- dbt transforms our data with staging, fact, dimension, and analysis models. 



<img width="570" alt="Screenshot 2024-09-18 at 8 18 02 PM" src="https://github.com/user-attachments/assets/b7ccc641-a979-48fb-8578-f8b0e85bf5fb">


____________________________________
____________________________________

-- dbt materializes our facts, dimensions, and analyses in Snowflake.



<img width="1036" alt="Screenshot 2024-09-18 at 8 14 16 PM" src="https://github.com/user-attachments/assets/b4eb1159-d9b7-4544-b211-1b997ef2ded1">


____________________________________
____________________________________
Orchestrated using Astronomer Cosmos managed Airflow. 


<img width="1422" alt="Screenshot 2024-09-18 at 8 12 16 PM" src="https://github.com/user-attachments/assets/00898a79-c809-46f3-b651-cd7bd014284a">



## Data Model

The data model consists of the following main tables:

- `dim_date`: Date dimension table
- `dim_drug`: Drug dimension table
- `dim_patient`: Patient dimension table
- `fct_adverse_events`: Fact table containing adverse event data
- Various analysis tables (e.g., `age_group_analysis`, `reaction_analysis`)


## Setup and Installation -- 
(There are quite a few steps and connections, so bear with me. I have linked some good documentation as well) :)

1. Clone this repository:
   ```
   git clone https://github.com/JarrodWade/fda_adverse_events_pipeline
   cd fda_adverse_events_pipeline
   ```

2. Set up a virtual environment (if preferred):
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
   ```

3. Install the required packages. Key pieces are astronomer-cosmos, dbt-snowflake (or dbt-postgres, etc, depending on the warehouse you choose), and dbt-labs dbt_utils.

   Helpful documentation: 

      - DBT-SNOWFLAKE
      
         - https://docs.getdbt.com/docs/core/connect-data-platform/snowflake-setup
      
     
         - https://pypi.org/project/dbt-snowflake/
      

      - ASTRONOMER-COSMOS
      
         - https://astronomer.github.io/astronomer-cosmos/getting_started/open-source.html
      

      - DBT-UTILS
      
         - https://hub.getdbt.com/dbt-labs/dbt_utils/latest/
      

5. (If using Snowflake) Configure Snowflake user, role (with GRANT PRIVELEGES), and any warehouse, database, and schema you would like to use to house this project's data.
      - You will want to make sure to update the dbt_project.yml and sources.yml with the relevant Snowflake references. In my example, we are using DBT_WH, DBT_SCHEMA, DBT_DB,       DBT_ROLE.

      
         https://docs.getdbt.com/reference/database-permissions/snowflake-permissions
      

    

6. Set up an S3 bucket in AWS and configure the Airflow connection, including AWS credentials for S3 access.

      
      https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html
      

7. Spin up cosmos Airflow environment using `astro dev start` in the command line. This will create a Docker container with Airflow.     _Note: this requires a current version of Docker or Docker-Desktop. _
   

8. To access Airflow UI, go to 'localhost:8080' in your web browser. Once here, we will be able to add some key connections to Snowflake and AWS. In the Airflow UI, navigate to the Admin > Connections on the top task bar. 


   <img width="1417" alt="Screenshot 2024-09-18 at 6 36 37 PM" src="https://github.com/user-attachments/assets/86dca787-6075-4a1d-8d3c-38ec9797f590">


   -- (If using Snowflake) Set up Snowflake connection in Airflow.

      https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/connections/snowflake.html
         
   -- Setup an AWS connection in the Airflow UI as well. This is us securely passing our AWS credentials to Airflow.

      
      https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/aws.html
   
      
   <img width="1410" alt="Screenshot 2024-09-18 at 6 59 59 PM" src="https://github.com/user-attachments/assets/4ed5c0d2-05a1-4f1a-965b-d520fcbea065">

10. (If using Snowflake) Configure an S3 Storage Integration and Stage for your Snowflake account to access data in S3. This will also likely require you to manage IAM roles and policies on both Snowflake and AWS.


      https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration
      

## Running the Pipeline

The pipeline is designed to be run as an Airflow DAG. Once your Airflow environment is set up:

1. Make sure the `fda_adverse_events_dag.py` file is in your Airflow DAGs directory.
2. Make sure the dbt folder is in the Airflow DAGs directory.
3. The DAG should appear in the Airflow UI.
5. Trigger the DAG manually or wait for it to run based on the scheduled interval, currently @weekly. 

## Configuration

- Modify the list of drugs in `fda_tasks.py` if needed. In this example, we are using statins.
- Adjust the date range for data fetching in `fda_tasks.py` or allow the DAG to run on a schedule (currently @weekly).
- Update the S3 bucket name in `fda_tasks.py` to the name of the bucket you created in your AWS account.
- Confifure the dbt yml and all models to your desired spec. Key pieces will be ensuring consistent reference of the SCHEMA, DATABASE, and WAREHOUSE you configured in Snowflake (along with managing credentials).
- Please also configure the ProfileConfig and ExecutionConfig to fit your environment and setup. These are essentially instructions for dbt to create connections and execute models. For testing connections to Snowflake, you can also manually specify user, password, and account values:

       ```
      # The path to the dbt project
      DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/dbt"
      # The path where Cosmos will find the dbt executable
      # in the virtual environment created in the Dockerfile
      DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"

      profile_config = ProfileConfig(
          profile_name="default",
          target_name="dev",
          profile_mapping=SnowflakeUserPasswordProfileMapping(
              conn_id="snowflake_dbt",
              profile_args={"database": "DBT_DB",
                            "role": "DBT_ROLE",
                            "account": "",
                            "password": "",
                            "user": ""},
          ),
      )
      ```

## Testing

- dbt tests are defined in `models/schema.yml` and include data integrity checks like _not null_ and _unique_. In dbt terms, these are considered 'generic tests'.
- In addition, in the dbt/tests subdirectory, we have two 'singular tests': `ensure_weight_conversion_accurate.sql` and `test_is_valid_date.sql`

## Monitoring and Maintenance

- Use Airflow's built-in monitoring features to track DAG runs and task statuses.
- Regularly review dbt test results to ensure data quality.
- Monitor Snowflake query performance and optimize as needed.


## Future Enhancements

- Apply additional data quality checks to data loaded into Snowflake via dbt models.
- Integrate results with data visualization tools like Grafana, Tableau, dbt Studio, etc.

## Contact

Jarrod Wade - jarrod.wadej@gmail.com
