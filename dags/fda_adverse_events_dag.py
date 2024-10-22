from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from pendulum import datetime
import os

# Import your task functions
from tasks.fda_tasks import pull_adverse_events, save_and_upload_adverse_events

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

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

with DAG(
    dag_id="fda_adverse_events_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
) as dag:

    # Here we pull the Adverse Events data from the OpenFDA API
    adverse_events_data = pull_adverse_events()
    
    # Here we convert the data to CSV and upload it to S3
    upload_adverse_events = save_and_upload_adverse_events(adverse_events_data)

    # Here we create (if not exists) a table in Snowflake to load the data into
    create_table = SQLExecuteQueryOperator(
       task_id='create_adverse_events_raw_table',
       conn_id='snowflake_default',
       sql="""
       CREATE TABLE IF NOT EXISTS DBT_DB.DBT_SCHEMA.ADVERSE_EVENTS_RAW (
           safetyreportid VARCHAR(255),
           drug_name VARCHAR(255),
           medicinalproduct VARCHAR(255),
           drugcharacterization VARCHAR(50),
           drugindication VARCHAR(1000),
           reportercountry VARCHAR(255),
           qualification VARCHAR(255),
           reactionmeddrapt VARCHAR(1000),
           reactionoutcome VARCHAR(50),
           receiptdate DATE,
           receivedate DATE,
           transmissiondate DATE,
           patientonsetage FLOAT,
           patientonsetageunit VARCHAR(50),
           patientweight FLOAT,
           patientsex VARCHAR(10),
           serious VARCHAR(10),
           seriousnessdeath VARCHAR(10),
           seriousnesshospitalization VARCHAR(10),
           seriousnesslifethreatening VARCHAR(10),
           seriousnessdisabling VARCHAR(10),
           seriousnesscongenitalanomali VARCHAR(10),
           seriousnessother VARCHAR(10),
           occurcountry VARCHAR(255),
           primarysourcecountry VARCHAR(255),
           fulfillexpeditecriteria VARCHAR(10),
           reporttype VARCHAR(50),
           date_pulled DATE
       );
       """
    )

    load_to_snowflake = SQLExecuteQueryOperator(
        task_id='load_to_snowflake',
        conn_id='snowflake_default',
        sql="""
        COPY INTO DBT_DB.DBT_SCHEMA.ADVERSE_EVENTS_RAW
           FROM (
        SELECT 
            $1::VARCHAR(255) as safetyreportid,
            $2::VARCHAR(255) as drugname,
            $3::VARCHAR(255) as medicinalproduct,
            $4::VARCHAR(50) as drugcharacterization,
            $5::VARCHAR(1000) as drugindication,
            $6::VARCHAR(255) as reportercountry,
            $7::VARCHAR(255) as qualification,
            $8::VARCHAR(1000) as reactionmeddrapt,
            $9::VARCHAR(50) as reactionoutcome,
            TO_DATE($10, 'YYYY-MM-DD') as receiptdate,
            TO_DATE($11, 'YYYY-MM-DD') as receivedate,
            TO_DATE($12, 'YYYY-MM-DD') as transmissiondate,
            $13::FLOAT as patientonsetage,
            $14::VARCHAR(50) as patientonsetageunit,
            $15::FLOAT as patientweight,
            $16::VARCHAR(10) as patientsex,
            $17::VARCHAR(10) as serious,
            $18::VARCHAR(10) as seriousnessdeath,
            $19::VARCHAR(10) as seriousnesshospitalization,
            $20::VARCHAR(10) as seriousnesslifethreatening,
            $21::VARCHAR(10) as seriousnessdisabling,
            $22::VARCHAR(10) as seriousnesscongenitalanomali,
            $23::VARCHAR(10) as seriousnessother,
            $24::VARCHAR(255) as occurcountry,
            $25::VARCHAR(255) as primarysourcecountry,
            $26::VARCHAR(10) as fulfillexpeditecriteria,
            $27::VARCHAR(50) as reporttype,
            TO_DATE($28, 'YYYY-MM-DD') as datepulled
        FROM @ADV_DRUG_S3_STAGE
           )
           PATTERN = '.*adverse_events_[0-9]{8}_[0-9]{8}\\.csv'
           FORCE = FALSE;
       """
    )

    dbt_tasks = DbtTaskGroup(
        group_id="dbt_tasks",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
    )

    # Define the order of execution
    adverse_events_data >> upload_adverse_events >> create_table >> load_to_snowflake >> dbt_tasks