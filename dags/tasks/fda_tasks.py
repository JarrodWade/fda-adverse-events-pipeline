# Import necessary libraries and modules
from airflow.decorators import task
import requests
import logging
import pendulum
import json
import pandas as pd
import boto3
from io import StringIO
from typing import List, Dict, Tuple
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from urllib.parse import quote

def get_adverse_events(drug_name: str, start_date: str, end_date: str, skip: int = 0) -> Dict:
    """
    Fetch adverse events data from the FDA API for a specific drug and date range.
    
    :param drug_name: Name of the drug to fetch data for
    :param start_date: Start date for the data range (format: YYYYMMDD)
    :param end_date: End date for the data range (format: YYYYMMDD)
    :param skip: Number of records to skip (for pagination)
    :return: JSON response from the API or None if the request fails
    """
    logging.info(f"get_adverse_events called with dates: {start_date} to {end_date}")
    
    encoded_drug_name = quote(f'"{drug_name}"')
    url = f"https://api.fda.gov/drug/event.json?search=patient.drug.medicinalproduct:{encoded_drug_name}+AND+receivedate:[{start_date}+TO+{end_date}]&limit=100&skip={skip}"
    
    logging.info(f"Requesting URL: {url}")
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        logging.info(f"HTTP error occurred: {http_err}, this means the data is not available in the API for the date range... moving on")
        logging.info(f"Response content: {response.text}")
    except json.JSONDecodeError as json_err:
        logging.error(f"JSON Decode error: {json_err}")
        logging.error(f"Response content: {response.text}")
    except requests.exceptions.RequestException as err:
        logging.error(f"An error occurred: {err}")
    
    logging.warning(f"Request failed for {drug_name} with status code {response.status_code}. Skip: {skip}")
    return None

@task
def pull_adverse_events(**context):
    
    execution_date = execution_date = context['execution_date']
    
    statins = ["atorvastatin", "rosuvastatin", "simvastatin", "pravastatin", "lovastatin"]
    
    # Calculate the date range for two months ago
    # end_date = execution_date.subtract(months=12)  # Start from 2 months ago
    # start_date = end_date.subtract(days=6)  # Go back 7 days from there

 # Uncomment these lines if you want to use fixed dates for testing 
    # or historical data    
    start_date_str = '20230101'
    end_date_str = '20230731'
    
    # Format dates for FDA API
    # start_date_str = start_date.format('YYYYMMDD')
    # end_date_str = end_date.format('YYYYMMDD')
    
    logging.info(f"Current date: {pendulum.now().format('YYYYMMDD')}")
    logging.info(f"Fetching data from {start_date_str} to {end_date_str}")
    
    adverse_event_data = []

    for statin in statins:
        skip = 0
        while True:
            data = get_adverse_events(statin, start_date_str, end_date_str, skip)
            if not data or 'results' not in data:
                break
            for event in data['results']:
                
                # Here we break down the data structure to get the relevant information
                # key pieces here are default values in case of missing data, and the use of the 'get' method
                # to safely navigate the potentially nested data structure.
                patient = event.get('patient', {})
                drug = patient.get('drug', [{}])[0] if patient.get('drug') else {}
                reaction = patient.get('reaction', [{}])[0] if patient.get('reaction') else {}
                primary_source = event.get('primarysource') or {}

                # Extract relevant information from each event, using the 'get' 
                # method to safely navigate the potentially nested data structure.
                adverse_event = {
                    'safetyreportid': event.get('safetyreportid'),
                    'drug_name': statin.capitalize(),
                    'medicinalproduct': drug.get('medicinalproduct'),
                    'drugcharacterization': drug.get('drugcharacterization'),
                    'drugindication': drug.get('drugindication'),
                    'reportercountry': primary_source.get('reportercountry'),
                    'qualification': primary_source.get('qualification'),
                    'reactionmeddrapt': reaction.get('reactionmeddrapt'),
                    'reactionoutcome': reaction.get('reactionoutcome'),
                    'receiptdate': event.get('receiptdate'),
                    'receivedate': event.get('receivedate'),
                    'transmissiondate': event.get('transmissiondate'),
                    'patientonsetage': patient.get('patientonsetage'),
                    'patientonsetageunit': patient.get('patientonsetageunit'),
                    'patientweight': patient.get('patientweight'),
                    'patientsex': patient.get('patientsex'),
                    'serious': event.get('serious'),
                    'seriousnessdeath': event.get('seriousnessdeath'),
                    'seriousnesshospitalization': event.get('seriousnesshospitalization'),
                    'seriousnesslifethreatening': event.get('seriousnesslifethreatening'),
                    'seriousnessdisabling': event.get('seriousnessdisabling'),
                    'seriousnesscongenitalanomali': event.get('seriousnesscongenitalanomali'),
                    'seriousnessother': event.get('seriousnessother'),
                    'occurcountry': event.get('occurcountry'),
                    'primarysourcecountry': event.get('primarysourcecountry'),
                    'fulfillexpeditecriteria': event.get('fulfillexpeditecriteria'),
                    'reporttype': event.get('reporttype'),
                    'date_pulled': pendulum.now().format('YYYY-MM-DD')
                }
                adverse_event_data.append(adverse_event)

            skip += 100

    logging.info(f"Retrieved {len(adverse_event_data)} adverse events")
    return adverse_event_data, start_date_str, end_date_str



def save_to_csv_and_upload(data: List[Dict], filename: str, start_date: str, end_date: str, bucket_name: str = 'adversedrugs') -> None:    
    """
    Save data to a CSV file and upload it to an S3 bucket.
    
    :param data: List of dictionaries containing the data to be saved
    :param filename: Name of the file (without extension)
    :param bucket_name: Name of the S3 bucket to upload to
    """
    df = pd.DataFrame(data)

    # Convert date columns to datetime
    date_columns = ['receiptdate', 'receivedate', 'transmissiondate']
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], format='%Y%m%d', errors='coerce')
    

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    # Parse the start and end dates
    start = pendulum.parse(start_date)
    end = pendulum.parse(end_date)
    
    # Format the filename with the date range
    date_range = f"{start.format('YYYYMMDD')}_{end.format('YYYYMMDD')}"
    
    # Construct the S3 key with the date range in both the path and filename
    s3_key = f"raw/adverse_events/{start.year}/{start.format('MM')}/{start.format('DD')}/{filename}_{date_range}.csv"

    try:
        s3_hook = S3Hook(aws_conn_id='aws_default')

        # Check if file already exists
        if s3_hook.check_for_key(s3_key, bucket_name):
            logging.info(f"File {s3_key} already exists. Skipping upload.")
            return
    
        s3_hook.load_string(
            string_data=csv_buffer.getvalue(),
            key=s3_key,
            bucket_name=bucket_name,
            replace=False # We check for existence above, so we don't need to replace
        )
        logging.info(f"Successfully uploaded {s3_key} to S3 bucket {bucket_name}")
    except Exception as e:
        logging.error(f"Failed to upload {s3_key} to S3. Error: {str(e)}")
        raise
    
    
# Decent chance that this will be reused for other uploads (ndc data for example), so to enforce some granularity
# and to not have a monolithic upload function, let's create a task for each upload
@task
def save_and_upload_adverse_events(adverse_events_data_and_dates: Tuple[List[Dict], str, str]) -> None:
    adverse_events_data, start_date, end_date = adverse_events_data_and_dates
    save_to_csv_and_upload(adverse_events_data, 'adverse_events', start_date, end_date, 'adversedrugs')