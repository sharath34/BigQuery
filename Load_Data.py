from google.cloud import bigquery
from bigquery import get_client
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\Users\Sharath\Downloads\key.json"

bigquery_client = bigquery.Client()

#Loading data into Table
dataset_id = 'Test_Dataset'
source_file_name = 'streams.json'
t = 'Streams'
source_file_name1 = 'users.json'
t1 = 'Users'
source_file_name2 = 'tracks.json'
t2 = 'Tracks'

def load_data_from_file(dataset_id, table_id, source_file_name):
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    with open(source_file_name, 'rb') as source_file:
        # This example uses CSV, but you can use other formats.
        # See https://cloud.google.com/bigquery/loading-data
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = 'NEWLINE_DELIMITED_JSON'
        job_config.autodetect = True
        job = bigquery_client.load_table_from_file(
            source_file, table_ref, job_config=job_config)

    job.result()  # Waits for job to complete

    print('Loaded {} rows into {}:{}.'.format(
        job.output_rows, dataset_id, table_id))

load_data_from_file(dataset_id, t, source_file_name)
load_data_from_file(dataset_id, t1, source_file_name1)
load_data_from_file(dataset_id, t2, source_file_name2)