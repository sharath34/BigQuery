from google.cloud import bigquery
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "***Use key location***"

client = bigquery.Client()
dataset_id = 'Test_Dataset'

job_config = bigquery.QueryJobConfig()
table_ref = client.dataset(dataset_id).table('Results')
job_config.destination = table_ref

#THIS PART WRITES THE DATA INTO NEW tABLE
job_config.write_disposition = 'WRITE_TRUNCATE'
res_query = 'select A.user_id,A.cached,A.timestamp,A.source_uri,A.track_id,A.source,A.length,A.version,A.device_type,A.message,A.os,A.stream_country,A.report_date,B.isrc,B.album_code,C.product,C.country,C.region,C.zip_code,C.access,C.gender,C.partner,C.referral,C.type,C.birth_year From Test_Dataset.Streams A join Test_Dataset.Tracks B ON A.track_id = B.track_id join Test_Dataset.Users C ON A.user_id = C.user_id Limit 10;'
# Start the query, passing in the extra configuration.
query_job = client.query(res_query, job_config=job_config)

rows = list(query_job)  # Waits for the query to finish

# Now Writing the Results into a file

Proj_name = 'profound-veld-202319'
destination_uri = 'gs://test-buck1/Results.csv'
#destination_uri = 'gs://{}/{}'.format('test-buck1', 'Results.txt')
dataset_ref = client.dataset(dataset_id, project=Proj_name)
table_ref = dataset_ref.table('Results')

extract_job = client.extract_table(
    table_ref, destination_uri)
extract_job.result()