from google.cloud import bigquery
from bigquery import get_client
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\Users\Sharath\Downloads\key.json"

bigquery_client = bigquery.Client()

# Creating Dataset
dataset_id = 'Test_Dataset1'
dataset_ref = bigquery_client.dataset(dataset_id)
dataset = bigquery.Dataset(dataset_ref)
dataset = bigquery_client.create_dataset(dataset)

print('Dataset {} created.'.format(dataset.dataset_id))

dataset_ref = bigquery_client.dataset(dataset_id)

#Create Empty Table

def Create_Table(table_name):
    table_ref = dataset_ref.table(table_name)
    table = bigquery.Table(table_ref)
    table = bigquery_client.create_table(table)
    print 'Table Created '+table_name

Create_Table('Streams')
Create_Table('Users')
Create_Table('Tracks')