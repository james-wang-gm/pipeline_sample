#Objective: Trigger cloud function to run SQL when d

import base64
from google.cloud import bigquery
from google.cloud import storage

def read_sql(gcs_client, bucket_id):
    bucket = gcs_client.get_bucket(bucket_id)
    blob = bucket.get_blob("testing.sql")
    contents = blob.download_as_string()
    return contents.decode('utf-8')

project = 'lunar-airport-298818'

def execute_sql_scripts(request):
    client = bigquery.Client(project=project)
    # Read query from GCS bucket.
    gcs_client = storage.Client(project)
    query = read_sql(gcs_client,'lunar-airport-298818-sample-pipeline')
    job = client.query(query)
    result = job.result()
