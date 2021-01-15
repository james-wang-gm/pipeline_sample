# Testing Pipeline - GCS to BigQuery
import logging
import json
import argparse
import sys
import pandas as pd

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam import Create, Map, ParDo, Flatten, Partition
from apache_beam import pvalue
import apache_beam.io.gcp.gcsio
import apache_beam.io.gcp.bigquery
from google.cloud import bigquery
import google.auth
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import matches_all, equal_to
from apache_beam.io import ReadFromText, ReadAllFromText
from apache_beam.io import WriteToText

from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
import apache_beam.runners.interactive.interactive_beam as ib

logging.getLogger().setLevel(logging.WARNING)

%pip install sympy
import sympy

!gcloud services enable bigquery
!gcloud services enable dataflow

PROJECT = "lunar-airport-298818.sample.testing"
BUCKET = 'lunar-airport-298818-sample-pipeline'
DATASET = 'lunar-airport-298818:sample'
defaultInputFile = 'gs://{0}/Sample_Data/majestic_million.csv'.format(BUCKET)
TLDFile = 'gs://{0}/Sample_Data/tlds.csv'.format(BUCKET)
excludedTLDFile = 'gs://{0}/Sample_Data/excludes.csv'.format(BUCKET)


def run(argv=None):

    pipeline_args =[
        '--project={0}'.format(PROJECT),
        '--job_name=sample_pipeline',
        '--save_main_session',
        '--staging_location=gs://{0}/staging/'.format(BUCKET),
        '--temp_location=gs://{0}/temp/'.format(BUCKET),
        '--num_workers=4',
        '--runner=DataflowRunner',
        #'--inputFile=gs://{0}/Sample_Data/majestic_million.csv'.format(BUCKET),
        #'--template_location=gs://{0}/templates/majestic_million_template'.format(BUCKET),
        #'--zone=australia-southeast1-a'
        '--region=us-east1'
        ]
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    inbound_options = pipeline_options.view_as(FileLoader)
    input = inbound_options.inputFile

    with beam.Pipeline(options=pipeline_options) as p:
        donuts = p |'read' >> ReadFromText('gs://lunar-airport-298818-sample-pipeline/donuts/donuts.json') | "PARSE JSON" >> beam.Map(json.loads) | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
           table = 'lunar-airport-298818:sample.testing',
           schema='SCHEMA_AUTODETECT',
           create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
           write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
           custom_gcs_temp_location = 'gs://lunar-airport-298818-sample-pipeline/donuts/donuts.json')
    p.run()


if __name__ == '__main__':
  run()










