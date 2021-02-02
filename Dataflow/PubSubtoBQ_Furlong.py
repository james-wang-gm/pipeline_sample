#Pub/Sub to BQ pipeline

#Import Packages
import logging
import json
import time
import traceback
import re

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options import pipeline_options
from apache_beam.io.gcp.pubsub import ReadFromPubSub, ReadStringsFromPubSub
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json

from apache_beam.runners import DataflowRunner
from apache_beam.options.pipeline_options import GoogleCloudOptions

import google.auth

class CustomPipelineOptions(PipelineOptions):
    """
    Runtime Parameters given during template execution
    path and organization parameters are necessary for execution of pipeline
    campaign is optional for committing to bigquery
    """
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--path',
            type=str,
            help='Path of the file to read from',
            default = 'dev-analytics-data-lake/testing/family.json')
        parser.add_value_provider_argument(
            '--output',
            type=str,
            help='Output file if needed')

project = 'furlong-platform-sbx-8d14f3'

#Pipeline Logic
def streaming_pipeline(project, region="us-east1"):
    
    topic = "projects/furlong-platform-sbx-8d14f3/topics/Analytics-Dummy"
    schema = 'id:int, father:string, mother:string, children:string'
    table = "furlong-platform-sbx-8d14f3:Analytics_Testing.family"
    bucket = "gs://dev-analytics-temp-files"
    subscription = 'projects/furlong-platform-sbx-8d14f3/subscriptions/AnalyticsDummySub'
    
    options = PipelineOptions(
        streaming=True,
        project=project,
        region=region,
        # Make sure staging and temp folder are created using cloud commands
        staging_location="gs://dev-analytics-temp-files/staging",
        temp_location="%s/temp" % bucket,
        template_location = 'gs://dev-analytics-temp-files',
        autoscaling_algorithm = 'THROUGHPUT_BASED',
        max_num_workers = 3 
    )

    p = beam.Pipeline(DataflowRunner(), options=options)

    # Can either use subscription or topic (preferably subscription)
    fam = (p | "Read Topic" >> ReadFromPubSub(topic = topic)
             | 'Parse JSON to Dict' >> beam.Map(json.loads) # Example message: {"name": "carlos", 'score': 10, 'timestamp': "2020-03-14 17:29:00.00000"}
             | "window" >> beam.WindowInto(beam.window.FixedWindows(5))
             | "Write to BQ" >> WriteToBigQuery(table=table, 
                                  # Could potentially use Schema detector 
                                  schema = schema,
                                  create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                                  write_disposition=BigQueryDisposition.WRITE_APPEND))

    return p.run()

try:
    pipeline = streaming_pipeline(project)
    print("\n PIPELINE RUNNING \n")
except (KeyboardInterrupt, SystemExit):
    raise
except:
    print("\n PIPELINE FAILED")
    traceback.print_exc()

