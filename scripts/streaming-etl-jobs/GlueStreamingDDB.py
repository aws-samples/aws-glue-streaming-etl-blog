import sys
import datetime
import base64
import decimal
import boto3
from pyspark.sql import DataFrame, Row
from pyspark.context import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, \
                            ['JOB_NAME', \
                            'aws_region', \
                            'checkpoint_location', \
                            'dynamodb_sink_table', \
                            'dynamodb_static_table'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read parameters
checkpoint_location = args['checkpoint_location']
aws_region = args['aws_region']

# DynamoDB config
dynamodb_sink_table = args['dynamodb_sink_table']
dynamodb_static_table = args['dynamodb_static_table']

def write_to_dynamodb(row):
    '''
    Add row to DynamoDB.
    '''
    dynamodb = boto3.resource('dynamodb', region_name=aws_region)
    start = str(row['window'].start)
    end = str(row['window'].end)
    dynamodb.Table(dynamodb_sink_table).put_item(
      Item = { 'ventilatorid': row['ventilatorid'], \
                'status': str(row['status']), \
                'start': start, \
                'end': end, \
                'avg_o2stats': decimal.Decimal(str(row['avg_o2stats'])), \
                'avg_pressurecontrol': decimal.Decimal(str(row['avg_pressurecontrol'])), \
                'avg_minutevolume': decimal.Decimal(str(row['avg_minutevolume']))})

#

dynamodb_dynamic_frame = glueContext.create_dynamic_frame.from_options( \
    "dynamodb", \
    connection_options={
    "dynamodb.input.tableName": dynamodb_static_table,
    "dynamodb.throughput.read.percent": "1.5"
  }
)

dynamodb_lookup_df = dynamodb_dynamic_frame.toDF().cache()

# Read from Kinesis Data Stream
streaming_data = spark.readStream \
                    .format("kinesis") \
                    .option("streamName","glue_ventilator_stream") \
                    .option("endpointUrl", "https://kinesis.us-east-1.amazonaws.com") \
                    .option("startingPosition", "TRIM_HORIZON") \
                    .load()

# Retrieve Sensor columns and simple projection
ventilator_fields = streaming_data \
    .select(from_json(col("data") \
    .cast("string"),glueContext.get_catalog_schema_as_spark_schema("ventilatordb","ventilators_table")) \
    .alias("ventilatordata")) \
    .select("ventilatordata.*") \
    .withColumn("event_time", to_timestamp(col('eventtime'), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("ts", to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))

# Stream static join, ETL to augment with status
ventilator_joined_df = ventilator_fields.join(dynamodb_lookup_df, "ventilatorid") \
    .withColumn('status', when( \
    ((ventilator_fields.o2stats < dynamodb_lookup_df.o2_stats_min) | \
    (ventilator_fields.o2stats > dynamodb_lookup_df.o2_stats_max)) & \
    ((ventilator_fields.pressurecontrol < dynamodb_lookup_df.pressure_control_min) | \
    (ventilator_fields.pressurecontrol > dynamodb_lookup_df.pressure_control_max)) & \
    ((ventilator_fields.minutevolume < dynamodb_lookup_df.minute_volume_min) | \
    (ventilator_fields.minutevolume > dynamodb_lookup_df.minute_volume_max)), "RED") \
    .when( \
    ((ventilator_fields.o2stats >= dynamodb_lookup_df.o2_stats_min) |
    (ventilator_fields.o2stats <= dynamodb_lookup_df.o2_stats_max)) & \
    ((ventilator_fields.pressurecontrol >= dynamodb_lookup_df.pressure_control_min) | \
    (ventilator_fields.pressurecontrol <= dynamodb_lookup_df.pressure_control_max)) & \
    ((ventilator_fields.minutevolume >= dynamodb_lookup_df.minute_volume_min) | \
    (ventilator_fields.minutevolume <= dynamodb_lookup_df.minute_volume_max)), "GREEN") \
    .otherwise("ORANGE"))

ventilator_joined_df.printSchema()

# Drop the normal metric values
ventilator_transformed_df = ventilator_joined_df \
                            .drop('eventtime', 'o2_stats_min', 'o2_stats_max', \
                            'pressure_control_min', 'pressure_control_max', \
                            'minute_volume_min', 'minute_volume_max')

ventilator_transformed_df.printSchema()

ventilators_df = ventilator_transformed_df \
    .groupBy(window(col('ts'), '10 minute', '5 minute'), \
    ventilator_transformed_df.status, ventilator_transformed_df.ventilatorid) \
    .agg( \
    avg(col('o2stats')).alias('avg_o2stats'), \
    avg(col('pressurecontrol')).alias('avg_pressurecontrol'), \
    avg(col('minutevolume')).alias('avg_minutevolume') \
    )

ventilators_df.printSchema()

# Write to DynamoDB sink
ventilator_query = ventilators_df \
    .writeStream \
    .foreach(write_to_dynamodb) \
    .outputMode("update") \
    .option("checkpointLocation", checkpoint_location) \
    .start()

ventilator_query.awaitTermination()

job.commit()
