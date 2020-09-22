import os
import json
import time
import random
import boto3
import datetime as dt
from io import StringIO
from faker import *

s3 = boto3.resource('s3')
# Write ventilator data to a S3 bucket
s3_bucket = 'rr-iad-analytics'
s3_load_prefix = 'dynamodb/input_data/'
filename = 'ventilator_data' +  '.json'

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
dynamodb_table = 'VentilatorLookup'

# Create a client with aws service and region
def create_client(service, region):
    return boto3.client(service, region_name=region)

def generate_ventilator_records(fake):
    '''
    Generates fake ventilator metrics
    '''
    for i in range(1,51):
     	yield { 'ventilatorid': i,
                'pressure_control': {
                    'min': fake.pyint(5,7),
                    'max': fake.pyint(29,30)
                },
                'o2_stats': {
                    'min': fake.pyint(92, 94),
                    'max': fake.pyint(96, 98)
                },
                'minute_volume': {
                    'min': fake.pyint(3, 5),
                    'max': fake.pyint(6, 8)
                }
    }

def dumps_lines(objs):
  for obj in objs:
  	yield json.dumps(obj, separators=(',',':')) + '\n'

def load_dynamodb_table(ventilator_json_data):
    # Load the data into DynamoDB
    table = dynamodb.Table(dynamodb_table)

    for ventilator in ventilator_json_data:
        ventilatorid = ventilator['ventilatorid']
        pressure_control = ventilator['pressure_control']
        o2_stats = ventilator['o2_stats']
        minute_volume = ventilator['minute_volume']

        print("Adding ventilator data:", ventilatorid)

        table.put_item(
           Item = {
               'ventilatorid': ventilatorid,
               'pressure_control_min': pressure_control['min'],
               'pressure_control_max': pressure_control['max'],
               'o2_stats_min': o2_stats['min'],
               'o2_stats_max': o2_stats['max'],
               'minute_volume_min': minute_volume['min'],
               'minute_volume_max': minute_volume['max'],
            }
        )

# main function
def main():
    # Generates ventilator data
    fake = Faker()
    fakeIO = StringIO()
    fake_ventilator_records = generate_ventilator_records(fake)

    fakeIO.write(str(''.join(dumps_lines(fake_ventilator_records))))

    # Write the json file to S3
    s3key = s3_load_prefix + filename
    s3object = s3.Object(s3_bucket, s3key)
    s3object.put(Body=(bytes(fakeIO.getvalue().encode('utf-8'))))
    fakeIO.close()

    time.sleep(5)

    ventilator_object= s3.Object(s3_bucket, s3key)
    ventilator_decoded_data = ventilator_object.get()['Body'].read().decode('utf-8')

    ventilator_stringio_data = StringIO(ventilator_decoded_data)

    # Read data line by line
    data = ventilator_stringio_data.readlines()

    # Deserialize json data
    ventilator_json_data = list(map(json.loads, data))

    # Load DDB table
    load_dynamodb_table(ventilator_json_data)

if __name__ == "__main__":
    # run main
    main()
