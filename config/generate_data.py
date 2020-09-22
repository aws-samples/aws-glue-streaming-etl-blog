import os
import io
import sys
import json
import random
import boto3
import argparse
import datetime as dt
from faker import *



# Create a client with aws service and region
def create_client(service, region):
	return boto3.client(service, region_name=region)

class RecordGenerator(object):
	'''
	A class used to generate ventilator data used as input for Glue Streaming ETL.
	'''
	def __init__(self):
	  self.ventilatorid = 0
	  self.eventtime = None
	  self.serialnumber = ""
	  self.pressurecontrol = 0
	  self.o2stats = 0
	  self.minutevolume = 0
	  self.manufacturer = None

	def get_ventilator_record(self, fake):
		'''
		Generates fake ventilator metrics
		'''
		record = {'ventilatorid': fake.pyint(min_value=1, max_value=50),
			'eventtime': fake.date_time_between(start_date='-10m', end_date='now').isoformat(),
			'serialnumber': fake.uuid4(),
			'pressurecontrol': fake.pyint(min_value=3, max_value=40),
			'o2stats': fake.pyint(min_value=90, max_value=100),
			'minutevolume': fake.pyint(min_value=2, max_value=10),
			'manufacturer': random.choice(['3M', 'GE', 'Vyaire', 'Getinge'])
			}
		data = json.dumps(record)
		return {'Data': bytes(data, 'utf-8'), 'PartitionKey': 'partition_key'}

	def get_ventilator_records(self, rate, fake):
		return [self.get_ventilator_record(fake) for _ in range(rate)]

	def dumps_lines(objs):
	  for obj in objs:
	  	yield json.dumps(obj, separators=(',',':')) + '\n'


# main function
def main():

    parser = argparse.ArgumentParser(description='Faker based streaming data generator')

    parser.add_argument('--streamname', action='store', dest='stream_name', help='Provide Kinesis Data Stream name to stream data')
    parser.add_argument('--region', action='store', dest='region', default='us-east-1')

    args = parser.parse_args()

    #print (args)
    # Make sure to set your profile here
    session = boto3.Session(profile_name='ravirala-acct1')

    try:
    	# Intialize Faker library
    	fake = Faker()

    	# Kinesis settings
    	kinesis_client = session.client('kinesis', args.region)

    	# Rate at which records are generated
    	rate = 500
    	generator = RecordGenerator()

    	# Generates ventilator data
    	while True:
    		fake_ventilator_records = generator.get_ventilator_records(rate, fake)
    		#print (fake_ventilator_records)
    		kinesis_client.put_records(StreamName=args.stream_name, Records=fake_ventilator_records)
        #fakeIO = StringIO()
        #fakeIO.write(str(''.join(dumps_lines(fake_ventilator_records))))
        #fakeIO.close()

    except:
        print("Error:", sys.exc_info()[0])
        raise

if __name__ == "__main__":
	# run main
	main()
