from __future__ import print_function # Python 2/3 compatibility
import boto3

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

ventilator_lookup_table = dynamodb.create_table(
    TableName='VentilatorLookup',
    KeySchema=[
        {
            'AttributeName': 'ventilatorid',
            'KeyType': 'HASH'  #Sort key
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'ventilatorid',
            'AttributeType': 'N'
        },

    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 500,
        'WriteCapacityUnits': 5
    }
)

ventilator_write_table = dynamodb.create_table(
    TableName = "VentilatorMetrics",
    KeySchema=[
        {
            'AttributeName': 'ventilatorid',
            'KeyType': 'HASH'  #Sort key
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'ventilatorid',
            'AttributeType': 'N'
        },

    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 500
    }
)
print("Table status:", ventilator_lookup_table.table_status)
