#!/usr/bin/python
import json
import boto3
import decimal
from decimal import Decimal
from boto3.dynamodb.conditions import Key, Attr

TABLE = 'Tokens'

dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
table = dynamodb.Table(TABLE_NAME)

# Helper class to convert a DynamoDB item to JSON
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if abs(o) % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

# Helper function to convert decimal object to float
def decimal_to_float(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def get_lastprice(name):
    try:
        response = table.query(
            KeyConditionExpression=Key('name').eq(name)
        )
    except ClientError as e:
        print(e.response['Error']['Message']
    else:
        item = max(response['Items'], key=(lambda x: x['datetime']))
        record = decimal_to_float(item['tvl']['USD']['value']
        print('Latest record for {}: {}'.format(name, record))
        return record

def lambda_handler(event, context):
    if event['queryStringParameters'] is None:
        return {
            'isBase64Encoded': False,
            'statusCode': 500,
            'headers': {},
            'body': json.dumps('Internal server error')
        }
    try:
        message = get_lastprice(event['queryStringParameters']['name'])
    except Exception as e:
        print(e.arps)
        return {
            'isBase64Encoded': False,
            'statusCode': 400,
            'headers': {},
            'body': json.dumps('Bad request')
        }
    else:
        return {
            'isBase64Encoded': False,
            'statusCode': 200,
            'headers': {},
            'body': json.dumps(message, cls=DecimalEncoder)
        }
