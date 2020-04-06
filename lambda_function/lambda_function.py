#!/usr/bin/python
import json
import boto3
import decimal
import datetime
import requests

TABLE_NAME = 'Tokens'
headers = {'Content-Type': 'application/json'}
api_url_base = "https://public.defipulse.com/api/GetProjects"
api_url = '{}'.format(api_url_base)

dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
table = dynamodb.Table(TABLE_NAME)

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if abs(o) % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecicmalEncoder, self).default(o)

# Helper function to convert dict to json format with decimal parse
def float_to_decimal(value):
    return json.loads(json.dumps(value), parse_float=decimal.Decimal)

def defipulse():
    try:
        response = requests.get(api_url, headers=headers)
    except Exception as e:
        print("An error occurred:", e.args)

    if response.status_code == 200:
        res = json.loads(response.content.decode('utf-8'))
        return res
    else:
        return None

def lambda_handler(event, context):
    response = defipulse()
    if response is None:
        return {
            'statusCode': 200,
            'body': json.dumps('Calling API did not succeed')
        }
    else:
        for res in response:
            dt = datetime.datetime.fromtimestamp(res['timestamp'])
            try:
                response = table.put_item(
                    Item={
                        'name': res['name'],
                        'datetime': dt.strftime("%Y/%m/%d %H:%M:%S"),
                        'tvl': float_to_decimal(res['value']['tvl']),
                        'total': float_to_decimal(res['value']['total']),
                        'balance': float_to_decimal(res['value']['balance'])
                    }
                )
                print("PutItem succeeded:")
                print(json.dumps(res, indent=4, cls=DecimalEncoder))
            except Exception as e:
                print("PutItem failed:", e.args)
                continue

        return {
            'statusCode': 200,
            'body': json.dumps('Calling API succeeded')
        }
