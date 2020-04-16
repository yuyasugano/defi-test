#
#  Copyright 2010-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#  This file is licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License. A copy of
#  the License is located at
# 
#  http://aws.amazon.com/apache2.0/
# 
#  This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
#  CONDITIONS OF ANY KIND, either express or implied. See the License for the
#  specific language governing permissions and limitations under the License.
#
from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import decimal
from decimal import Decimal
from boto3.dynamodb.conditions import Key, Attr

import pandas as pd

TABLE_NAME = 'Tokens'
TOKEN_LIST = ["Maker", "Compound", "Synthetix", "Uniswap", "Kyber", "dYdX"]

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

# Helper function to convert decimal object to float.
def decimal_to_float(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

# dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')
table = dynamodb.Table(TABLE_NAME)
df = pd.DataFrame()

def main():
    i = 0
    for token in TOKEN_LIST:
        try:
            datetime, tvl = [], []
            response = table.query(
                KeyConditionExpression=Key('name').eq(token)
            )
            print("Token {} has been extracted successfully.".format(token))
            # print(response['Items'])
            for item in response['Items']:
                datetime.append(item["datetime"])
                tvl.append(decimal_to_float(item["tvl"]["USD"]["value"]))

            if i == 0:
                df_temp = pd.DataFrame(tvl, columns=[token], index=datetime)
                df = df_temp
                print("DataFrame has been initialized successfully with", token)
            else:
                ret = pd.Series(tvl, index=datetime, name=token)
                # FutureWarning: Sorting because non-concatenation axis is not aligned.
                # A future version of pandas will change to not sort by default.
                df = pd.concat([df, ret], axis=1, sort=True)
            i = i + 1
        except Exception as e:
            print("Token id: {} extraction failed.".format(token))
            print("Stack Trace: {}".format(e))
            i = i + 1
            continue
    # df.dropna().info()
    df.index = pd.to_datetime(df.index)
    df.dropna().to_csv('data/to_csv_tokens.csv', header=True, index=True)

if __name__ == '__main__':
    main()

