## What we do
 
- configure aws
```
aws configure
```
- create a dynamodb table
```
python TokensCreateTable.py 
```
- Invoke a query and get data under data directory
```
python TokensQueryTable.py
```
 
### Lambda function setup
 
AWS Lambda function can be set up to call defipulse api and collect data in json.
  
* __IAM__: create an iam policy that has access on dynamodb and set it on a role
* __Lambda__: Upload zipped lambda\_function directory and set the prepared role
* __CloudWatch__: Configure CloudWatch Events and run it periodically, every minute
 
