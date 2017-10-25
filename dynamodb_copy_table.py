import sys
import os
from time import sleep
import boto3
import json

if len(sys.argv) != 3:
    print 'Usage: %s <source_table_name>' \
        ' <destination_table_name>' % sys.argv[0]
    sys.exit(1)

src_table = sys.argv[1]
dst_table = sys.argv[2]
region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')

iam_role = boto3.session.Session(profile_name='intern')
client = iam_role.client('dynamodb')
dynamodb = iam_role.resource('dynamodb', region_name = region)

#get source table and its schema
table_schema = None
try:
    table = dynamodb.Table(src_table)
    table_schema = client.describe_table(TableName=src_table)["Table"]
except client.exceptions.ResourceNotFoundException:
    print "!!! Table %s does not exist. Exiting..." % src_table
    sys.exit(1)

print '*** Reading key schema from %s table' % src_table

#create keyword args for copy able
keyword_args = {"TableName":dst_table}

keyword_args['AttributeDefinitions'] = table_schema['AttributeDefinitions']
keyword_args['KeySchema'] = table_schema['KeySchema']

globalSecondaryIndexes = []
localSecondaryIndexes = []

if table_schema.get("GlobalSecondaryIndexes"):
    for item in table_schema["GlobalSecondaryIndexes"]:
        index = {}
        for k,v in item.iteritems():
            if k in ["IndexName","KeySchema","Projection","ProvisionedThroughput"]:
                if k == "ProvisionedThroughput":
                    # for key in v.keys():
                    #     if key not in ["ReadCapacityUnits", "WriteCapacityUnits"]:
                    #         del v[key]
                    index[k] = {"ReadCapacityUnits": 3, "WriteCapacityUnits":100}
                    continue
                index[k] = v
        globalSecondaryIndexes.append(index)

if table_schema.get("LocalSecondaryIndexes"):
    for item in table_schema["LocalSecondaryIndexes"]:
        index = {}
        for k,v in item.iteritems():
            if k in ["IndexName","KeySchema","Projection"]:
                index[k] = v
        localSecondaryIndexes.append(index)

if globalSecondaryIndexes:
    keyword_args["GlobalSecondaryIndexes"] = globalSecondaryIndexes
if localSecondaryIndexes:
    keyword_args["LocalSecondaryIndexes"] = localSecondaryIndexes

# provisionedThroughput = table_schema['ProvisionedThroughput']
# for key in provisionedThroughput.keys():
#     if key not in ["ReadCapacityUnits", "WriteCapacityUnits"]:
#         del provisionedThroughput[key]

# keyword_args["ProvisionedThroughput"] = provisionedThroughput

keyword_args["ProvisionedThroughput"] = {"ReadCapacityUnits": 3, "WriteCapacityUnits":100}

if table_schema.get('StreamSpecification'):
    keyword_args['SteamSpecification'] = table_schema['StreamSpecification']

#create copy table
table_schema = None
try:
    table_schema = client.describe_table(TableName=dst_table)
    print '!!! Table %s already exists. Exiting...' % dst_table
    sys.exit(0)
except client.exceptions.ResourceNotFoundException:
    
    table_schema= client.create_table(**keyword_args)

    print '*** Waiting for the new table %s to become active' % dst_table
    sleep(5)
    while client.describe_table(TableName = dst_table)['Table']['TableStatus'] != 'ACTIVE':
        sleep(3)
    print '*** New table %s to is now active!' % dst_table


#copy over item
paginator = client.get_paginator('scan')

response = paginator.paginate(
    TableName=src_table,
    Select='ALL_ATTRIBUTES',
    ReturnConsumedCapacity='NONE',
    ConsistentRead=True
)

for page in response:
    for item in page['Items']:
        client.put_item(
            TableName = dst_table,
            Item = item
            )

print '*** Done. Exiting...'
