import sys
import os
import boto3
import multiprocessing


# Uncomment this section for after AWS has updated total item count 

# new_table = sys.argv[1]

# region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')

# iam_role = boto3.session.Session(profile_name='intern')
# dynamodb = iam_role.resource('dynamodb', region_name=region)
# table = dynamodb.Table(new_table)

# print table.item_count

def scan_table(src_table,client,segment,total_segments):
    item_count = 0
    paginator = client.get_paginator('scan')

    for page in paginator.paginate(
            TableName=src_table,
            Select='ALL_ATTRIBUTES',
            ReturnConsumedCapacity='NONE',
            ConsistentRead=True,
            Segment=segment,
            TotalSegments=total_segments,
            PaginationConfig={"PageSize": 500}):

        for item in page['Items']:
            item_count += 1

    print "Process %d counted %d items" % (segment,item_count)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print 'Usage: %s <source_table_name>' % sys.argv[0]
        sys.exit(1)

    table_1 = sys.argv[1]
    region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')

    iam_role = boto3.session.Session(profile_name='intern')
    db_client = iam_role.client('dynamodb')

    pool_size = 4
    pool = []

    for i in range(pool_size):
        worker = multiprocessing.Process(
            target=scan_table,
            kwargs={
                'src_table':table_1,
                'client': db_client,
                'segment': i,
                'total_segments': pool_size
            }
        )
        pool.append(worker)
        worker.start()


