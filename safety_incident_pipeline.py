import json
import os
import time
import boto3
import csv
import io

# blame Thomas "Reid" Harrison
os.environ['TZ'] = 'US/Pacific'
time.tzset()

#aws_access_key_id = os.environ['ID']
#aws_secret_access_key = os.environ['KEY']

def save_json(data):
    # Save data as a JSON file in S3
    #s3 = boto3.resource('s3',region_name='us-east-1',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)
    s3 = boto3.resource('s3',region_name='us-east-1')
    key_folder = time.strftime("%Y%m%d") + "/error/"
    resource_name = key_folder +'error-' + time.strftime("%Y%m%d-%H%M%S") + '.txt'
    s3object = s3.Object('coldt-dump',resource_name)
    try:
        s3object.put(
            Body=data, ContentType='text/plain'
        )
    except Exception as e:
        print(e)

def lambda_handler(event, context):
    csvio = io.StringIO()
    writer = csv.writer(csvio)
    writer.writerow(['warehouse_id','timestamp','metric','value','type'])

    queueName = 'bucket-atv'
    region = 'us-east-1'
    max_queue = 10
    max_loop = 1000
    msg = []
    #sqs = boto3.resource('sqs',region_name=region,aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)
    sqs = boto3.resource('sqs',region_name=region)
    queue = sqs.get_queue_by_name(QueueName=queueName)
    process_queue = False

    for i in range(max_loop):
        msg_to_delete = []
        for record in queue.receive_messages(MaxNumberOfMessages = max_queue):
            # Process each message in the queue
            #print(json.loads(record.body))
            process_queue = True
            row = []
            type_metric = None
            body = json.loads(record.body)
            try:
                data = json.loads(body['Message'])
                msg.append(data)
                type_metric = list(data)[3]
                #print(type_metric)
                for key, value in data.items():
                    row.append(value)

                if row:
                    row.append(type_metric)
                    writer.writerow(row)

                msg_to_delete.append({
                    'Id':record.message_id,
                    'ReceiptHandle':record.receipt_handle
                })
            except KeyError:
                msg_to_delete.append({
                    'Id':record.message_id,
                    'ReceiptHandle':record.receipt_handle
                })
                print("Message not found in id:" + record.message_id)
                save_json(json.dumps(body))
                # record.delete()

        if len(msg_to_delete) > 0:
            # Delete processed messages from the queue
            delete_resp = queue.delete_messages(Entries=msg_to_delete)
        else:
            # Break the loop if no messages to process
            print("Breaking all the rules at {} iteration".format(i))
            break
    if process_queue == False:
        # Nothing in the queue, so we return
        return
    # We have something in the queue, so we save it as a CSV file in S3
    #s3 = boto3.resource('s3',region_name=region,aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)
    s3 = boto3.resource('s3',region_name=region)
