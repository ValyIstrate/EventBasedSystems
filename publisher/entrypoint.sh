#!/bin/bash

localstack start &

sleep 10

QUEUE_NAMES=${SQS_QUEUE_NAMES:-"sqs_all_broker_queue"}

for queue_name in $(echo $QUEUE_NAMES | tr ',' ' '); do
    if [ -n "$queue_name" ]; then
        echo "Creating queue: $queue_name"
        awslocal sqs create-queue --queue-name $queue_name --region $AWS_REGION
    fi
done

tail -f /dev/null
