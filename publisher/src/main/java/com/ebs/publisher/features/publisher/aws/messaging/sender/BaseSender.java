package com.ebs.publisher.features.publisher.aws.messaging.sender;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.util.List;

public class BaseSender {
    private final SqsClient sqsClient;

    public BaseSender(SqsClient sqsClient) {
        this.sqsClient = sqsClient;
    }

    protected void sendMessage(final String queueName, final String message) {
        var queueUrlRequest = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();

        var request = SendMessageRequest.builder()
                .queueUrl(sqsClient.getQueueUrl(queueUrlRequest).queueUrl())
                .messageBody(message)
                .build();

        sqsClient.sendMessage(request);
    }

    protected void sendBatch(List<SendMessageBatchRequestEntry> batch, String queueName) {
        try {
            SendMessageBatchRequest request = SendMessageBatchRequest.builder()
                    .queueUrl(queueName)
                    .entries(batch)
                    .build();

            sqsClient.sendMessageBatch(request);
        } catch (Exception e) {
            System.err.println("Failed to send batch: " + e.getMessage());
        }
    }
}
