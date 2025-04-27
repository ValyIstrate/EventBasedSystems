package com.ebs.publisher.features.publisher.aws.messaging.message;

public record PubSubMessage<T>(
        String messageType,
        T message
) {
}
