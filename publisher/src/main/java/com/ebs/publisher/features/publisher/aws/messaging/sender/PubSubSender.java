package com.ebs.publisher.features.publisher.aws.messaging.sender;

import com.ebs.publisher.features.publisher.aws.messaging.message.PubSubMessage;
import com.ebs.publisher.features.publisher.models.Publication;
import com.ebs.publisher.features.publisher.models.Subscription;
import com.ebs.publisher.features.publisher.proto_classes.PublicationProto;
import com.ebs.publisher.features.publisher.proto_classes.SubscriptionProto;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sqs.SqsClient;

@Slf4j
@ConditionalOnMissingBean
@Service
public class PubSubSender extends BaseSender {
    private static final String PUBLICATION = "PubType";
    private static final String SUBSCRIPTION = "SubType";

    @Value("${aws.sqs.all_broker_queue_name}")
    private String allBrokerQueueName;

    private final ObjectMapper objectMapper;

    public PubSubMessage<PublicationProto.PubProto> buildPublicationMessage(Publication publication) {
        return new PubSubMessage<>(PUBLICATION, publication.buildProto());
    }

    public PubSubMessage<SubscriptionProto.SubProto> buildSubscriptionMessage(Subscription subscription) {
        return new PubSubMessage<>(SUBSCRIPTION, subscription.buildProto());
    }

    public PubSubSender(SqsClient sqsClient, ObjectMapper objectMapper) {
        super(sqsClient);
        this.objectMapper = objectMapper;
    }

    public void sendMessage(Object pubOrSub) {
        try {
            if (pubOrSub instanceof Publication) {
                sendMessage(allBrokerQueueName,
                        objectMapper.writeValueAsString(buildPublicationMessage((Publication) pubOrSub)));
            }  else if (pubOrSub instanceof Subscription) {
                sendMessage(allBrokerQueueName,
                        objectMapper.writeValueAsString(buildSubscriptionMessage((Subscription) pubOrSub)));
            } else {
                log.error("Unrecognized object type {}", pubOrSub.getClass());
            }
        } catch (JsonProcessingException ex) {
            log.error("The pub/sub message cannot be serialized as string! -> {}", ex.getMessage());
        }
    }
}
