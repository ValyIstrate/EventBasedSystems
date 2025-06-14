package com.ebs.publisher.features.publisher.aws.messaging.sender;

import com.ebs.publisher.features.publisher.aws.messaging.message.PubSubMessage;
import com.ebs.publisher.features.publisher.models.Publication;
import com.ebs.publisher.features.publisher.models.Subscription;
import com.ebs.publisher.features.publisher.proto_classes.MessageProto;
import com.ebs.publisher.features.publisher.proto_classes.PublicationProto;
import com.ebs.publisher.features.publisher.proto_classes.SubscriptionProto;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.protobuf.ProtobufFactory;
import com.fasterxml.jackson.dataformat.protobuf.ProtobufMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sqs.SqsClient;

@Slf4j
@Service
public class PubSubSender extends BaseSender {
    private static final String PUBLICATION = "PubType";
    private static final String SUBSCRIPTION = "SubType";

    @Value("${aws.sqs.all_broker_queue_name}")
    private String allBrokerQueueName;

    private static final ProtobufMapper PROTOBUF_MAPPER = new ProtobufMapper(ProtobufFactory.builder().build());

    public MessageProto.MsgProto buildPublicationMessage(Publication publication) {
        var protoBuilder = MessageProto.MsgProto.newBuilder();
        protoBuilder.setMessageType(PUBLICATION);
        protoBuilder.setPublication(publication.buildProto());
        return protoBuilder.build();
    }

    public MessageProto.MsgProto buildSubscriptionMessage(Subscription subscription) {
        var protoBuilder = MessageProto.MsgProto.newBuilder();
        protoBuilder.setMessageType(SUBSCRIPTION);
        protoBuilder.setSubscription(subscription.buildProto());
        return protoBuilder.build();
    }

    public PubSubSender(SqsClient sqsClient) {
        super(sqsClient);
    }

    public void sendMessage(Object pubOrSub) {
        try {
            if (pubOrSub instanceof Publication) {
                MessageProto.MsgProto pubMessage = buildPublicationMessage((Publication) pubOrSub);
                String serializedMessage = PROTOBUF_MAPPER.writeValueAsString(pubMessage);
                sendMessage(allBrokerQueueName, serializedMessage);
            }  else if (pubOrSub instanceof Subscription) {
                MessageProto.MsgProto subMessage = buildSubscriptionMessage((Subscription) pubOrSub);
                String serializedMessage = PROTOBUF_MAPPER.writeValueAsString(subMessage);
                sendMessage(allBrokerQueueName, serializedMessage);
            } else {
                log.error("Unrecognized object type {}", pubOrSub.getClass());
            }
        } catch (JsonProcessingException ex) {
            log.error("The pub/sub message cannot be serialized as string! -> {}", ex.getMessage());
        }
    }
}
