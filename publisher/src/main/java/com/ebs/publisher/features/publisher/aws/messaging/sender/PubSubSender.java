package com.ebs.publisher.features.publisher.aws.messaging.sender;

import com.ebs.publisher.features.publisher.models.ProtobufConvertible;
import com.ebs.publisher.features.publisher.models.Publication;
import com.ebs.publisher.features.publisher.models.Subscription;
import com.ebs.publisher.features.publisher.proto_classes.MessageProto;
import com.fasterxml.jackson.dataformat.protobuf.ProtobufFactory;
import com.fasterxml.jackson.dataformat.protobuf.ProtobufMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

@Slf4j
@Service
public class PubSubSender extends BaseSender {
    private static final String PUBLICATION = "PubType";
    private static final String SUBSCRIPTION = "SubType";

    @Value("${aws.sqs.all_broker_queue_name}")
    private String allBrokerQueueName;

    private static final ProtobufMapper PROTOBUF_MAPPER = new ProtobufMapper(ProtobufFactory.builder().build());

    private static final Base64.Encoder base64Encoder = Base64.getEncoder();

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

//    public void sendMessages(List<Publication> items) {
//        items.parallelStream().forEach(item -> {
//            try {
//                MessageProto.MsgProto message = item.toProto();
//                byte[] protoBytes = message.toByteArray();
//                String base64EncodedMessage = base64Encoder.encodeToString(protoBytes);
//                sendMessage(allBrokerQueueName, base64EncodedMessage);
//            } catch (Exception ex) {
//                log.error("Failed to send message: {}", ex.getMessage(), ex);
//            }
//        });
//    }

    public void sendMessages(List<Publication> items) {
        final int BATCH_SIZE = 10;

        List<SendMessageBatchRequestEntry> batch = new ArrayList<>(BATCH_SIZE);
        int messageId = 0;

        for (ProtobufConvertible item : items) {
            try {
                MessageProto.MsgProto message = item.toProto();
                byte[] protoBytes = message.toByteArray();
                String base64 = Base64.getEncoder().encodeToString(protoBytes);

                batch.add(SendMessageBatchRequestEntry.builder()
                        .id(String.valueOf(messageId++))
                        .messageBody(base64)
                        .build());

                if (batch.size() == BATCH_SIZE) {
                    sendBatch(batch, allBrokerQueueName);
                    batch.clear();
                }
            } catch (Exception ex) {
                System.err.println("Error preparing message: " + ex.getMessage());
            }
        }

        if (!batch.isEmpty()) {
            sendBatch(batch, allBrokerQueueName);
        }
    }
}
