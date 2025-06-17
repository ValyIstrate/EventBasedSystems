package ebs.project.spouts;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

public class SqsSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private SqsClient sqsClient;
    private static final String queueUrl = "http://localhost:4566/000000000000/sqs_all_broker_queue";
    private int sentPublicationsNumber; // Counter for sent publications

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.sqsClient = SqsClient.builder()
                .endpointOverride(URI.create("http://localhost:4566"))
                .region(Region.of("eu-central-1"))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("dummy", "dummy")))
                .build();
        this.sentPublicationsNumber = 0; // Initialize counter
    }

    @Override
    public void nextTuple() {
        ReceiveMessageResponse response = sqsClient.receiveMessage(ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(10)
                .waitTimeSeconds(10)
                .build());

        List<Message> messages = response.messages();
        if (!messages.isEmpty()) {
            for (Message message : messages) {
                long emissionTime = System.currentTimeMillis();
                collector.emit(new Values(message.body(), emissionTime));
                sentPublicationsNumber++;
                sqsClient.deleteMessage(DeleteMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .receiptHandle(message.receiptHandle())
                        .build());
            }
        } else {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("message", "emissionTime"));
    }

    @Override
    public void close() {
        File resultsDir = new File("results");
        if (!resultsDir.exists()) {
            resultsDir.mkdirs();
        }

        try (BufferedWriter writer = new BufferedWriter(
                new FileWriter("results/publisher.txt"))) {
            writer.write("Publications sent: " + sentPublicationsNumber);
            writer.newLine();
        } catch (IOException e) {
            System.err.println("Error writing to file: " + e.getMessage());
        }
    }
}