package ebs.project.bolts;

import ebs.project.models.Subscription;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

public class ComplexSubscriberBolt extends BaseRichBolt {
    private OutputCollector collector;
    private final String subscriberId;
    private final String subscriptionFile;

    public ComplexSubscriberBolt(String subscriberId, String subscriptionFile) {
        this.subscriberId = subscriberId;
        this.subscriptionFile = subscriptionFile;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        loadSubscriptions();
    }

    private void loadSubscriptions() {
        try (BufferedReader reader = new BufferedReader(new FileReader(subscriptionFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                Subscription sub = parseSubscription(line);
                if (sub != null) {
                    collector.emit("subscription-stream", new Values(subscriberId, sub));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Subscription parseSubscription(String line) {
        try {
            // Format: {(field,op,value);(field,op,value);...}
            line = line.substring(1, line.length() - 1);
            String[] parts = line.split(";");

            Subscription sub = new Subscription();

            for (String part : parts) {
                part = part.trim();
                if (part.startsWith("(")) part = part.substring(1);
                if (part.endsWith(")")) part = part.substring(0, part.length() - 1);

                String[] elems = part.split(",");
                if (elems.length != 3) continue;

                String field = elems[0].trim();
                String operator = elems[1].trim();
                String value = elems[2].trim().replace("\"", "");

                sub.addInfo(field, value);
                sub.addOperator(operator);
            }

            return sub;
        } catch (Exception e) {
            System.err.println("Error parsing subscription line: " + line);
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void execute(Tuple input) {
        if (input.getSourceStreamId().equals("notification-stream")) {
            String subscriberId = input.getStringByField("subscriberId");
            Object publication = input.getValueByField("publication");
            System.out.println("[COMPLEX] Subscriber " + subscriberId + " received meta-publication: " + publication);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("subscription-stream", new Fields("subscriberId", "subscription"));
    }
}
