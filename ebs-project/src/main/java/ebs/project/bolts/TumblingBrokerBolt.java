package ebs.project.bolts;

import ebs.project.models.Publication;
import ebs.project.models.Subscription;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;

public class TumblingBrokerBolt extends BaseRichBolt {

    private OutputCollector collector;
    private final int windowSize = 5; 
    private final List<Publication> window = new ArrayList<>();
    private final List<Subscription> subscriptions = new ArrayList<>();
    private final List<String> subscriberIds = new ArrayList<>();

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String streamId = input.getSourceStreamId();

        if (streamId.equals("subscription-stream")) {
            String subscriberId = input.getStringByField("subscriberId");
            Subscription sub = (Subscription) input.getValueByField("subscription");
            subscriptions.add(sub);
            subscriberIds.add(subscriberId);
        }

        if (streamId.equals("decoded-stream")) {
            Publication pub = (Publication) input.getValueByField("publication");

            window.add(pub);

            if (window.size() == windowSize) {
                processWindow();
                window.clear();
            }
        }
    }

    private void processWindow() {
        for (int i = 0; i < subscriptions.size(); i++) {
            Subscription sub = subscriptions.get(i);
            String subscriberId = subscriberIds.get(i);

            Map<String, String> filters = sub.getInfo();
            List<String> ops = sub.getOperator();
            boolean isWindowSubscription = filters.keySet().stream().anyMatch(f -> f.startsWith("avg_"));
            if (!isWindowSubscription) continue;

            String cityFilter = filters.getOrDefault("city", null);
            List<Publication> matchingCity = new ArrayList<>();

            for (Publication pub : window) {
                if (cityFilter == null || pub.getCity().equals(cityFilter)) {
                    matchingCity.add(pub);
                }
            }

            if (matchingCity.size() < windowSize) return;

            boolean allMatch = true;
            int j = 0;

            for (Map.Entry<String, String> entry : filters.entrySet()) {
                String field = entry.getKey();
                String value = entry.getValue();
                String op = ops.get(j++);

                if (field.equals("city")) continue;

                String metric = field.replace("avg_", "");
                double average = calculateAverage(metric, matchingCity);

                double conditionValue;
                try {
                    conditionValue = Double.parseDouble(value);
                } catch (NumberFormatException e) {
                    continue;
                }

                if (!evaluateCondition(average, conditionValue, op)) {
                    allMatch = false;
                    break;
                }
            }

            if (allMatch) {
                String metaPub = String.format("{(city,=,%s);(conditions,=,true)}", cityFilter);
                collector.emit("notification-stream", new Values(subscriberId, metaPub));
            }
        }
    }

    private double calculateAverage(String field, List<Publication> pubs) {
        double sum = 0;
        int count = 0;

        for (Publication pub : pubs) {
            switch (field) {
                case "temp":
                    sum += pub.getTemperature();
                    break;
                case "wind":
                    sum += pub.getWindSpeed();
                    break;
                case "rain":
                    sum += pub.getRainProbability();
                    break;
                default:
                    break;
            }
            count++;
        }
        return (count == 0) ? 0 : sum / count;
    }

    private boolean evaluateCondition(double actual, double expected, String operator) {
        return switch (operator) {
            case "=" -> actual == expected;
            case ">" -> actual > expected;
            case "<" -> actual < expected;
            case ">=" -> actual >= expected;
            case "<=" -> actual <= expected;
            case "!=" -> actual != expected;
            default -> false;
        };
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("notification-stream", new Fields("subscriberId", "publication"));
        declarer.declareStream("subscription-stream", new Fields("subscriberId", "subscription"));
    }
}
