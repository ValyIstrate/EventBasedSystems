package ebs.project.bolts;

import ebs.project.models.Publication;
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
    private Map<String, List<Map<String, Map<Object, String>>>> subscriptions;
    private final String brokerId;

    private int receivedPublicationsNumber;
    private int matchedPublicationsNumber;

    public TumblingBrokerBolt(String brokerId) {
        this.brokerId = brokerId;
        this.subscriptions = new HashMap<>();
        this.receivedPublicationsNumber = 0;
        this.matchedPublicationsNumber = 0;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String streamId = input.getSourceStreamId();

        if (streamId.equals("subscription-stream")) {
            handleSubscriptionRequest(input);
        } else { // decoded-stream
            handleInStreamMessage(input);
        }
    }

    private void handleInStreamMessage(Tuple input) {
        Long stationId = input.getLongByField("stationId");
        String city = input.getStringByField("city");
        String direction = input.getStringByField("direction");
        String date = input.getStringByField("date");
        Long temp = input.getLongByField("temp");
        Long wind = input.getLongByField("wind");
        Double rain = input.getDoubleByField("rain");

        Publication pub = new Publication(stationId, city, temp, rain, wind, direction, date);

        window.add(pub);

        if (window.size() == windowSize) {
            processWindow();
            window.clear();
        }
    }

    private void handleSubscriptionRequest(Tuple input) {
        String subId = input.getStringByField("subscriberId");

        Map<String, String> city = Optional.ofNullable((Map<String, String>) input.getValueByField("city"))
                .orElse(new HashMap<>());
        Map<String, String> date = Optional.ofNullable((Map<String, String>) input.getValueByField("date"))
                .orElse(new HashMap<>());
        Map<String, String> direction = Optional.ofNullable((Map<String, String>) input.getValueByField("direction"))
                .orElse(new HashMap<>());
        Map<Long, String> temp = Optional.ofNullable((Map<Long, String>) input.getValueByField("temp"))
                .orElse(new HashMap<>());
        Map<Double, String> rain = Optional.ofNullable((Map<Double, String>) input.getValueByField("rain"))
                .orElse(new HashMap<>());
        Map<Long, String> wind = Optional.ofNullable((Map<Long, String>) input.getValueByField("wind"))
                .orElse(new HashMap<>());
        Map<Double, String> avgTemp = Optional.ofNullable((Map<Double, String>) input.getValueByField("avg_temp"))
                .orElse(new HashMap<>());
        Map<Double, String> avgRain = Optional.ofNullable((Map<Double, String>) input.getValueByField("avg_rain"))
                .orElse(new HashMap<>());
        Map<Double, String> avgWind = Optional.ofNullable((Map<Double, String>) input.getValueByField("avg_wind"))
                .orElse(new HashMap<>());

        Map<String, Map<Object, String>> subFields = new HashMap<>();
        subFields.put("city", new HashMap<>(city));
        subFields.put("date", new HashMap<>(date));
        subFields.put("direction", new HashMap<>(direction));
        subFields.put("temp", new HashMap<>((Map<Object, String>) (Map<?, ?>) temp));
        subFields.put("rain", new HashMap<>((Map<Object, String>) (Map<?, ?>) rain));
        subFields.put("wind", new HashMap<>((Map<Object, String>) (Map<?, ?>) wind));
        subFields.put("avgTemp", new HashMap<>((Map<Object, String>) (Map<?, ?>) avgTemp));
        subFields.put("avgRain", new HashMap<>((Map<Object, String>) (Map<?, ?>) avgRain));
        subFields.put("avgWind", new HashMap<>((Map<Object, String>) (Map<?, ?>) avgWind));

        synchronized (subscriptions) {
            subscriptions.computeIfAbsent(subId, k -> new ArrayList<>()).add(subFields);
        }
    }

    private void processWindow() {
        synchronized (subscriptions) {
            for (var entry : subscriptions.entrySet()) {
                boolean suchSubFound = false;
                String subId =  entry.getKey();;
                List<Map<String, Map<Object, String>>> subs = entry.getValue();
                for (Map<String, Map<Object, String>> sub : subs) {
                    if (foundMatch(sub, window)) {
                        window.forEach(publication -> {
                            collector.emit("notification-stream",
                                    new Values(subId, publication.getField("stationId"), publication.getField("city"),
                                            publication.getField("date"), publication.getField("direction"),
                                            publication.getField("temp"), publication.getField("rain"),
                                            publication.getField("wind")));
                            matchedPublicationsNumber++;
                        });
                        suchSubFound = true;
                        break;
                    }
                }
                if (suchSubFound) {
                    break;
                }
            }
        }
    }

    private boolean foundMatch(Map<String, Map<Object, String>> sub, List<Publication> window) {
        var windowTempAvg = calculateAverage("temp", window);
        var windowRainAvg = calculateAverage("rain", window);
        var windowWindAvg = calculateAverage("wind", window);

        for (var entry : sub.entrySet()) {
            var subFieldName = entry.getKey();
            var subFieldMap = entry.getValue();

            if (subFieldMap.isEmpty()) {
                for (var field : subFieldMap.entrySet()) {
                    Object subFieldValue = field.getKey();
                    String operator = field.getValue();

                    return switch (subFieldName) {
                        case "avg_temp" -> evaluateCondition((Double) subFieldValue, windowTempAvg, operator);
                        case "avg_rain" -> evaluateCondition((Double) subFieldValue, windowRainAvg, operator);
                        case "avg_wind" -> evaluateCondition((Double) subFieldValue, windowWindAvg, operator);
                        default -> throw new RuntimeException("Unknown field: " + subFieldName);
                    };
                }
            }
        }
        return true;
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
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("notification-stream",
                new Fields("subscriberId", "stationId", "city", "date", "direction", "temp", "rain", "wind"));
        outputFieldsDeclarer.declareStream("subscription-stream",
                new Fields("subscriberId", "city", "date", "direction", "temp", "rain", "wind", "avg_temp", "avg_rain", "avg_wind"));
        outputFieldsDeclarer.declareStream("decoded-stream",
                new Fields("stationId", "city", "date", "direction", "temp", "rain", "wind"));
    }
}
