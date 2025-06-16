package ebs.project.bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.util.*;
import ebs.project.proto_classes.PublicationProto;
import ebs.project.proto_classes.MessageProto;


public class BrokerBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String, List<Map<String, Map<Object, String>>>> subsMap;
    private final String brokerId;

    private int receivedPublicationsNumber;
    private int matchedPublicationsNumber;

    public BrokerBolt(String brokerId) {
        this.brokerId = brokerId;
        this.subsMap = new HashMap<>();
        this.receivedPublicationsNumber = 0;
        this.matchedPublicationsNumber = 0;
    }

    public String getBrokerId() {
        return this.brokerId;
    }

    public int getReceivedPublicationsNumber() {
        return receivedPublicationsNumber;
    }

    public int getMatchedPublicationsNumber() {
        return matchedPublicationsNumber;
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple input) {
        String streamType = input.getSourceStreamId();
        if (streamType.equals("subscription-stream")) {
            handleSubscriptionRequest(input);
        } else if (streamType.equals("default")) {
            handleMessageReadFromQueue(input, streamType);
        } else {
            if (streamType.equals("decoded-stream")) {
                receivedPublicationsNumber++;
            }
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
        long emissionTime = input.getLongByField("emissionTime");
        Map<String, Object> pubFields = Map.of(
                "stationId", stationId,
                "city", city,
                "direction", direction,
                "date", date,
                "temp", temp,
                "wind", wind,
                "rain", rain
        );

        synchronized (subsMap) {
            for (var entry : subsMap.entrySet()) {
                boolean suchSubFound = false;
                String subId =  entry.getKey();;
                List<Map<String, Map<Object, String>>> subs = entry.getValue();
                for (Map<String, Map<Object, String>> sub : subs) {
                    if (foundMatch(sub, pubFields)) {
                        collector.emit("notification-stream",
                                new Values(subId, stationId, city, date, direction, temp, rain, wind, emissionTime));
                        matchedPublicationsNumber++;
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

    private boolean foundMatch(Map<String, Map<Object, String>> sub, Map<String, Object> pubFields) {
        for (var entry : sub.entrySet()) {
            var subFieldName = entry.getKey();
            var subFieldMap = entry.getValue();

            Object pubFieldValue =  pubFields.get(subFieldName);

            if (Objects.nonNull(pubFieldValue) && !subFieldMap.isEmpty()) {
                for (var field : subFieldMap.entrySet()) {
                    Object subFieldValue = field.getKey();
                    String operator = field.getValue();

                    return switch (subFieldName) {
                        case "city", "direction" -> compareStrings(pubFieldValue, subFieldValue, operator);
                        case "date" -> compareDates(pubFieldValue, subFieldValue, operator);
                        case "temp", "wind" -> compareLongs(pubFieldValue, subFieldValue, operator);
                        case "rain" -> compareDoubles(pubFieldValue, subFieldValue, operator);
                        default -> throw new RuntimeException("Unknown field: " + subFieldName);
                    };
                }
            }
        }
        return true;
    }

    private void handleMessageReadFromQueue(Tuple input, String streamType) {
        try {
            String base64EncodedMessage = input.getStringByField("message");

            byte[] protoBytes = Base64.getDecoder().decode(base64EncodedMessage);
            MessageProto.MsgProto msgProto = MessageProto.MsgProto.parseFrom(protoBytes);

            if (msgProto.getMessageType().equals("PubType")) {
                PublicationProto.PubProto publication = msgProto.getPublication();
                processPublication(publication);
            } else if (msgProto.getMessageType().equals("SubType")) {
                // DO NOTHING
                return;
            }

            collector.ack(input);
        } catch (Exception ex) {
            collector.reportError(ex);
            collector.fail(input);
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

        Map<String, Map<Object, String>> subFields = new HashMap<>();
        subFields.put("city", new HashMap<>(city));
        subFields.put("date", new HashMap<>(date));
        subFields.put("direction", new HashMap<>(direction));
        subFields.put("temp", new HashMap<>((Map<Object, String>) (Map<?, ?>) temp));
        subFields.put("rain", new HashMap<>((Map<Object, String>) (Map<?, ?>) rain));
        subFields.put("wind", new HashMap<>((Map<Object, String>) (Map<?, ?>) wind));

        synchronized (subsMap) {
            subsMap.computeIfAbsent(subId, k -> new ArrayList<>()).add(subFields);
        }

        collector.emit("subscription-stream", new Values(brokerId, city, date, direction, temp, rain, wind));
    }

    private void processPublication(PublicationProto.PubProto publication) {
        Long stationId = publication.getStationId();
        String city = publication.getCity();
        String date = publication.getDate();
        String direction = publication.getDirection();
        Long temp = publication.getTemperature();
        Long wind = publication.getWindSpeed();
        Double rain = publication.getRainProbability();
        long emissionTime = publication.getEmissionTime();
        collector.emit("decoded-stream",
                new Values(stationId, city, date, direction, temp, rain, wind, emissionTime));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("notification-stream",
                new Fields("subscriberId", "stationId", "city", "date", "direction", "temp", "rain", "wind", "emissionTime"));
        outputFieldsDeclarer.declareStream("subscription-stream",
                new Fields("subscriberId", "city", "date", "direction", "temp", "rain", "wind"));
        outputFieldsDeclarer.declareStream("decoded-stream",
                new Fields("stationId", "city", "date", "direction", "temp", "rain", "wind", "emissionTime"));
    }

    private boolean compareStrings(Object pubFieldValue, Object subFieldValue, String operator) {
        String pubFieldValueStr = pubFieldValue.toString();
        String subFieldValueStr = subFieldValue.toString();

        return switch (operator) {
            case "=" ->  pubFieldValueStr.equals(subFieldValueStr);
            case "!=" ->  !pubFieldValueStr.equals(subFieldValueStr);
            default -> false;
        };
    }

    private boolean compareDoubles(Object pubFieldValue, Object subFieldValue, String operator) {
        Double pubDoubleValue = (Double) pubFieldValue;
        Double subDoubleValue = (Double) subFieldValue;

        return switch (operator) {
            case ">" -> pubDoubleValue > subDoubleValue;
            case "<" -> pubDoubleValue < subDoubleValue;
            case "=" -> Objects.equals(pubDoubleValue, subDoubleValue);
            case "!=" -> !Objects.equals(pubDoubleValue, subDoubleValue);
            default -> false;
        };
    }

    private boolean compareLongs(Object pubFieldValue, Object subFieldValue, String operator) {
        Long pubLong = (Long) pubFieldValue;
        Long subLong = (Long) subFieldValue;

        return switch (operator) {
            case ">" -> pubLong > subLong;
            case "<" -> pubLong < subLong;
            case "=" -> Objects.equals(pubLong, subLong);
            case "!=" -> !Objects.equals(pubLong, subLong);
            default -> false;
        };
    }

    private boolean compareDates(Object pubFieldValue, Object subFieldValue, String operator) {
        LocalDate pubDate = LocalDate.parse(String.valueOf(pubFieldValue));
        LocalDate subDate = LocalDate.parse((String) subFieldValue);

        return switch (operator) {
            case "=" -> (pubDate.isEqual(subDate));
            case ">" -> (pubDate.isAfter(subDate));
            case "<" -> (pubDate.isBefore(subDate));
            case "!=" -> (!pubDate.isEqual(subDate));
            default -> false;
        };
    }

    @Override
    public void cleanup() {


        try (BufferedWriter writer = new BufferedWriter(
                new FileWriter("results/stats/" + this.brokerId + ".txt"))) {
            writer.write("Publications received: " + receivedPublicationsNumber);
            writer.newLine();
        } catch (IOException e) {
            System.err.println("Error writing to file: " + e.getMessage());
        }
    }
}
