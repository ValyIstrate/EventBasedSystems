package ebs.project.bolts;

import ebs.project.models.Publication;
import ebs.project.models.Subscription;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SubscriberBolt extends BaseRichBolt {
    private OutputCollector collector;
    private final String subscriberId;
    private List<Subscription> subscriptions;
    private int receivedPubs = 0;
    private String filename;

    public SubscriberBolt(String subscriberId, String filename) {
        this.subscriberId = subscriberId;
        this.receivedPubs = 0;
        this.filename = filename;
    }

    private List<Subscription> readSubsFromFile(String filename) {
        List<Subscription> subscriptions = new ArrayList<>();
        Pattern pattern = Pattern.compile("\\(([^)]+)\\)");

        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }
                Subscription subscription = extractSubscription(pattern, line);
                subscriptions.add(subscription);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return subscriptions;
    }

    private static Subscription extractSubscription(Pattern pattern, String line) {
        Subscription subscription = new Subscription();
        Matcher matcher = pattern.matcher(line);

        while (matcher.find()) {
            String[] parts = matcher.group(1).split(",");
            if (parts.length == 3) {
                String key = parts[0].trim();
                String operator = parts[1].trim();
                String value = parts[2].trim();

                subscription.addInfo(key, value);
                subscription.addOperator(operator);
            }
        }
        return subscription;
    }

    @Override
    public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.subscriptions = readSubsFromFile(this.filename);

        for (var sub : subscriptions) {
            Map<String, String> city = new HashMap<>();
            Map<String, String> direction = new HashMap<>();
            Map<Long, String> temp = new HashMap<>();
            Map<Double, String> rain = new HashMap<>();
            Map<Long, String> wind = new HashMap<>();
            Map<String, String> date = new HashMap<>();

            int idx = 0;
            for (var field : sub.getInfo().entrySet()) {
                String key = field.getKey();
                Object value = field.getValue();
                String operator = sub.getOperator().get(idx);
                idx++;

                switch (key) {
                    case "city":
                        city.put((String) value, operator);
                        break;
                    case "direction":
                        direction.put((String) value, operator);
                        break;
                    case "rain":
                        rain.put(Double.valueOf((String)value), operator);
                        break;
                    case "wind":
                        wind.put(Long.valueOf(String.valueOf(value)), operator);
                        break;
                    case "temp":
                        temp.put(Long.valueOf(String.valueOf(value)), operator);
                        break;
                    case "date":
                        date.put((String) value, operator);
                        break;
                }
            }

            collector.emit("subscription-stream",
                    new Values(subscriberId, city, date, direction, temp, rain, wind));
        }
    }

    @Override
    public void execute(Tuple input) {
        if (!input.getValueByField("subscriberId").equals(subscriberId)) {
            return;
        }

        String streamId = input.getSourceStreamId();

        if (streamId.equals("notification-stream")) {
            receivedPubs++;

            Long stationId = input.getLongByField("stationId");
            String city = input.getStringByField("city");
            String direction = input.getStringByField("direction");
            Double rain = input.getDoubleByField("rain");
            Long wind = input.getLongByField("wind");
            Long temp = input.getLongByField("temp");
            String date = input.getStringByField("date");

            Publication publication = new Publication(stationId, city, temp, rain, wind, direction, date);

            String filePath = "publication_files/" + input.getStringByField("subscriberId") + ".txt";

            File directory = new File("publication_files");
            if (!directory.exists()) {
                directory.mkdirs();
            }

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) {
                writer.write(publication.toString());
                writer.newLine();
            } catch (IOException e) {
                System.err.println("Error writing to file: " + e.getMessage());
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("subscription-stream",
                new Fields("subscriberId", "city", "date", "direction", "temp", "rain", "wind"));
    }
}