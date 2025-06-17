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

import java.io.*;
import java.util.*;

public class ComplexSubscriberBolt extends BaseRichBolt {
    private OutputCollector collector;
    private final String subscriberId;
    private final String subscriptionFile;
    private List<Subscription> subscriptions;
    private int receivedPubs = 0;

    public ComplexSubscriberBolt(String subscriberId, String subscriptionFile) {
        this.subscriberId = subscriberId;
        this.subscriptionFile = subscriptionFile;
        this.subscriptions = new ArrayList<>();
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        loadSubscriptions();

        for (var sub : subscriptions) {
            Map<String, String> city = new HashMap<>();
            Map<String, String> direction = new HashMap<>();
            Map<Long, String> temp = new HashMap<>();
            Map<Double, String> rain = new HashMap<>();
            Map<Long, String> wind = new HashMap<>();
            Map<String, String> date = new HashMap<>();
            Map<Double, String> avgTemp = new HashMap<>();
            Map<Double, String> avgRain = new HashMap<>();
            Map<Double, String> avgWind = new HashMap<>();

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
                        rain.put(Double.valueOf((String) value), operator);
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
                    case "avg_rain":
                        avgRain.put(Double.valueOf((String) value), operator);
                        break;
                    case "avg_wind":
                        avgWind.put(Double.valueOf(String.valueOf(value)), operator);
                        break;
                    case "avg_temp":
                        avgTemp.put(Double.valueOf(String.valueOf(value)), operator);
                }
            }

            collector.emit("subscription-stream",
                    new Values(subscriberId, city, date, direction, temp, rain, wind, avgTemp, avgRain, avgWind));
        }
    }

    private void loadSubscriptions() {
        try (BufferedReader reader = new BufferedReader(new FileReader(subscriptionFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                Subscription sub = parseSubscription(line);
                if (sub != null) {
                    subscriptions.add(sub);
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
            if (!input.getValueByField("subscriberId").equals(subscriberId)) {
                return;
            }

            receivedPubs++;

            Long stationId = Long.valueOf(input.getStringByField("stationId"));
            String city = input.getStringByField("city");
            String direction = input.getStringByField("direction");
            Double rain = Double.valueOf(input.getStringByField("rain"));
            Long wind = Long.valueOf(input.getStringByField("wind"));
            Long temp = Long.valueOf(input.getStringByField("temp"));
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
                new Fields("subscriberId", "city", "date", "direction", "temp", "rain", "wind", "avg_temp", "avg_rain", "avg_wind"));
    }
}
