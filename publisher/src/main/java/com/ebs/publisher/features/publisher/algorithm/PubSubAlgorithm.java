package com.ebs.publisher.features.publisher.algorithm;

import com.ebs.publisher.features.publisher.models.Publication;
import com.ebs.publisher.features.publisher.models.Subscription;
import com.ebs.publisher.features.publisher.utils.Utils;
import com.ebs.publisher.features.publisher.workers.NonParallelSubscriptionGenerator;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

@Slf4j
public class PubSubAlgorithm {
    private int numberOfSubs;
    private int numberOfPubs;
    private int cityRate;
    private int tempRate;
    private int rainRate;
    private int windRate;
    private int directionRate;
    private int dateRate;

    private boolean isParallel;

    public static int minimumCity;

    private List<Subscription> generatedSubscriptions;
    private List<Publication> generatedPublications;

    public PubSubAlgorithm() {
        this.numberOfSubs = 300;
        this.numberOfPubs = 300;
        this.cityRate = 90;
        this.tempRate = 40;
        this.rainRate = 30;
        this.windRate = 70;
        this.directionRate = 80;
        this.dateRate = 50;
        minimumCity = 70;
        this.isParallel = false;
        this.generatedSubscriptions = new ArrayList<>();
        this.generatedPublications = new ArrayList<>();
    }

    public void init(int numberOfSubs, int numberOfPubs, int cityRate,
                     int tempRate, int rainRate, int windRate, int directionRate,
                     int dateRate, boolean isParallel) {
        this.numberOfSubs = numberOfSubs;
        this.numberOfPubs = numberOfPubs;
        this.cityRate = cityRate;
        this.tempRate = tempRate;
        this.rainRate = rainRate;
        this.windRate = windRate;
        this.directionRate = directionRate;
        this.dateRate = dateRate;
        this.isParallel = isParallel;
    }

    public List<String> generateSubscriptions() {
        if (!isParallel) {
            return generateNonParallelSubscriptions();
        } else {
            return generateParallelSubscriptions();
        }
    }

    public void generatePublications() {
        if (!isParallel) {
            generateNonParallelPublications();
        } else {
            generateParallelPublications();
        }
    }

    private void generateParallelPublications() {
        return;
    }

    private void generateNonParallelPublications() {
        for (int i = 0; i <= numberOfPubs; i++) {
            generatedPublications.add(new Publication());
        }
    }

    private List<String> generateParallelSubscriptions() {
        return List.of();
    }

    private List<String> generateNonParallelSubscriptions() {
        Random random = new Random();

        List<String> metadataKeys = Utils.getMetadataKeys();
        List<Integer> rates = List.of(cityRate, tempRate, rainRate, windRate, directionRate, dateRate);

        List<NonParallelSubscriptionGenerator> availableGenerators = new ArrayList<>();

        IntStream.range(0, metadataKeys.size()).forEach(i -> {
            NonParallelSubscriptionGenerator generator = new NonParallelSubscriptionGenerator(
                    rates.get(i),
                    metadataKeys.get(i),
                    numberOfSubs);

            availableGenerators.add(generator);
            generator.generateSubs();
        });

        AtomicInteger subCount = new AtomicInteger(0);
        availableGenerators.forEach(generator -> {
            var generatedSubs = generator.getSubscriptions();
            if (generatedSubs.size() + subCount.get() <= numberOfSubs) {
                generatedSubscriptions.addAll(generatedSubs);
                subCount.addAndGet(generatedSubs.size());
                return;
            }

            generatedSubs.forEach(sub -> {
               if (subCount.get() < numberOfSubs) {
                   generatedSubscriptions.add(sub);
                   subCount.incrementAndGet();
               } else {
                   AtomicBoolean okFlag = new AtomicBoolean(false);
                   while (!okFlag.get()) {
                       int randIndex = random.nextInt(generatedSubscriptions.size());
                       Subscription randSub = generatedSubscriptions.get(randIndex);

                       sub.getInfo().keySet().forEach(key -> {
                           if (!randSub.getInfo().containsKey(key)) {
                               randSub.addInfo(key, sub.getInfo().get(key));
                               randSub.addOperator(sub.getOperator().get(0));
                               okFlag.set(true);
                           }
                       });
                   }
               }
            });
        });

        Long citySubCount = generatedSubscriptions.stream()
                .filter(sub -> sub.getInfo().containsKey("city"))
                .count();

        Long tempSubCount = generatedSubscriptions.stream()
                .filter(sub -> sub.getInfo().containsKey("temp"))
                .count();

        Long rainSubCount = generatedSubscriptions.stream()
                .filter(sub -> sub.getInfo().containsKey("rain"))
                .count();

        Long windSubCount = generatedSubscriptions.stream()
                .filter(sub -> sub.getInfo().containsKey("wind"))
                .count();

        Long directionSubCount = generatedSubscriptions.stream()
                .filter(sub -> sub.getInfo().containsKey("direction"))
                .count();

        Long dateSubCount = generatedSubscriptions.stream()
                .filter(sub -> sub.getInfo().containsKey("date"))
                .count();

        String citySubMessage = String.format("Number of subs containing the city field: %d -> %.2f%%",
                citySubCount, (double) citySubCount / subCount.get() * 100);

        String tempSubMessage = String.format("Number of subs containing the temp field: %d -> %.2f%%",
                tempSubCount, (double) tempSubCount / subCount.get() * 100);

        String rainSubMessage = String.format("Number of subs containing the rain field: %d -> %.2f%%",
                rainSubCount, (double) rainSubCount / subCount.get() * 100);

        String windSubMessage = String.format("Number of subs containing the wind field: %d -> %.2f%%",
                windSubCount, (double) windSubCount / subCount.get() * 100);

        String directionSubMessage = String.format("Number of subs containing the direction field: %d -> %.2f%%",
                directionSubCount, (double) directionSubCount / subCount.get() * 100);

        String dateSubMessage = String.format("Number of subs containing the date field: %d -> %.2f%%",
                dateSubCount, (double) dateSubCount / subCount.get() * 100);

        log.info(citySubMessage);
        log.info(tempSubMessage);
        log.info(rainSubMessage);
        log.info(windSubMessage);
        log.info(directionSubMessage);
        log.info(dateSubMessage);

        return List.of(
                citySubMessage,
                tempSubMessage,
                rainSubMessage,
                windSubMessage,
                directionSubMessage,
                dateSubMessage
        );
    }

    public void createFilesContainingData(String filePath, List<?> dataList) {
        File file = new File(filePath);

        try (FileWriter fw = new FileWriter(file, false);
             BufferedWriter bw = new BufferedWriter(fw)) {
            for (Object data : dataList) {
                if (data instanceof Publication || data instanceof Subscription) {
                    bw.write(data.toString());
                    bw.newLine();
                }
            }
        } catch (IOException ex) {
            log.error("Failed to create files for created entities: {}", ex.getMessage());
        }
    }

    public void createFilesContainingPubsAndSubs() {
        String dirPath = "pubsAndSubs";

        File directory = new File(dirPath);
        if (!directory.exists()) {
            directory.mkdirs();
        }

        String pubFilePath = dirPath + File.separator + "publications.txt";
        String subFilePath = dirPath + File.separator + "subscriptions.txt";

        createFilesContainingData(pubFilePath, generatedPublications);
        createFilesContainingData(subFilePath, generatedSubscriptions);
    }
}
