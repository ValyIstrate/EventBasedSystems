package com.ebs.publisher.features.publisher.algorithm;

import com.ebs.publisher.features.publisher.models.Publication;
import com.ebs.publisher.features.publisher.models.Subscription;
import com.ebs.publisher.features.publisher.utils.Utils;
import com.ebs.publisher.features.publisher.workers.PublisherGeneratorThread;
import com.ebs.publisher.features.publisher.workers.SubscriptionGenerator;
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
public class PubSubGenerationAlgorithm {
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

    public PubSubGenerationAlgorithm() {
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

        int limitPubs = 1000;
        int countThreads = numberOfPubs <= limitPubs ? 1 : numberOfPubs / limitPubs + 1;

        List<PublisherGeneratorThread> publisherGeneratorThreads = new ArrayList<>();
        for(int i = 0; i < countThreads; i++){
            if (i == countThreads - 1 && numberOfPubs != limitPubs) {
                limitPubs = numberOfPubs % limitPubs;
            }


            PublisherGeneratorThread thread = new PublisherGeneratorThread(limitPubs);
            publisherGeneratorThreads.add(thread);
            thread.start();
        }
        for(PublisherGeneratorThread thread : publisherGeneratorThreads){
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        for (PublisherGeneratorThread thread : publisherGeneratorThreads){
            generatedPublications.addAll(thread.getPublications());
        }


    }

    private void generateNonParallelPublications() {
        for (int i = 0; i < numberOfPubs; i++) {
            generatedPublications.add(new Publication());
        }
    }

    private void combineGeneratedSubscriptions(List<SubscriptionGenerator> generators) {
        Random random = new Random();
        AtomicInteger subCount = new AtomicInteger(0);

        generators.forEach(generator -> {
            var generatedSubs = generator.getSubscriptions();

            if (generatedSubs.size() + subCount.get() <= numberOfSubs) {
                generatedSubscriptions.addAll(generatedSubs);
                subCount.addAndGet(generatedSubs.size());
            } else {
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
                                    randSub.addOperator(computeOperatorForKey(sub.getOperator().get(0), key));
                                    okFlag.set(true);
                                }
                            });
                        }
                    }
                });
            }
        });
    }

    private String computeOperatorForKey(String operator, String key) {
        if (key.equals("city") || key.equals("direction")) {
            if (operator.equals("=") || operator.equals("!=")) {
                return operator;
            }

            return "=";
        }

        return operator;
    }

    private List<String> generateSubscriptionStats(int totalSubs) {
        List<String> metadataKeys = Utils.getMetadataKeys(); // city, temp, etc.
        List<String> messages = new ArrayList<>();

        for (String key : metadataKeys) {
            long count = generatedSubscriptions.stream()
                    .filter(sub -> sub.getInfo().containsKey(key))
                    .count();
            String msg = String.format("Number of subs containing the %s field: %d -> %.2f%%",
                    key, count, (double) count / totalSubs * 100);
            log.info(msg);
            messages.add(msg);
        }

        return messages;
    }

    private List<String> generateNonParallelSubscriptions() {
        List<String> metadataKeys = Utils.getMetadataKeys();
        List<Integer> rates = List.of(cityRate, tempRate, rainRate, windRate, directionRate, dateRate);

        List<SubscriptionGenerator> generators = new ArrayList<>();

        IntStream.range(0, metadataKeys.size()).forEach(i -> {
            SubscriptionGenerator generator = new SubscriptionGenerator(
                    rates.get(i), metadataKeys.get(i), numberOfSubs
            );
            generators.add(generator);
            generator.generateSubs();
        });

        combineGeneratedSubscriptions(generators);

        return generateSubscriptionStats(numberOfSubs);
    }

    private List<String> generateParallelSubscriptions() {
        List<String> metadataKeys = Utils.getMetadataKeys();
        List<Integer> rates = List.of(cityRate, tempRate, rainRate, windRate, directionRate, dateRate);

        List<SubscriptionGenerator> generators = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();

        IntStream.range(0, metadataKeys.size()).forEach(i -> {
            SubscriptionGenerator generator = new SubscriptionGenerator(
                    rates.get(i), metadataKeys.get(i), numberOfSubs
            );
            generators.add(generator);
            Thread t = new Thread(generator::generateSubs);
            threads.add(t);
            t.start();
        });

        threads.forEach(t -> {
            try {
                t.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        combineGeneratedSubscriptions(generators);

        return generateSubscriptionStats(numberOfSubs);
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
