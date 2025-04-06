package com.ebs.publisher.features.publisher;

import com.ebs.publisher.features.publisher.algorithm.PubSubAlgorithm;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;

@Service
@Slf4j
public class PublisherService {
    public PublisherRunResultDto runGeneration(int numberOfSubs, int numberOfPubs, int cityRate,
                                                          int tempRate, int rainRate, int windRate, int directionRate,
                                                          int dateRate, boolean isParallel) {

        Instant start = Instant.now();
        PubSubAlgorithm pubSubAlgorithm = new PubSubAlgorithm();

        pubSubAlgorithm.init(numberOfSubs, numberOfPubs, cityRate, tempRate, rainRate, windRate,
                directionRate, dateRate, isParallel);

        pubSubAlgorithm.generatePublications();
        var subLogs = pubSubAlgorithm.generateSubscriptions();

        pubSubAlgorithm.createFilesContainingPubsAndSubs();

        Instant end = Instant.now();

        Duration duration = Duration.between(start, end);
        long resultTimeMillis = duration.toMillis();
        System.out.println("Generation took " + resultTimeMillis + " ms");
      //  log.info("Generation took {} ms", resultTimeMillis);

        return new PublisherRunResultDto(
                isParallel ? "PARALLEL" : "NON-PARALLEL",
                subLogs,
                String.valueOf(resultTimeMillis)
        );
    }
}
