package com.ebs.publisher.features.publisher;

import com.ebs.publisher.features.publisher.algorithm.PubSubGenerationAlgorithm;
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
        PubSubGenerationAlgorithm pubSubGenerationAlgorithm = new PubSubGenerationAlgorithm();

        pubSubGenerationAlgorithm.init(numberOfSubs, numberOfPubs, cityRate, tempRate, rainRate, windRate,
                directionRate, dateRate, isParallel);

        pubSubGenerationAlgorithm.generatePublications();
        var subLogs = pubSubGenerationAlgorithm.generateSubscriptions();

        pubSubGenerationAlgorithm.createFilesContainingPubsAndSubs();

        Instant end = Instant.now();

        Duration duration = Duration.between(start, end);
        long resultTimeMillis = duration.toMillis();
        log.info("Generation took {} ms", resultTimeMillis);

        return new PublisherRunResultDto(
                isParallel ? "PARALLEL" : "NON-PARALLEL",
                subLogs,
                String.valueOf(resultTimeMillis)
        );
    }
}
