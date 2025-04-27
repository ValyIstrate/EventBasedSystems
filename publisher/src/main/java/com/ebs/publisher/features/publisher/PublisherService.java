package com.ebs.publisher.features.publisher;

import com.ebs.publisher.features.publisher.algorithm.PubSubGenerationAlgorithm;
import com.ebs.publisher.features.publisher.aws.messaging.sender.PubSubSender;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;

@Service
@Slf4j
@RequiredArgsConstructor
public class PublisherService {

    private final PubSubSender pubSubSender;

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

    @Scheduled(fixedRate = 10000)
    public void runGeneration() {
        PubSubGenerationAlgorithm pubSubGenerationAlgorithm = new PubSubGenerationAlgorithm();

        pubSubGenerationAlgorithm.init(10, 1000, 90, 50, 30, 30,
                25, 100, false);

        pubSubGenerationAlgorithm.generatePublications();
        pubSubGenerationAlgorithm.generateSubscriptions();

        pubSubGenerationAlgorithm.getGeneratedPublications()
                .forEach(pubSubSender::sendMessage);

        pubSubGenerationAlgorithm.getGeneratedSubscriptions()
                .forEach(pubSubSender::sendMessage);
    }
}
