package com.ebs.publisher.features.publisher;

import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/publish")
public class PublisherController {

    private final PublisherService publisherService;
    public PublisherController(PublisherService publisherService) {
        this.publisherService = publisherService;
    }
    @PostMapping(path = "", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<PublisherRunResultDto> runNonParallelMessageGeneration(
            @RequestParam("numberOfSubs") int numberOfSubs,
            @RequestParam("numberOfPubs") int numberOfPubs,
            @RequestParam("cityRate") int cityRate,
            @RequestParam("tempRate") int tempRate,
            @RequestParam("rainRate") int rainRate,
            @RequestParam("windRate") int windRate,
            @RequestParam("directionRate") int directionRate,
            @RequestParam("dateRate") int dateRate,
            @RequestParam("isParallel") boolean isParallel
    ) {
        return new ResponseEntity<>(
                publisherService.runGeneration(numberOfSubs, numberOfPubs, cityRate, tempRate,
                        rainRate, windRate, directionRate, dateRate, isParallel),
                HttpStatus.OK
        );
    }
}
