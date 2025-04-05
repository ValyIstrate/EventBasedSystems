package com.ebs.publisher.features.publisher;

import java.util.List;

public record PublisherRunResultDto (
        String runType,
        List<String> fieldValidations,
        String executeDuration
) {
}
