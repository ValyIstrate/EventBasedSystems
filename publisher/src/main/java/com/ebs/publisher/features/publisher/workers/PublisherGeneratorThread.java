package com.ebs.publisher.features.publisher.workers;

import com.ebs.publisher.features.publisher.models.Publication;

import java.util.ArrayList;
import java.util.List;

public class PublisherGeneratorThread extends Thread{
    private final int numberOfPublishers;
    private List<Publication> publications;

    public PublisherGeneratorThread(int numberOfPublishers) {
        this.numberOfPublishers = numberOfPublishers;
    }

    @Override
    public void run() {
        publications = new ArrayList<>();
        for (int i = 0; i < numberOfPublishers; i++) {
            Publication publication = new Publication();
            publications.add(publication);
        }
    }

    public List<Publication> getPublications() {
        return publications;
    }

}
