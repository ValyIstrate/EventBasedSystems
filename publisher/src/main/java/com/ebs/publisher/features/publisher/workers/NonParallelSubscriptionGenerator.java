package com.ebs.publisher.features.publisher.workers;

import com.ebs.publisher.features.publisher.models.Subscription;
import com.ebs.publisher.features.publisher.utils.Utils;
import lombok.Getter;

import java.time.LocalDate;
import java.time.Month;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.stream.IntStream;

import static com.ebs.publisher.features.publisher.algorithm.PubSubAlgorithm.minimumCity;

@Getter
public class NonParallelSubscriptionGenerator {

    private int rate;
    private String metadata;
    private int numberOfSubs;
    private List<Subscription> subscriptions;

    private static final List<String> EQ_OPERATOR = List.of("=", "!=");
    private static final List<String> NUM_OPERATORS = List.of("<", "<=", "=", ">", ">=");

    public NonParallelSubscriptionGenerator(int rate, String metadata, int numberOfSubs) {
        this.rate = rate;
        this.metadata = metadata;
        this.numberOfSubs = numberOfSubs;
        this.subscriptions = new ArrayList<>();
    }

    public void generateSubs() {
        int actualItems = (rate * numberOfSubs) / 100;
        Random random = new Random();

        IntStream.range(0, actualItems).forEach(i -> {
            Subscription subscription = new Subscription();

            if (Objects.equals(metadata, "city")) {
                processCityField(random, subscription);
                return;
            }

            if (Objects.equals(metadata, "date")) {
                processDateField(random, subscription);
                return;
            }

            if (Objects.equals(metadata, "direction")) {
                processDirectionField(random, subscription);
                return;
            }

            Object numericalValue = null;
            if (Objects.equals(metadata, "temp")) {
                numericalValue = random.nextInt(35) + 1;
            }

            if (Objects.equals(metadata, "rain")) {
                numericalValue = random.nextDouble(100);
            }

            if (Objects.equals(metadata, "wind")) {
                numericalValue = random.nextInt(50);
            }

            if (Objects.nonNull(numericalValue)) {
                String operator = NUM_OPERATORS.get(random.nextInt(EQ_OPERATOR.size()));
                subscription.addOperator(operator);
                subscription.addInfo(metadata, numericalValue.toString());
                subscriptions.add(subscription);
            }
        });
    }

    private void processDirectionField(Random random, Subscription subscription) {
        var directions = Utils.getDirections();
        String direction = directions.get(random.nextInt(directions.size()));
        String directionOperator = EQ_OPERATOR.get(random.nextInt(EQ_OPERATOR.size()));
        subscription.addOperator(directionOperator);
        subscription.addInfo(metadata, direction);
        subscription.addInfo(metadata, directionOperator);
        subscriptions.add(subscription);
    }

    private void processDateField(Random random, Subscription subscription) {
        LocalDate start = LocalDate.of(2024, Month.JANUARY, 1);
        long days = ChronoUnit.DAYS.between(start, LocalDate.now());
        LocalDate randomDate = start.plusDays(new Random().nextInt((int) days + 1));
        String operator = NUM_OPERATORS.get(random.nextInt(NUM_OPERATORS.size()));
        subscription.addOperator(operator);
        subscription.addInfo(metadata, randomDate.toString());
        subscriptions.add(subscription);
    }

    private void processCityField(Random random, Subscription subscription) {
        var cities = Utils.getCities();
        String city = cities.get(random.nextInt(cities.size()));
        String cityOperator = random.nextInt(100) > minimumCity
                ? EQ_OPERATOR.get(1) : EQ_OPERATOR.get(0);
        subscription.addOperator(cityOperator);
        subscription.addInfo(metadata, city);
        subscriptions.add(subscription);
    }
}
