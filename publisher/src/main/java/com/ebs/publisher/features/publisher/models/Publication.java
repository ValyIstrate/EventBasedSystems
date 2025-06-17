package com.ebs.publisher.features.publisher.models;

import com.ebs.publisher.features.publisher.proto_classes.MessageProto;
import com.ebs.publisher.features.publisher.proto_classes.PublicationProto;

import java.time.LocalDate;
import java.time.Month;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Random;

import static com.ebs.publisher.features.publisher.proto_classes.MessageProto.MsgProto.MessageCase.PUBLICATION;

public class Publication implements ProtobufConvertible {

    Integer stationId;
    String city;
    Integer temperature;
    Double rainProbability;
    Integer windSpeed;
    String direction;
    LocalDate date;

    private static final Integer NUMBER_OF_STATIONS = 10;
    private static final List<String> CITIES = List.of("Bucharest", "Cluj", "Iasi", "Timisoara", "Oradea");
    private static final List<String> DIRECTIONS = List.of("N", "S", "E", "W", "NE", "NW", "SE", "SW");

    public Publication() {
        Random random = new Random();

        this.stationId = random.nextInt(NUMBER_OF_STATIONS);
        this.city = CITIES.get(random.nextInt(CITIES.size()));
        this.temperature = random.nextInt(35) + 1;
        this.rainProbability = random.nextDouble(100);
        this.windSpeed = random.nextInt(50);
        this.direction = DIRECTIONS.get(random.nextInt(DIRECTIONS.size()));

        LocalDate start = LocalDate.of(2024, Month.JANUARY, 1);
        long days = ChronoUnit.DAYS.between(start, LocalDate.now());
        this.date = start.plusDays(new Random().nextInt((int) days + 1));
    }

    @Override
    public String toString() {
        return String.format("{(stationId,%s);(city,%s);(temp,%s);(rain,%s);(wind,%s);(direction,%s);(date,%s)}",
                stationId, city, temperature, rainProbability, windSpeed, direction, date);
    }

    public PublicationProto.PubProto buildProto() {
        PublicationProto.PubProto.Builder proto = PublicationProto.PubProto.newBuilder();
        proto.setStationId(this.stationId);
        proto.setCity(this.city);
        proto.setTemperature(this.temperature);
        proto.setRainProbability(this.rainProbability);
        proto.setWindSpeed(this.windSpeed);
        proto.setDirection(this.direction);
        proto.setDate(this.date.toString());
        return proto.build();
    }

    @Override
    public MessageProto.MsgProto toProto() {
        var protoBuilder = MessageProto.MsgProto.newBuilder();
        protoBuilder.setMessageType(String.valueOf(PUBLICATION));
        protoBuilder.setPublication(this.buildProto());
        return protoBuilder.build();
    }
}
