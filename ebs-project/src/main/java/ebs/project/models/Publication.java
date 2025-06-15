package ebs.project.models;

import java.time.LocalDate;
import java.util.List;

public class Publication {

    Long stationId;
    String city;
    Long temperature;
    Double rainProbability;
    Long windSpeed;
    String direction;
    LocalDate date;

    public Publication(Long stationId, String city, Long temperature, Double rainProbability, Long windSpeed,
                       String direction, String date) {
        this.stationId = stationId;
        this.city = city;
        this.temperature = temperature;
        this.rainProbability = rainProbability;
        this.windSpeed = windSpeed;
        this.direction = direction;
        this.date = LocalDate.parse(date);
    }

    @Override
    public String toString() {
        return String.format("{(stationId,%s);(city,%s);(temp,%s);(rain,%s);(wind,%s);(direction,%s);(date,%s)}",
                stationId, city, temperature, rainProbability, windSpeed, direction, date);
    }
}
