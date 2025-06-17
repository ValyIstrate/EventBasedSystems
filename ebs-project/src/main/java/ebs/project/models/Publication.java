package ebs.project.models;

import java.time.LocalDate;

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

     public String getField(String name) {
        switch (name) {
            case "stationId":
                return String.valueOf(stationId);
            case "city":
                return city;
            case "temp":
                return String.valueOf(temperature);
            case "rain":
                return String.valueOf(rainProbability);
            case "wind":
                return String.valueOf(windSpeed);
            case "direction":
                return direction;
            case "date":
                return date.toString();
            default:
                throw new IllegalArgumentException("Unknown field: " + name);
        }
    }

    public String getCity() {
        return city;
    }

    public Long getTemperature() {
        return temperature;
    }

    public Double getRainProbability() {
        return rainProbability;
    }

    public Long getWindSpeed() {
        return windSpeed;
    }


}
