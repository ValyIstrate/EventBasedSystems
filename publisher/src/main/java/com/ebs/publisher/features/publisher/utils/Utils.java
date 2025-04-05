package com.ebs.publisher.features.publisher.utils;

import java.util.List;

public class Utils {

    public static List<String> getMetadataKeys() {
        return List.of("city", "temp", "rain", "wind", "direction", "date");
    }

    public static List<String> getCities() {
        return List.of("Bucharest", "Cluj", "Iasi", "Timisoara", "Oradea");
    }

    public static List<String> getDirections() {
        return List.of("N", "S", "E", "W", "NE", "NW", "SE", "SW");
    }
}
