package com.sebis.helper.model;

import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * Created by sohaib on 30/03/17.
 */
@Entity
public class City {
    @Id
    private int cityId;
    private String cityName;
    private double latitude;
    private double longitude;

    public City(int cityId, String cityName, double latitude, double longitude) {
        this.cityId = cityId;
        this.cityName = cityName;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public String getCityName() {
        return cityName;
    }

    public int getCityId() {
        return cityId;
    }

    public double getLatitude() {
        return latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    private double getDistanceFromLatLonInKm(double lat1, double lon1, double lat2, double lon2) {
        int R = 6371; // Radius of the earth in km
        double dLat = deg2rad(lat2-lat1);  // deg2rad below
        double dLon = deg2rad(lon2-lon1);
        double a =
                Math.sin(dLat/2) * Math.sin(dLat/2) +
                        Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) *
                                Math.sin(dLon/2) * Math.sin(dLon/2)
                ;
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
        return R * c; // Distance in km
    }

    private double deg2rad(double deg) {
        return deg * (Math.PI/180);
    }

    public double calculateDistance(City other) {
        return getDistanceFromLatLonInKm(this.latitude, this.longitude, other.latitude, other.longitude);
    }
}
