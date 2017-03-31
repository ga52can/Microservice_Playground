package com.sebis.mobility.model;

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
}
