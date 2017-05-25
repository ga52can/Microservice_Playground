package com.sebis.mobility.model;

import org.joda.time.DateTime;

import javax.persistence.Entity;
import javax.persistence.Id;
/**
 * Created by sohaib on 31/03/17.
 */
@Entity
public class Route {
    @Id
    private int routeId;
    private int origin;
    private int destination;
    private DateTime date;
    private String partner;
    private int cost;
    private String serviceProvider;

    public Route() {}

    public Route(int routeId,
                 int origin,
                 int destination,
                 DateTime date,
                 String partner,
                 int cost,
                 String serviceProvider) {
        this.routeId = routeId;
        this.origin = origin;
        this.destination = destination;
        this.date = date;
        this.partner = partner;
        this.cost = cost;
        this.serviceProvider = serviceProvider;
    }

    public int getRouteId() {
        return routeId;
    }

    public void setRouteId(int routeId) {
        this.routeId = routeId;
    }

    public int getOrigin() {
        return origin;
    }

    public void setOrigin(int origin) {
        this.origin = origin;
    }

    public int getDestination() {
        return destination;
    }

    public void setDestination(int destination) {
        this.destination = destination;
    }

    public DateTime getDate() {
        return date;
    }

    public void setDate(DateTime date) {
        this.date = date;
    }

    public String getPartner() {
        return partner;
    }

    public void setPartner(String partner) {
        this.partner = partner;
    }

    public int getCost() {
        return cost;
    }

    public void setCost(int cost) {
        this.cost = cost;
    }

    public String getServiceProvider() {
        return serviceProvider;
    }

    public void setServiceProvider(String serviceProvider) {
        this.serviceProvider = serviceProvider;
    }
}
