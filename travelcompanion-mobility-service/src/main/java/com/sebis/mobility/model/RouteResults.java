package com.sebis.mobility.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by sohaib on 30/03/17.
 */
public class RouteResults {
    double distance;
    ArrayList<Route> routes;

    public RouteResults() {
        routes = new ArrayList<>();
    }

    public RouteResults(double distance) {
        this.distance = distance;
        routes = new ArrayList<>();
    }

    public RouteResults(double distance, ArrayList<Route> routes) {
        this.distance = distance;
        this.routes = routes;
    }

    public void addRoute(Route route) {
        routes.add(route);
    }

    public void addRoutes(List<Route> routes) {
        this.routes.addAll(routes);
    }

    public double getDistance() {
        return distance;
    }

    public void setDistance(double distance) {
        this.distance = distance;
    }

    public ArrayList<Route> getRoutes() {
        return routes;
    }

    public void setRoutes(ArrayList<Route> routes) {
        this.routes = routes;
    }



}
