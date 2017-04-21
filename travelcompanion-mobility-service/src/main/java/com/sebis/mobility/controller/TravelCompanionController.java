package com.sebis.mobility.controller;

import com.sebis.mobility.model.City;
import com.sebis.mobility.model.Route;
import com.sebis.mobility.model.RouteResults;
import com.sebis.mobility.model.Travel;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@RefreshScope
@RestController
public class TravelCompanionController {

    @Value("${name}")
    private String serviceName;

    @Value("${maps-url}")
    private String mapsUrl;
    @Value(("${drive-now-url}"))
    private String driveNowUrl;
    @Value(("${deutsche-bahn-url}"))
    private String deutscheBahnUrl;

    @Autowired
    private JdbcTemplate jdbcTemplate;
    private @Autowired
    RestTemplate restTemplate;

    private final static String DRIVE_NOW = "drive-now";
    private final static String DEUTSCHE_BAHN = "deutsche-bahn";
    private final ExecutorService pool = Executors.newFixedThreadPool(10);
    private static Logger log = Logger.getLogger(TravelCompanionController.class.getName());


    @RequestMapping(value = {"/routes", "/travelcompanion-mobility-service/routes"}, method = RequestMethod.GET)
    @ResponseBody
    public ModelAndView showCities(HttpServletRequest request) {
        ModelAndView model = new ModelAndView("route");
        List<City> cities =
                jdbcTemplate.query(
                        "SELECT city_id, city_name, latitude, longitude FROM cities",
                        (resultSet, i) -> {
                            int id = resultSet.getInt(1);
                            String cityName = resultSet.getString(2);
                            double latitude = resultSet.getDouble(3);
                            double longitude = resultSet.getDouble(4);
                            return new City(id, cityName, latitude, longitude);
                        });
        model.addObject("cities", cities);
        model.addObject("travel", new Travel());
        return model;
    }

    @Async
    public Future<List<Route>> getRoutes(final String baseUrl, Travel travel) {
        ResponseEntity<List<Route>> routes =
                restTemplate.exchange(
                        baseUrl + "/getroutes?origin={origin}&destination={destination}",
                        HttpMethod.GET,
                        null,
                        new ParameterizedTypeReference<List<Route>>(){},
                        travel.getOrigin(),
                        travel.getDestination()
                );
        List<Route> results = routes.getStatusCode() == HttpStatus.OK ? routes.getBody() : new ArrayList<>();
        return new AsyncResult<>(results);
    }

    @PostMapping(value = {"/getroutes", "/travelcompanion-mobility-service/getroutes"}, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    @CrossOrigin
    public RouteResults getRoutes(@ModelAttribute Travel travel, HttpServletResponse response) {
        RouteResults routeResults = new RouteResults();

        ResponseEntity<HashMap> mapsResponse =
                restTemplate.getForEntity(
                        mapsUrl + "/distance?origin={origin}&destination={destination}",
                        HashMap.class, travel.getOrigin(), travel.getDestination());
        if (mapsResponse.getStatusCode() == HttpStatus.OK) {
            routeResults.setDistance((double) mapsResponse.getBody().get("result"));
        }
        Future<List<Route>> driveNowRoutes = getRoutes(driveNowUrl, travel);
        Future<List<Route>> deutscheBahnRoutes = getRoutes(deutscheBahnUrl, travel);
        try {
            routeResults.addRoutes(driveNowRoutes.get());
            routeResults.addRoutes(deutscheBahnRoutes.get());
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error getting routes from partner services");
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
        return routeResults;
    }

}
