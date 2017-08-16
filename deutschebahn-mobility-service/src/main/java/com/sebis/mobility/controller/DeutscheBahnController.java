package com.sebis.mobility.controller;

import com.sebis.mobility.model.City;
import com.sebis.mobility.model.Route;
import com.sebis.mobility.model.Travel;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletResponse;
import java.util.List;

/**
 * Created by sohaib on 30/03/17.
 */
@RestController
public class DeutscheBahnController {

	@Autowired
	JdbcTemplate jdbcTemplate;

	@RequestMapping(value = { "/routes", "/deutschebahn-mobility-service/routes" }, method = RequestMethod.GET)
	@ResponseBody
	public ModelAndView showCities() {
		ModelAndView model = new ModelAndView("route");
		List<City> cities = jdbcTemplate.query("SELECT city_id, city_name, latitude, longitude FROM cities",
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

	@RequestMapping(value = { "/getroutes", "/deutschebahn-mobility-service/getroutes" }, method = { RequestMethod.GET,
			RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public List<Route> findDistance(@RequestParam("origin") int cityOrigin,
			@RequestParam("destination") int cityDestination,
			@RequestParam(value = "info", required = false) String info, HttpServletResponse response) {

		if (info != null) {
			// simulate uncaught NullpointerException
			if (info.contains("db-distance-nullpointer")) {
				String nullPointer = null;
				nullPointer.charAt(5);
			}

			// simulate performance issue
			if (info.contains("db-distance-25msDelay")) {
				try {
					Thread.sleep(25);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (info.contains("db-distance-50msDelay")) {
				try {
					Thread.sleep(50);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (info.contains("db-distance-100msDelay")) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (info.contains("db-distance-200msDelay")) {
				try {
					Thread.sleep(200);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		List<Route> routes = jdbcTemplate.query("SELECT * FROM deutschebahn_routes where origin=? AND destination=?",
				new Integer[] { cityOrigin, cityDestination }, (rs, rowNum) -> {
					int routeId = rs.getInt(1);
					int origin = rs.getInt(2);
					int destination = rs.getInt(3);
					DateTime travelDate = new DateTime(rs.getDate(4));
					String partner = rs.getString(5);
					int cost = rs.getInt(6);
					return new Route(routeId, origin, destination, travelDate, partner, cost, "deutsche-bahn");
				});
		return routes;
	}
}
