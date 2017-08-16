package com.sebis.helper.controller;

import com.sebis.helper.model.City;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.Locale;

/**
 * Created by sohaib on 30/03/17.
 */
@RestController
public class MapsController {

	@Autowired
	JdbcTemplate jdbcTemplate;

	@Autowired
	Tracer tracer;

	@RequestMapping(value = { "/distance",
			"/maps-helper-service/distance" }, method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public String findDistance(@RequestParam("origin") int cityOrigin, @RequestParam("destination") int cityDestination,
			@RequestParam(value = "info") String info, HttpServletResponse response) {
		List<City> cities = jdbcTemplate.query("SELECT * FROM cities where city_id=? OR city_id=?",
				new Integer[] { cityOrigin, cityDestination }, (rs, rowNum) -> {
					int id = rs.getInt(1);
					String cityName = rs.getString(2);
					double latitude = rs.getDouble(3);
					double longitude = rs.getDouble(4);
					return new City(id, cityName, latitude, longitude);
				});
		if (info != null) {
			// simulate uncaught NullpointerException
			if (info.contains("mapsController-distance-nullpointer")) {
				String nullPointer = null;
				nullPointer.charAt(5);
			}

			// simulate performance issue
			if (info.contains("mapsController-distance-25msDelay")) {
				try {
					Thread.sleep(25);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (info.contains("mapsController-distance-50msDelay")) {
				try {
					Thread.sleep(50);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (info.contains("mapsController-distance-100msDelay")) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (info.contains("mapsController-distance-200msDelay")) {
				try {
					Thread.sleep(200);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		if (cities.size() == 2) {
			return String.format(Locale.US, "{\"result\": %.2f}", cities.get(0).calculateDistance(cities.get(1)));
		} else {
			response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			return "{\"error\" : \"City not found in database\"}";
		}
	}
}
