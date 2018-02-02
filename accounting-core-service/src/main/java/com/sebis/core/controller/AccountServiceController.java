package com.sebis.core.controller;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RefreshScope
@RestController
public class AccountServiceController {

    @Value("${name}")
    private String serviceName;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final ExecutorService pool = Executors.newFixedThreadPool(10);
    private static Logger log = LogManager.getLogger(AccountServiceController.class.getName());


    @RequestMapping(
            value = {"/{service}/{route_id}/book", "/accounting-core-service/{service}/{route_id}/book"},
            method = RequestMethod.GET)
    @ResponseBody
    public ModelAndView book(@PathVariable(value="service") String serviceName,
                             @PathVariable(value = "route_id") int routeId,
                             HttpServletResponse response) {
        ModelAndView model = new ModelAndView("book_result");
        String deutscheBahnService = "deutsche-bahn";
        String driveNowService = "drive-now";
        if (!(serviceName.equals(deutscheBahnService) || serviceName.equals(driveNowService))) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            model.addObject("result", "invalid_request");
        } else {
            try {
                String tableName = serviceName.equals(deutscheBahnService) ?
                        "deutschebahn_routes" : "drivenow_routes";
                String serviceProvider = serviceName.equals(deutscheBahnService) ? "deutschebahn" : "drivenow";
                List result = jdbcTemplate.queryForList(
                        "SELECT * FROM " + tableName + " WHERE route_id=?",
                        routeId);
                if (result.size() > 0) {
                    jdbcTemplate.update("INSERT INTO booked_routes(service_provider, route_id, booking_time) " +
                            "VALUES (?, ?, NOW())", serviceProvider, routeId);
                    model.addObject("result", "booking success");
                } else {
                    model.addObject("result", "route now found");
                }
            } catch (Exception e) {
                return model.addObject("result", "invalid_request");
            }
        }
        return model;
    }

}
