package com.sebis.core.controller;

import com.sebis.core.model.Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.cloud.netflix.eureka.EurekaDiscoveryClient;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

import java.util.stream.Collectors;

@RefreshScope
@RestController
public class BusinessListController {

    @Autowired
    private EurekaDiscoveryClient discoveryClient;
    
    @Value("${name}")
    private String serviceName;
    
    @RequestMapping("/info/name")
    public String getInfo(){
        return this.serviceName;
    }

    @RequestMapping(value = { "/businesses/list" }, method = RequestMethod.GET)
    @ResponseBody
    public ModelAndView getServices() {
        ModelAndView model = new ModelAndView();
        model.addObject(
                "services",
                discoveryClient
                        .getServices()
                        .stream()
                        .filter(service -> service.endsWith("mobility-service"))
                        .map(service ->
                                new Service(
                                        service,
                                        String.format("%s/routes", service))).collect(Collectors.toList())
        );
        model.setViewName("service");
        return model;
    }
    
}
