package com.sebis.core.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.cloud.netflix.eureka.EurekaDiscoveryClient;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RefreshScope
@RestController
public class BusinessListController {

    @Autowired
    EurekaDiscoveryClient discoveryClient;
    
    @Value("${name}")
    private String serviceName;
    
    @RequestMapping("/info/name")
    public String getInfo(){
        return this.serviceName;
    }

    @RequestMapping("/businesses/list")
    @ResponseBody
    public String getServices() {
        StringBuilder builder = new StringBuilder("");
    	discoveryClient.getServices()
                .stream()
                .filter(service -> service.endsWith("mobility-service"))
                .forEach(builder::append);
        return builder.toString();
    }
    
}
