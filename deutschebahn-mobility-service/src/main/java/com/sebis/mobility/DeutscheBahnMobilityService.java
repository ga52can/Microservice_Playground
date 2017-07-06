package com.sebis.mobility;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;


@EnableDiscoveryClient
@SpringBootApplication
public class DeutscheBahnMobilityService {

    public static void main(String[] args) {
        SpringApplication.run(DeutscheBahnMobilityService.class, args);
    }
    
    
}
