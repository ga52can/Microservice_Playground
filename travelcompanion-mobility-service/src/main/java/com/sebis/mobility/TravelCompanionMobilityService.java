package com.sebis.mobility;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

@EnableDiscoveryClient
@SpringBootApplication
public class TravelCompanionMobilityService {

    public static void main(String[] args) {
        SpringApplication.run(TravelCompanionMobilityService.class, args);
    }
}
