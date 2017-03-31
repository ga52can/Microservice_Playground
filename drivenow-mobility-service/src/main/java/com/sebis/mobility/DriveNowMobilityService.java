package com.sebis.mobility;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

@EnableDiscoveryClient
@SpringBootApplication
public class DriveNowMobilityService {

    public static void main(String[] args) {
        SpringApplication.run(DriveNowMobilityService.class, args);
    }
}
