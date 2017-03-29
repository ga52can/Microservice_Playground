package com.sebis.business;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

@EnableDiscoveryClient
@SpringBootApplication
public class BusinessCoreApplication {

    public static void main(String[] args) {
        SpringApplication.run(BusinessCoreApplication.class, args);
    }
}
