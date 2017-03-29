package com.sebis.helper;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

@EnableDiscoveryClient
@SpringBootApplication
public class MapsHelperApplication {

    public static void main(String[] args) {
        SpringApplication.run(MapsHelperApplication.class, args);
    }
}
