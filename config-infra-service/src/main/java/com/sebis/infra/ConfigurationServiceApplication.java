package com.sebis.infra;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.config.server.EnableConfigServer;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

@EnableDiscoveryClient
@EnableConfigServer
@SpringBootApplication
public class ConfigurationServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(ConfigurationServiceApplication.class, args);
	}
}
