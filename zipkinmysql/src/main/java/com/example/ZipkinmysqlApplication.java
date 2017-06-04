package com.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import zipkin.server.*;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

@EnableDiscoveryClient
@RefreshScope
@SpringBootApplication
@EnableZipkinServer
public class ZipkinmysqlApplication {

	public static void main(String[] args) {
		SpringApplication.run(ZipkinmysqlApplication.class, args);
	}


}
