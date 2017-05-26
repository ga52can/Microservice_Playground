package com.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.cloud.sleuth.zipkin.stream.EnableZipkinStreamServer;

import zipkin.server.*;

@RefreshScope
@SpringBootApplication
@EnableZipkinStreamServer
public class ZipkinmysqlApplication {

	public static void main(String[] args) {
		SpringApplication.run(ZipkinmysqlApplication.class, args);
	}


}
