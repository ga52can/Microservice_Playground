package com.sebis.mobility.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.netflix.eureka.EurekaDiscoveryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;

import javax.annotation.PostConstruct;

/**
 * Created by sohaib on 27/03/17.
 */

@Configuration
public class MvcConfig extends WebMvcConfigurerAdapter {

    @Override
    public void addViewControllers(ViewControllerRegistry registry) {
        registry.addViewController("/routes").setViewName("route");
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new CustomObjectMapper();
    }

    @Bean(name = "dataSource")
    public DriverManagerDataSource dataSource() {
        DriverManagerDataSource driverManagerDataSource = new DriverManagerDataSource();
        driverManagerDataSource.setDriverClassName("com.mysql.jdbc.Driver");
        driverManagerDataSource.setUrl("jdbc:mysql://52.40.172.29:3306/sebis");
        driverManagerDataSource.setUsername("sebis_user");
        driverManagerDataSource.setPassword("sebisTUM12417582");
        return driverManagerDataSource;
    }

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

}