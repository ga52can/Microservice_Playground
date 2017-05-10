package com.sebis.mobility.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;

/**
 * Created by sohaib on 27/03/17.
 */

@Configuration
public class MvcConfig extends WebMvcConfigurerAdapter {

    @Autowired
    Environment env;

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
        driverManagerDataSource.setUrl("jdbc:mysql://" + env.getProperty("jdbc.url") + ":3306/" + env.getProperty("jdbc.db"));
        driverManagerDataSource.setUsername(env.getProperty("jdbc.user"));
        driverManagerDataSource.setPassword(env.getProperty("jdbc.password"));
        return driverManagerDataSource;
    }

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

}

