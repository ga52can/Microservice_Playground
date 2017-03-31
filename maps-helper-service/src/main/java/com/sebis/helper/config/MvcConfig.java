package com.sebis.helper.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

/**
 * Created by sohaib on 30/03/17.
 */
@Configuration
public class MvcConfig extends WebMvcConfigurerAdapter {

    @Bean(name = "dataSource")
    public DriverManagerDataSource dataSource() {
        DriverManagerDataSource driverManagerDataSource = new DriverManagerDataSource();
        driverManagerDataSource.setDriverClassName("com.mysql.jdbc.Driver");
        driverManagerDataSource.setUrl("jdbc:mysql://52.40.172.29:3306/sebis");
        driverManagerDataSource.setUsername("sebis_user");
        driverManagerDataSource.setPassword("sebisTUM12417582");
        return driverManagerDataSource;
    }
}
