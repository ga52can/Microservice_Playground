package com.sebis.gateway.config;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import de.sebis.sleuthextension.CustomFilter;
import de.sebis.sleuthextension.CustomTraceHandlerInterceptor;

import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;

/**
 * Created by sohaib on 27/03/17.
 */

@Configuration
public class MvcConfig extends WebMvcConfigurerAdapter {

	@Autowired
	Tracer tracer;

	@Autowired
	BeanFactory beanFactory;

	@Bean
	public CustomTraceHandlerInterceptor customTraceHandlerInterceptor(BeanFactory beanFactory) {
		return new CustomTraceHandlerInterceptor(beanFactory, tracer);
	}

	@Autowired
	Environment env;

	@Override
	public void addViewControllers(ViewControllerRegistry registry) {
		registry.addViewController("/home").setViewName("home");
		registry.addViewController("/").setViewName("home");
		registry.addViewController("/hello").setViewName("hello");
		registry.addViewController("/login").setViewName("login");
	}

	@Bean(name = "dataSource")
	public DriverManagerDataSource dataSource() {
		DriverManagerDataSource driverManagerDataSource = new DriverManagerDataSource();
		driverManagerDataSource.setDriverClassName("com.mysql.jdbc.Driver");
		driverManagerDataSource
				.setUrl("jdbc:mysql://" + env.getProperty("jdbc.url") + ":3306/" + env.getProperty("jdbc.db"));
		driverManagerDataSource.setUsername(env.getProperty("jdbc.user"));
		driverManagerDataSource.setPassword(env.getProperty("jdbc.password"));
		return driverManagerDataSource;
	}

	@Bean
	CustomFilter customFilter() {
		return new CustomFilter(tracer);
	}
}
