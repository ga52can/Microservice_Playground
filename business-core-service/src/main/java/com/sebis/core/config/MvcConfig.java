package com.sebis.core.config;

import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import de.sebis.sleuthextension.CustomFilter;
import de.sebis.sleuthextension.CustomTraceHandlerInterceptor;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;

/**
 * Created by sohaib on 27/03/17.
 */

@Configuration
public class MvcConfig extends WebMvcConfigurerAdapter {

    @Override
    public void addViewControllers(ViewControllerRegistry registry) {
        registry.addViewController("/businesses/list").setViewName("service");
    }

    @Autowired
    Tracer tracer;
    
	@Autowired BeanFactory beanFactory;

	@Bean
	public CustomTraceHandlerInterceptor customTraceHandlerInterceptor(BeanFactory beanFactory) {
		return new CustomTraceHandlerInterceptor(beanFactory,tracer);
	}

    @Bean
    CustomFilter customFilter(){
    	return new CustomFilter(tracer);
    }
}
