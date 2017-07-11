package com.sebis.core.config;

import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import de.sebis.sleuthextension.CustomFilter;
import de.sebis.sleuthextension.CustomSpanAdjuster;
import de.sebis.sleuthextension.CustomTraceHandlerInterceptor;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.sleuth.SpanAdjuster;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;

/**
 * Created by sohaib on 27/03/17.
 */

@Configuration
public class MvcConfig extends WebMvcConfigurerAdapter {

	Tracer tracer;

	@Autowired
	BeanFactory beanFactory;

	Tracer tracer() {
		if (this.tracer == null) {
			this.tracer = this.beanFactory.getBean(Tracer.class);
		}
		return this.tracer;
	}

	@Bean
	public CustomTraceHandlerInterceptor customTraceHandlerInterceptor(BeanFactory beanFactory) {
		return new CustomTraceHandlerInterceptor(beanFactory, tracer());
	}

	@Bean
	public SpanAdjuster customSpanAdjuster() {
		return new CustomSpanAdjuster();
	}

	@Bean
	CustomFilter customFilter() {
		return new CustomFilter(tracer());
	}
	
    @Override
    public void addViewControllers(ViewControllerRegistry registry) {
        registry.addViewController("/businesses/list").setViewName("service");
    }

    
}
