package com.sebis.helper.config;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;



/**
 * Created by sohaib on 30/03/17.
 */
@Configuration
public class MvcConfig extends WebMvcConfigurerAdapter {

    @Autowired
    Environment env;

    @Bean(name = "dataSource")
    public DriverManagerDataSource dataSource() {
        DriverManagerDataSource driverManagerDataSource = new DriverManagerDataSource();
        driverManagerDataSource.setDriverClassName("com.mysql.jdbc.Driver");
        driverManagerDataSource.setUrl("jdbc:mysql://" + env.getProperty("jdbc.url") + ":3306/" + env.getProperty("jdbc.db"));
        driverManagerDataSource.setUsername(env.getProperty("jdbc.user"));
        driverManagerDataSource.setPassword(env.getProperty("jdbc.password"));
        return driverManagerDataSource;
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
