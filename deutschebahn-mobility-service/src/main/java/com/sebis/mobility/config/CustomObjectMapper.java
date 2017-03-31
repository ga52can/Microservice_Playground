package com.sebis.mobility.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;

/**
 * Created by sohaib on 31/03/17.
 */
@Service
@Primary
public class CustomObjectMapper extends ObjectMapper {
    public CustomObjectMapper() {
        this.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        this.setDateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssz"));
        this.registerModule(new JodaModule());
    }
}