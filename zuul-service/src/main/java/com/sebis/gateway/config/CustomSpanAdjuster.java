package com.sebis.gateway.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.sleuth.Span;
import org.springframework.cloud.sleuth.SpanAdjuster;
import org.springframework.context.annotation.Bean;

import javax.servlet.http.HttpServletRequest;

/**
 * Created by kleehausm on 25.10.2017.
 */
public class CustomSpanAdjuster implements SpanAdjuster {

    private HttpServletRequest request;

    @Autowired
    public CustomSpanAdjuster(HttpServletRequest request) {
        this.request = request;
    }

    @Override
    public Span adjust(Span span) {

        span.tag("usersession", request.getHeader("usersession"));
        return span;
    }

}