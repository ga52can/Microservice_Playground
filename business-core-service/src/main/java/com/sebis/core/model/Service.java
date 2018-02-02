package com.sebis.core.model;


/**
 * Created by sohaib on 29/03/17.
 */


public class Service {

    private String serviceName;
    private String url;

    public Service(String serviceName, String url) {
        this.serviceName = serviceName;
        this.url = url;
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getUrl() {
        return url;
    }
}
