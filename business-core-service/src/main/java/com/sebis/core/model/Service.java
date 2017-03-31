package com.sebis.core.model;

import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * Created by sohaib on 29/03/17.
 */

@Entity
public class Service {
    @Id
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
