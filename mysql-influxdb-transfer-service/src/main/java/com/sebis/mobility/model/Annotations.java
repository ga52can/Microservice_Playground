package com.sebis.mobility.model;

import org.joda.time.DateTime;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;

/**
 * Created by sohaib on 31/03/17.
 */

@Entity
public class Annotations {
    private long trace_id;
    private long span_id;
    private String a_key;
    private String a_value;
    private long a_type;
    private long endpoint_ipv4;
    private long endpoint_ipv6;
    private int endpoint_port;
    @Id private long a_timestamp;
    private String endpoint_service_name;

    public Annotations() {}

    public Annotations(long trace_id,
                       long span_id,
                       String a_key,
                       String a_value,
                       long a_type,
                       long a_timestamp,
                       long endpoint_ipv4,
                       long endpoint_ipv6,
                       int endpoint_port,
                       String endpoint_service_name
                       ) {
        this.trace_id = trace_id;
        this.span_id = span_id;
        this.a_key = a_key;
        this.a_value = a_value;
        this.a_type = a_type;
        this.endpoint_ipv4 = endpoint_ipv4;
        this.endpoint_ipv6 = endpoint_ipv6;
        this.endpoint_port = endpoint_port;
        this.a_timestamp = a_timestamp;
        this.endpoint_service_name = endpoint_service_name;
    }

    public long getTrace_id() {
        return trace_id;
    }

    public void setTrace_id(long trace_id) {
        this.trace_id = trace_id;
    }

    public long getSpan_id() {
        return span_id;
    }

    public void setSpan_id(long span_id) {
        this.span_id = span_id;
    }

    public String getA_key() {
        return a_key;
    }

    public void setA_key(String a_key) {
        this.a_key = a_key;
    }

    public String getA_value() {
        return a_value;
    }

    public void setA_value(String a_value) {
        this.a_value = a_value;
    }

    public long getA_type() {
        return a_type;
    }

    public void setA_type(long a_type) {
        this.a_type = a_type;
    }

    public long getEndpoint_ipv4() {
        return endpoint_ipv4;
    }

    public void setEndpoint_ipv4(long endpoint_ipv4) {
        this.endpoint_ipv4 = endpoint_ipv4;
    }

    public long getEndpoint_ipv6() {
        return endpoint_ipv6;
    }

    public void setEndpoint_ipv6(long endpoint_ipv6) {
        this.endpoint_ipv6 = endpoint_ipv6;
    }

    public int getEndpoint_port() {
        return endpoint_port;
    }

    public void setEndpoint_port(int endpoint_port) {
        this.endpoint_port = endpoint_port;
    }

    public Long getA_timestamp() {
        return a_timestamp;
    }

    public void setA_timestamp(Long a_timestamp) {
        this.a_timestamp = a_timestamp;
    }

    public String getEndpoint_service_name() {
        return endpoint_service_name;
    }

    public void setEndpoint_service_name(String endpoint_service_name) {
        this.endpoint_service_name = endpoint_service_name;
    }
}
