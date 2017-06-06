package com.sebis.mobility.model;

import com.sebis.mobility.SpanKeys;
import org.joda.time.DateTime;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import java.io.Serializable;
import java.sql.Timestamp;

/**
 * Created by sohaib on 31/03/17.
 */

@Entity
@IdClass(SpanKeys.class)
public class Spans {
    @Id private long trace_id;
    @Id private long span_id;
    private String name;
    private long parent_id;
    private long start_ts;
    private long duration;
    @Id private String a_key;
    @Id private String a_value;
    private long a_type;
    private long endpoint_ipv4;
    private long endpoint_ipv6;
    private int endpoint_port;
    @Id private long a_timestamp;
    private String endpoint_service_name;

    public Spans() {}

    /*
    public Spans(long trace_id_high,
                 long trace_id,
                 long id,
                 String name,
                 long parent_id,
                 int debug,
                 long start_ts,
                 long duration,
                 Timestamp created_at) {
        this.trace_id_high = trace_id_high;
        this.trace_id = trace_id;
        this.id = id;
        this.name = name;
        this.parent_id = parent_id;
        this.debug = debug;
        this.start_ts = start_ts;
        this.duration = duration;
        this.created_at = created_at;
    }
    */
    public Spans(long trace_id,
                 long span_id,
                 long parent_id,
                 String name,
                 String endpoint_service_name,
                 int endpoint_port,
                 String a_key,
                 String a_value,
                 long a_type,
                 long endpoint_ipv4,
                 long endpoint_ipv6,
                 long start_ts,
                 long a_timestamp,
                 long duration) {
        this.trace_id = trace_id;
        this.span_id = span_id;
        this.parent_id = parent_id;
        this.name = name;
        this.a_key = a_key;
        this.a_value = a_value;
        this.a_type = a_type;
        this.endpoint_ipv4 = endpoint_ipv4;
        this.endpoint_ipv6 = endpoint_ipv6;
        this.endpoint_port = endpoint_port;
        this.a_timestamp = a_timestamp;
        this.endpoint_service_name = endpoint_service_name;
        this.start_ts = start_ts;
        this.duration = duration;
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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getParent_id() {
        return parent_id;
    }

    public void setParent_id(long parent_id) {
        this.parent_id = parent_id;
    }

    public long getStart_ts() {
        return start_ts;
    }

    public void setStart_ts(long start_ts) {
        this.start_ts = start_ts;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
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

    public long getA_timestamp() {
        return a_timestamp;
    }

    public void setA_timestamp(long a_timestamp) {
        this.a_timestamp = a_timestamp;
    }

    public String getEndpoint_service_name() {
        return endpoint_service_name;
    }

    public void setEndpoint_service_name(String endpoint_service_name) {
        this.endpoint_service_name = endpoint_service_name;
    }
}
