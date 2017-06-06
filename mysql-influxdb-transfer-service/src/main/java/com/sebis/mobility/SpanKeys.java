package com.sebis.mobility;

import javax.persistence.Id;
import java.io.Serializable;

/**
 * Created by kleehausm on 06.06.2017.
 */
public class SpanKeys implements Serializable {
    private long trace_id;
    private long span_id;
    private String a_key;
    private String a_value;
    private long a_timestamp;
}
