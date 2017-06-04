package com.sebis.mobility;

import com.sebis.mobility.model.Annotations;
import com.sebis.mobility.model.Spans;
import org.influxdb.dto.Point;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.influxdb.InfluxDBTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by kleehausm on 04.06.2017.
 */

@Component
public class TransferSpansTask {
    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    private InfluxDBTemplate<Point> influxDBTemplate;

    @Scheduled(fixedDelay = 5000)
    public void runSpanTask() {
        influxDBTemplate.createDatabase();
        //Select trace_id,span_id,name,parent_id,start_ts,duration from zipkin_spans where scanned = false
        List<Spans> spans = jdbcTemplate.query("" +
                        "Select " +
                        "t1.trace_id," +
                        "t2.span_id," +
                        "t1.parent_id," +
                        "t1.name," +
                        "t2.endpoint_service_name," +
                        "t2.endpoint_port," +
                        "t2.a_key," +
                        "t2.a_value," +
                        "t2.a_type," +
                        "t2.endpoint_ipv4," +
                        "t2.endpoint_ipv6," +
                        "t1.start_ts," +
                        "t2.a_timestamp," +
                        "t1.duration " +
                        "from zipkin_spans t1 left join zipkin_annotations t2 on " +
                        "t1.trace_id = t2.trace_id and t1.id = t2.span_id " +
                        "where t1.scanned = false", new Object[]{},
                (rs, rowNum) -> new Spans(rs.getLong("trace_id"),
                        rs.getLong("span_id"),
                        rs.getLong("parent_id"),
                        rs.getString("name"),
                        rs.getString("endpoint_service_name"),
                        rs.getInt("endpoint_port"),
                        rs.getString("a_key"),
                        rs.getString("a_value"),
                        rs.getLong("a_type"),
                        rs.getLong("endpoint_ipv4"),
                        rs.getLong("endpoint_ipv6"),
                        rs.getLong("start_ts"),
                        rs.getLong("a_timestamp"),
                        rs.getLong("duration")
                        )
        );

        Point point1;
        for (Spans span : spans) {
            point1 = Point.measurement("zipkin_spans")
                    .time(span.getA_timestamp(), TimeUnit.MICROSECONDS)
                    .addField("start_ts", span.getStart_ts())
                    .addField("trace_id", span.getTrace_id())
                    .addField("span_id", span.getSpan_id())
                    .addField("name", span.getName() == null ? "" : span.getName())
                    .addField("parent_id", span.getParent_id())
                    .addField("duration", span.getDuration())
                    .addField("a_key", span.getA_key() == null ? "" : span.getA_key())
                    .addField("a_value", (span.getA_value() == null) ? "" : span.getA_value())
                    .addField("a_type", span.getA_type())
                    .addField("endpoint_ipv4", span.getEndpoint_ipv4())
                    .addField("endpoint_ipv6", span.getEndpoint_ipv6())
                    .addField("endpoint_port", span.getEndpoint_port())
                    .addField("endpoint_service_name", span.getEndpoint_service_name() == null ? "" : span.getEndpoint_service_name())
                    .build();
            influxDBTemplate.write(point1);
            jdbcTemplate.update(
                    "update zipkin_spans set scanned = true where trace_id = ? and id = ?", span.getTrace_id(), span.getSpan_id());
        }
    }
}
