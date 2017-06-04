package com.sebis.mobility;

/**
 * Created by kleehausm on 04.06.2017.
 */
import com.sebis.mobility.model.Annotations;
import com.sebis.mobility.model.Spans;
import org.influxdb.dto.Point;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.influxdb.InfluxDBTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.Date;

import java.util.List;
import java.util.Timer;

import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

/*
@Component
public class ClassExecutionTask {

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    private InfluxDBTemplate<Point> influxDBTemplate;

    long delay = 10 * 1000; // delay in milliseconds
    LoopTask task = new LoopTask();
    Timer timer = new Timer("TaskName");

    public void start() {
        timer.cancel();
        timer = new Timer("TaskName");
        Date executionDate = new Date(); // no params = now
        timer.scheduleAtFixedRate(task, executionDate, delay);
    }

    private class LoopTask extends TimerTask {
        @Override
        public void run() {
            influxDBTemplate.createDatabase();
            List<Spans> spans = jdbcTemplate.query("Select trace_id,id,name,parent_id,start_ts,duration from zipkin_spans where scanned = false",new Object[]{},
                    (rs, rowNum) -> new Spans(rs.getLong("trace_id"),
                            rs.getLong("id"),
                            rs.getString("name"),
                            rs.getLong("parent_id"),
                            rs.getLong("start_ts"),
                            rs.getLong("duration"))
            );

            Point point1;
            for(Spans span : spans) {
                point1 = Point.measurement("zipkin_spans")
                        .time(span.getStart_ts(), TimeUnit.NANOSECONDS)
                        .addField("trace_id", span.getTrace_id())
                        .addField("id", span.getId())
                        .addField("name", span.getName())
                        .addField("parent_id", span.getParent_id())
                        .addField("duration", span.getDuration())
                        .build();
                influxDBTemplate.write(point1);
                jdbcTemplate.update(
                        "update zipkin_spans set scanned = true where trace_id = ? and id = ?", span.getTrace_id(), span.getId());
            }

            List<Annotations> annotations = jdbcTemplate.query("Select trace_id, span_id, a_key, a_value, a_type, a_timestamp, endpoint_ipv4, endpoint_ipv6, endpoint_port, endpoint_service_name from zipkin_annotations where scanned = false",new Object[]{},
                    (rs, rowNum) -> new Annotations(rs.getLong("trace_id"),
                            rs.getLong("span_id"),
                            rs.getString("a_key"),
                            rs.getString("a_value"),
                            rs.getLong("a_type"),
                            rs.getLong("a_timestamp"),
                            rs.getLong("endpoint_ipv4"),
                            rs.getLong("endpoint_ipv6"),
                            rs.getInt("endpoint_port"),
                            rs.getString("endpoint_service_name"))
            );
            //InfluxDB influxDB = InfluxDBFactory.connect("http://172.17.0.2:8086", "root", "root");
            //String dbName = "aTimeSeries";
            //influxDB.createDatabase(dbName);

            Point point2;
            for(Annotations annotation : annotations) {
                point2 = Point.measurement("zipkin_annotations")
                        .time(annotation.getA_timestamp(), TimeUnit.NANOSECONDS)
                        .addField("trace_id", annotation.getTrace_id())
                        .addField("span_id", annotation.getSpan_id())
                        .addField("a_key", annotation.getA_key())
                        .addField("a_value", (annotation.getA_value()==null) ? "" : annotation.getA_value())
                        .addField("a_type", annotation.getA_type())
                        .addField("endpoint_ipv4", annotation.getEndpoint_ipv4())
                        .addField("endpoint_ipv6", annotation.getEndpoint_ipv6())
                        .addField("endpoint_port", annotation.getEndpoint_port())
                        .addField("endpoint_service_name", annotation.getEndpoint_service_name())
                        .build();
                influxDBTemplate.write(point2);
                jdbcTemplate.update(
                        "update zipkin_annotations set scanned = true where trace_id = ? and span_id = ? and a_timestamp = ? and a_key = ? and a_value = ?", annotation.getTrace_id(), annotation.getSpan_id(), annotation.getA_timestamp(), annotation.getA_key(), annotation.getA_value());
            }
        }
    }


}
*/


