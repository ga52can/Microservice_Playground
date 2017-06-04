package com.sebis.mobility;

import com.sebis.mobility.model.Annotations;
import com.sebis.mobility.model.Spans;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.data.influxdb.InfluxDBTemplate;
import org.springframework.data.influxdb.InfluxDBOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

/**
 * Created by kleehausm on 02.06.2017.
 */

/*
@Component
public class ApplicationStartup implements ApplicationListener<ApplicationReadyEvent> {

    @Override
    public void onApplicationEvent(final ApplicationReadyEvent event) {

        MysqlScanTask task = new MysqlScanTask();
        int SECONDS = 10; // The delay in minutes
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() { // Function runs every MINUTES minutes.
                // Run the code you want here
                task.run();
            }
        }, 0, 1000 * SECONDS);

        //ClassExecutionTask executingTask = new ClassExecutionTask();
        //executingTask.start();
    }
}
*/