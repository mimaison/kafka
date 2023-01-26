package org.apache.kafka.connect.runtime;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Monitorable;
import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class MyTask extends SinkTask implements Monitorable {

    private Sensor sensor;

    @Override
    public String version() {
        return null;
    }

    @Override
    public void start(Map<String, String> props) {

    }

    @Override
    public void put(Collection<SinkRecord> records) {
        sensor.record();
    }

    @Override
    public void stop() {

    }

    @Override
    public void setPluginMetrics(PluginMetrics metrics) {
        sensor = metrics.sensor("put");
        MetricName rate = metrics.metricName("rate", "Average number of calls per second.", Collections.emptyMap());
        MetricName total = metrics.metricName("total", "Total number of calls.", Collections.emptyMap());
        sensor.add(rate, new Rate());
        sensor.add(total, new CumulativeCount());
    }
}
