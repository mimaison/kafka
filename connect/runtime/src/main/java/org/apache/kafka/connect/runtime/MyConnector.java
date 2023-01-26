package org.apache.kafka.connect.runtime;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.metrics.Monitorable;
import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MyConnector extends SinkConnector implements Monitorable {

    private Sensor sensor;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        sensor.record();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MyTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return Collections.singletonList(Collections.emptyMap());
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void setPluginMetrics(PluginMetrics metrics) {
        sensor = metrics.sensor("start");
        MetricName rate = metrics.metricName("rate", "Average number of calls per second.", Collections.emptyMap());
        MetricName total = metrics.metricName("total", "Total number of calls.", Collections.emptyMap());
        sensor.add(rate, new Rate());
        sensor.add(total, new CumulativeCount());
    }
}
