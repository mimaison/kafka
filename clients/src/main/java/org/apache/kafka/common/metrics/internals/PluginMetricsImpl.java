/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.metrics.internals;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.MetricValueProvider;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.common.metrics.Sensor;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class PluginMetricsImpl implements PluginMetrics {

    private final Metrics metrics;
    private final Map<String, String> tags;
    private final Set<MetricName> metricNames = new HashSet<>();
    private final Set<String> sensors = new HashSet<>();

    /**
     * Create a PluginMetric instance for plugins to register metrics
     *
     * @param metrics The underlying Metrics repository to use for metrics
     * @param tags The tags for the plugin
     */
    public PluginMetricsImpl(Metrics metrics, Map<String, String> tags) {
        this.metrics = metrics;
        this.tags = tags;
    }

    /**
     * Create a MetricName with the given name, description and tags.
     *
     * @param name        The name of the metric
     * @param description A human-readable description to include in the metric
     * @param tags        additional key/value attributes of the metric
     */
    public MetricName metricName(String name, String description, Map<String, String> tags) {
        Map<String, String> newTags = new LinkedHashMap<>(tags);
        newTags.putAll(this.tags);
        return metrics.metricName(name, "plugins", description, newTags);
    }

    /**
     * Add a metric to monitor an object that implements MetricValueProvider. This metric won't be associated with any
     * sensor. This is a way to expose existing values as metrics.
     *
     * @param metricName The name of the metric
     * @param metricValueProvider The metric value provider associated with this metric
     * @throws IllegalArgumentException if a metric with same name already exists.
     */
    public synchronized void addMetric(MetricName metricName, MetricValueProvider<?> metricValueProvider) {
        metrics.addMetric(metricName, null, metricValueProvider);
        metricNames.add(metricName);
    }

    /**
     * Remove a metric if it exists and return it. Return null otherwise.
     *
     * @param metricName The name of the metric
     */
    public synchronized void removeMetric(MetricName metricName) {
        if (metricNames.remove(metricName)) {
            metrics.removeMetric(metricName);
        }
    }

    /**
     * Get or create a sensor with the given unique name.
     *
     * @param name The sensor name
     * @return The sensor
     * @throws IllegalArgumentException if a sensor with same name already exists.
     */
    public synchronized Sensor sensor(String name) {
        String sensorName = name + tags.toString();
        if (metrics.getSensor(sensorName) != null) {
            throw new IllegalArgumentException("A sensor with this name already exists, can't register another one.");
        }
        Sensor sensor = metrics.sensor(sensorName);
        sensors.add(sensorName);
        return sensor;
    }

    /**
     * Remove a sensor (if it exists) and its associated metrics.
     *
     * @param name The name of the sensor to be removed
     */
    public synchronized void removeSensor(String name) {
        if (sensors.contains(name)) {
            metrics.removeSensor(name);
            sensors.remove(name);
        }
    }

    /**
     * Delete all metrics and sensors registered by this plugin
     */
    @Override
    public void close() {
        for (MetricName metricName : metricNames) {
            metrics.removeMetric(metricName);
        }
        for (String sensor : sensors) {
            metrics.removeSensor(sensor);
        }
    }
}
