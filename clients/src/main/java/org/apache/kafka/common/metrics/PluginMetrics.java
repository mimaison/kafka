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
package org.apache.kafka.common.metrics;

import org.apache.kafka.common.MetricName;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PluginMetrics {

    private final Metrics metrics;
    private final String className;
    private final Set<MetricName> metricNames = new HashSet<>();
    private final Set<String> sensors = new HashSet<>();

    /**
     * Create a PluginMetric instance for plugins to register metrics
     *
     * @param metrics The underlying Metrics repository to use for metrics
     * @param className The class name of the plugin
     */
    public PluginMetrics(Metrics metrics, String className) {
        this.metrics = metrics;
        this.className = className;
    }

    /**
     * Create a MetricName with the given name, description and tags. The plugin class name will be used as the metric group.
     *
     * @param name        The name of the metric
     * @param description A human-readable description to include in the metric
     * @param tags        additional key/value attributes of the metric
     */
    public MetricName metricName(String name, String description, Map<String, String> tags) {
        return metrics.metricName(name, className, description, new HashMap<>(tags));
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
     * @return the removed KafkaMetric or null if no such metric exists
     */
    public synchronized KafkaMetric removeMetric(MetricName metricName) {
        if (metricNames.remove(metricName)) {
            return metrics.removeMetric(metricName);
        }
        return null;
    }

    /**
     * Get or create a sensor with the given unique name.
     *
     * @param name The sensor name
     * @return The sensor
     */
    public synchronized Sensor sensor(String name) {
        Sensor sensor = metrics.sensor(name);
        sensors.add(name);
        return sensor;
    }

    /**
     * Remove a sensor (if it exists) and its associated metrics.
     *
     * @param name The name of the sensor to be removed
     */
    public synchronized void removeSensor(String name) {
        metrics.removeSensor(name);
        sensors.remove(name);
    }

    /**
     * Delete all metrics and sensors registered by this plugin
     */
    void close() {
        for (MetricName metricName : metricNames) {
            metrics.removeMetric(metricName);
        }
        for (String sensor : sensors) {
            metrics.removeSensor(sensor);
        }
    }
}
