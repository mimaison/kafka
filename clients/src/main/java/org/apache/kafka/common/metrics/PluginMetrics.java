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
import org.apache.kafka.common.MetricNameTemplate;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class PluginMetrics {

    private final Metrics metrics;
    private final String name;
    private final Set<String> templateTags;

    public PluginMetrics(Metrics metrics, String name) {
        this.metrics = metrics;
        this.name = name;
        this.templateTags = new LinkedHashSet<>(this.metrics.config().tags().keySet());
    }

    public MetricNameTemplate metricNameTemplate(String name, String description, Set<String> tags) {
        Set<String> newTemplateTags = new LinkedHashSet<>(templateTags);
        newTemplateTags.addAll(tags);
        return new MetricNameTemplate(name, this.name, description, newTemplateTags);
    }

    public MetricName metricInstance(MetricNameTemplate template, Map<String, String> tags) {
        Map<String, String> newTags = new HashMap<>(tags);
        tags.putAll(tags);
        return metrics.metricInstance(template, newTags);
    }

    public Sensor sensor(String name) {
        return metrics.sensor(name);
    }

    // need to forward many other calls from Metrics
}
