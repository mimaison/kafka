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
package org.apache.kafka.server.metrics;

import org.apache.kafka.common.requests.PushTelemetryRequest;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryContextImpl;
import org.apache.kafka.server.telemetry.ClientTelemetryExporter;
import org.apache.kafka.server.telemetry.ClientTelemetryReceiver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Plugin to register client telemetry receivers and export metrics. This class is used by the Kafka
 * server to export client metrics to the registered receivers.
 */
public class ClientMetricsReceiverPlugin {

    private final List<Object> receiversAndExporters;

    public ClientMetricsReceiverPlugin() {
        this.receiversAndExporters = Collections.synchronizedList(new ArrayList<>());
    }

    public boolean isEmpty() {
        return receiversAndExporters.isEmpty();
    }

    public void add(Object receiver) {
        receiversAndExporters.add(receiver);
    }

    public DefaultClientTelemetryPayload getPayLoad(PushTelemetryRequest request) {
        return new DefaultClientTelemetryPayload(request);
    }

    @SuppressWarnings("deprecation")
    public void exportMetrics(ClientTelemetryContextImpl context, PushTelemetryRequest request) {
        DefaultClientTelemetryPayload payload = getPayLoad(request);
        for (Object receiverOrExporter : receiversAndExporters) {
            if (receiverOrExporter instanceof ClientTelemetryExporter exporter) {
                exporter.exportMetrics(context, payload);
            } else if (receiverOrExporter instanceof ClientTelemetryReceiver receiver) {
                receiver.exportMetrics(context.authorizableRequestContext(), payload);
            }
        }
    }
}
