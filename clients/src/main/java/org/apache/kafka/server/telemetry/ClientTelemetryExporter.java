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

package org.apache.kafka.server.telemetry;

/**
 * {@code ClientTelemetryExporter} defines the behaviour for telemetry exporter on the broker side
 * which receives client telemetry metrics.
 */
public interface ClientTelemetryExporter {

    /**
     * Called by the broker when a client reports telemetry metrics. The associated telemetry context
     * can be used by the metrics plugin to retrieve additional client information such as client ids,
     * endpoints or the push interval.
     * <p>
     * This method may be called from the request handling thread, and as such should avoid blocking.
     *
     * @param context the client telemetry context for the corresponding {@code PushTelemetryRequest}
     *                api call.
     * @param payload the encoded telemetry payload as sent by the client.
     */
     void exportMetrics(ClientTelemetryContext context, ClientTelemetryPayload payload);
}
