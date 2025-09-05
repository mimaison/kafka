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

import org.apache.kafka.common.message.PushTelemetryRequestData;
import org.apache.kafka.common.requests.PushTelemetryRequest;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.metrics.ClientMetricsTestUtils.TestClientMetricsExporter;
import org.apache.kafka.server.metrics.ClientMetricsTestUtils.TestClientMetricsExporterReceiver;
import org.apache.kafka.server.metrics.ClientMetricsTestUtils.TestClientMetricsReceiver;

import org.apache.kafka.server.telemetry.ClientTelemetryContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientMetricsReceiverPluginTest {

    private ClientMetricsReceiverPlugin clientMetricsReceiverPlugin;

    @BeforeEach
    public void setUp() {
        clientMetricsReceiverPlugin = new ClientMetricsReceiverPlugin();
    }

    @Test
    public void testExportMetrics() throws UnknownHostException {
        TestClientMetricsExporter telemetryReceiver = new TestClientMetricsExporter();

        assertTrue(clientMetricsReceiverPlugin.isEmpty());

        clientMetricsReceiverPlugin.add(telemetryReceiver);
        assertFalse(clientMetricsReceiverPlugin.isEmpty());

        assertEquals(0, telemetryReceiver.exportMetricsInvokedCount);
        assertTrue(telemetryReceiver.metricsData.isEmpty());

        byte[] metrics = "test-metrics".getBytes(StandardCharsets.UTF_8);
        clientMetricsReceiverPlugin.exportMetrics(ClientMetricsTestUtils.clientTelemetryContext(123),
            new PushTelemetryRequest.Builder(new PushTelemetryRequestData().setMetrics(ByteBuffer.wrap(metrics)), true).build());

        assertEquals(1, telemetryReceiver.exportMetricsInvokedCount);
        assertEquals(1, telemetryReceiver.metricsData.size());
        assertEquals(metrics, telemetryReceiver.metricsData.get(0).array());
        assertInstanceOf(ClientTelemetryContext.class, telemetryReceiver.context);
        assertEquals(123, telemetryReceiver.context.pushIntervalMs());
    }

    @Test
    public void testDeprecatedExportMetrics() throws UnknownHostException {
        TestClientMetricsReceiver telemetryReceiver = new TestClientMetricsReceiver();

        assertTrue(clientMetricsReceiverPlugin.isEmpty());

        clientMetricsReceiverPlugin.add(telemetryReceiver);
        assertFalse(clientMetricsReceiverPlugin.isEmpty());

        assertEquals(0, telemetryReceiver.exportMetricsInvokedCount);
        assertTrue(telemetryReceiver.metricsData.isEmpty());

        byte[] metrics = "test-metrics".getBytes(StandardCharsets.UTF_8);
        clientMetricsReceiverPlugin.exportMetrics(ClientMetricsTestUtils.clientTelemetryContext(123),
                new PushTelemetryRequest.Builder(new PushTelemetryRequestData().setMetrics(ByteBuffer.wrap(metrics)), true).build());

        assertEquals(1, telemetryReceiver.exportMetricsInvokedCount);
        assertEquals(1, telemetryReceiver.metricsData.size());
        assertEquals(metrics, telemetryReceiver.metricsData.get(0).array());
        assertInstanceOf(AuthorizableRequestContext.class, telemetryReceiver.context);
        assertFalse(telemetryReceiver.context instanceof ClientTelemetryContext);
    }

    @Test
    public void testExportMetricsReceiverAndExporter() throws UnknownHostException {
        TestClientMetricsExporterReceiver telemetryReceiver = new TestClientMetricsExporterReceiver();

        assertTrue(clientMetricsReceiverPlugin.isEmpty());

        clientMetricsReceiverPlugin.add(telemetryReceiver);
        assertFalse(clientMetricsReceiverPlugin.isEmpty());

        assertEquals(0, telemetryReceiver.exportMetricsInvokedCount);
        assertTrue(telemetryReceiver.metricsData.isEmpty());

        byte[] metrics = "test-metrics".getBytes(StandardCharsets.UTF_8);
        clientMetricsReceiverPlugin.exportMetrics(ClientMetricsTestUtils.clientTelemetryContext(123),
                new PushTelemetryRequest.Builder(new PushTelemetryRequestData().setMetrics(ByteBuffer.wrap(metrics)), true).build());

        assertEquals(1, telemetryReceiver.exportMetricsInvokedCount);
        assertEquals(1, telemetryReceiver.metricsData.size());
        assertEquals(metrics, telemetryReceiver.metricsData.get(0).array());
        assertInstanceOf(ClientTelemetryContext.class, telemetryReceiver.context);
    }
}
