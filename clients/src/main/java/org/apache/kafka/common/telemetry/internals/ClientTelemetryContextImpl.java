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
package org.apache.kafka.common.telemetry.internals;

import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.telemetry.ClientTelemetryContext;

import java.net.InetAddress;

/**
 * Client telemetry context that provides context from the request as well as the push interval for the client telemetry.
 */
public class ClientTelemetryContextImpl implements ClientTelemetryContext {

    private final AuthorizableRequestContext context;
    private final int pushInterval;

    public ClientTelemetryContextImpl(AuthorizableRequestContext context, int pushInterval) {
        this.context = context;
        this.pushInterval = pushInterval;
    }

    @Override
    public String listenerName() {
        return context.listenerName();
    }

    @Override
    public SecurityProtocol securityProtocol() {
        return context.securityProtocol();
    }

    @Override
    public KafkaPrincipal principal() {
        return context.principal();
    }

    @Override
    public InetAddress clientAddress() {
        return context.clientAddress();
    }

    @Override
    public int requestType() {
        return context.requestType();
    }

    @Override
    public int requestVersion() {
        return context.requestVersion();
    }

    @Override
    public String clientId() {
        return context.clientId();
    }

    @Override
    public int correlationId() {
        return context.correlationId();
    }

    public int pushIntervalMs() {
        return pushInterval;
    }

    public AuthorizableRequestContext authorizableRequestContext() {
        return context;
    }
}
