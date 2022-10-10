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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.util.ConnectorTaskId;

import java.util.Map;

public class WorkerSourceTaskContext implements SourceTaskContext {

    private final OffsetStorageReader reader;
    private final ConnectorTaskId id;
    private final ClusterConfigState configState;
    private final WorkerTransactionContext transactionContext;
    private final Metrics metrics;

    public WorkerSourceTaskContext(OffsetStorageReader reader,
                                   ConnectorTaskId id,
                                   ClusterConfigState configState,
                                   WorkerTransactionContext transactionContext,
                                   Metrics metrics) {
        this.reader = reader;
        this.id = id;
        this.configState = configState;
        this.transactionContext = transactionContext;
        this.metrics = metrics;
    }

    @Override
    public Map<String, String> configs() {
        return configState.taskConfig(id);
    }

    @Override
    public OffsetStorageReader offsetStorageReader() {
        return reader;
    }

    @Override
    public WorkerTransactionContext transactionContext() {
        return transactionContext;
    }

    @Override
    public Metrics metrics() {
        return metrics;
    }
}
