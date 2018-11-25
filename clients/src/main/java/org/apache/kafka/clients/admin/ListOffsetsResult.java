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
package org.apache.kafka.clients.admin;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * The result of the {@link AdminClient#listOffsets(String)} call.
 *
 * The API of this class is evolving, see {@link AdminClient} for details.
 */
@InterfaceStability.Evolving
public class ListOffsetsResult {

    private final Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> futures;

    public ListOffsetsResult(Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> futures) {
        this.futures = futures;
    }

    /**
     * Return a map from TopicPartition to futures which can be used to retrieve the offsets
     */
    public Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> values() {
        return futures;
    }

    /**
     * Return a future which succeeds only if offsets for all specified partitions have been successfully
     * retrieved.
     */
    public KafkaFuture<Map<TopicPartition, ListOffsetsResultInfo>> all() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0])).thenApply(
            new KafkaFuture.BaseFunction<Void, Map<TopicPartition, ListOffsetsResultInfo>>() {
                @Override
                public Map<TopicPartition, ListOffsetsResultInfo> apply(Void v) {
                    try {
                        Map<TopicPartition, ListOffsetsResultInfo> offsets = new HashMap<>(futures.size());
                        for (Map.Entry<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> entry : futures.entrySet()) {
                            offsets.put(entry.getKey(), entry.getValue().get());
                        }
                        return offsets;
                    } catch (InterruptedException | ExecutionException e) {
                        // This should be unreachable, since the KafkaFuture#allOf already ensured
                        // that all of the futures completed successfully.
                        throw new RuntimeException(e);
                    }
                }
            });
    }

    static public class ListOffsetsResultInfo {
        private final long offset;
        private final long timestamp;

        ListOffsetsResultInfo(long offset, long timestamp) {
            this.offset = offset;
            this.timestamp = timestamp;
        }

        long getOffset() {
            return offset;
        }

        long getTimestamp() {
            return timestamp;
        }
    }
}

