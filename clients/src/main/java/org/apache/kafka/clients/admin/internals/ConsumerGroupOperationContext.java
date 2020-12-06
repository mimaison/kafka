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
package org.apache.kafka.clients.admin.internals;

import java.util.Arrays;

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


import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.apache.kafka.clients.admin.AbstractOptions;
import org.apache.kafka.clients.admin.KafkaAdminClient.Call;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.protocol.Errors;

/**
 * Context class to encapsulate parameters of a call to find and use a consumer group coordinator.
 * Some of the parameters are provided at construction and are immutable whereas others are provided
 * as "Call" are completed and values are available, like node id of the coordinator.
 *
 * @param <T> The type of return value of the KafkaFuture
 * @param <O> The type of configuration option. Different for different consumer group commands.
 */
public final class ConsumerGroupOperationContext<T, O extends AbstractOptions<O>> {
    final private Collection<String> groupIds;
    final private O options;
    final private long deadline;
    final private Map<String, KafkaFutureImpl<T>> futures;
    final private Map<String, Optional<Node>> nodes;
    final private boolean batch;
    private final Function<ConsumerGroupOperationContext<T, O>, List<Call>> function;

    public ConsumerGroupOperationContext(Collection<String> groupIds,
            O options,
            long deadline,
            Map<String, KafkaFutureImpl<T>> futures,
            Function<ConsumerGroupOperationContext<T, O>, List<Call>> supplier) {
        this(groupIds, options, deadline, futures, supplier, true);
    }

    public ConsumerGroupOperationContext(String groupId,
            O options,
            long deadline,
            KafkaFutureImpl<T> future,
            Function<ConsumerGroupOperationContext<T, O>, List<Call>> supplier) {
        this(Arrays.asList(groupId), options, deadline, Collections.singletonMap(groupId, future), supplier, false);
    }

    private ConsumerGroupOperationContext(Collection<String> groupIds,
                                         O options,
                                         long deadline,
                                         Map<String, KafkaFutureImpl<T>> futures,
                                         Function<ConsumerGroupOperationContext<T, O>, List<Call>> supplier,
                                         boolean batch) {
        this.groupIds = groupIds;
        this.options = options;
        this.deadline = deadline;
        this.futures = futures;
        this.nodes = new HashMap<>();
        for (String groupId : groupIds) {
            nodes.put(groupId, Optional.empty());
        }
        this.function = supplier;
        this.batch = batch;
    }

    public Collection<String> groupIds() {
        return groupIds;
    }

    public O options() {
        return options;
    }

    public long deadline() {
        return deadline;
    }

    public Collection<KafkaFutureImpl<T>> futures() {
        return futures.values();
    }

    public KafkaFutureImpl<T> future(String groupId) {
        return futures.get(groupId);
    }

    public Map<String, Optional<Node>> nodes() {
        return nodes;
    }

    public boolean batch() {
        return batch;
    }

    public Optional<Node> node(String groupId) {
        return nodes.get(groupId);
    }

    public void setNode(String groupId, Node node) {
        this.nodes.replace(groupId, Optional.ofNullable(node));
    }

    public static boolean hasCoordinatorMoved(Map<Errors, Integer> errorCounts) {
        return errorCounts.containsKey(Errors.NOT_COORDINATOR);
    }

    public static boolean shouldRefreshCoordinator(Map<Errors, Integer> errorCounts) {
        return errorCounts.containsKey(Errors.COORDINATOR_LOAD_IN_PROGRESS) ||
                errorCounts.containsKey(Errors.COORDINATOR_NOT_AVAILABLE);
    }

    public ConsumerGroupOperationContext<T, O> getContextFor(String groupId) {
        return new ConsumerGroupOperationContext<>(groupId, options(), deadline() + 10000000, future(groupId), function);
    }

    public Function<ConsumerGroupOperationContext<T, O>, List<Call>> getFunction() {
        return function;
    }

}
