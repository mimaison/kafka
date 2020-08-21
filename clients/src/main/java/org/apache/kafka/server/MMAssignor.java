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
package org.apache.kafka.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

public class MMAssignor implements ReplicaAssignor {

    @Override
    public Map<Integer, List<Integer>> assignReplicasToBrokers(String topicName, List<Integer> partitions,
            int replicationFactor, Cluster cluster, KafkaPrincipal principal) {

        Map<Integer, List<Integer>> assignment = new HashMap<>();
        Map<Integer, Integer> partitionsByBroker = new HashMap<>();
        Map<String, List<Integer>> brokersByRack = new HashMap<>();
        Map<Integer, Integer> leadersByBroker = new HashMap<>();
        for (Node node : cluster.nodes()) {
            partitionsByBroker.putIfAbsent(node.id(), 0);
            List<PartitionInfo> partitionsForNode = cluster.partitionsForNode(node.id());
            leadersByBroker.put(node.id(), partitionsForNode.size());
            brokersByRack.putIfAbsent(node.rack(), new ArrayList<>());
            brokersByRack.get(node.rack()).add(node.id());
            for (PartitionInfo pInfo : partitionsForNode) {
                for (Node replica : pInfo.replicas()) {
                    if (replica.id() == node.id()) continue;
                    partitionsByBroker.putIfAbsent(replica.id(), 0);
                    int partition = partitionsByBroker.get(replica.id());
                    partitionsByBroker.put(replica.id(), partition + 1);
                }
            }
        }

        List<String> racks = new ArrayList<>(brokersByRack.keySet());
        if (racks.size() != replicationFactor) {
            throw new RuntimeException("Number of racks is different than requested replication factor");
        }

        for (Integer pId : partitions) {
            List<Integer> replicas = new ArrayList<>();
            for (int j = 0; j < replicationFactor; j++) {
                String rack = racks.get(j);
                List<Integer> brokersInRack = brokersByRack.get(rack);
                int leastLoadedBroker = findLeastLoadedBroker(brokersInRack, partitionsByBroker);
                replicas.add(leastLoadedBroker);
                partitionsByBroker.put(leastLoadedBroker, partitionsByBroker.get(leastLoadedBroker) + 1);
            }
            assignment.put(pId, ensureBrokerWithLeastLeadersIsLeader(replicas, leadersByBroker));
        }

        return assignment;
    }

    private List<Integer> ensureBrokerWithLeastLeadersIsLeader(List<Integer> brokers, Map<Integer, Integer> leadersByBroker) {
        int brokerId = findLeastLoadedBroker(brokers, leadersByBroker);
        int index = brokers.indexOf(brokerId);
        Collections.swap(brokers, 0, index);
        leadersByBroker.put(brokerId, leadersByBroker.get(brokerId) + 1);
        return brokers;
    }

    private int findLeastLoadedBroker(List<Integer> brokersInRack, Map<Integer, Integer> partitionsByBroker) {
        int leastLoadedBroker = -1;
        int currentLeastCount = Integer.MAX_VALUE;
        for (int broker : brokersInRack) {
            int partitions = partitionsByBroker.get(broker);
            if (partitions < currentLeastCount) {
                currentLeastCount = partitions;
                leastLoadedBroker = broker;
            }
        }
        return leastLoadedBroker;
    }

}
