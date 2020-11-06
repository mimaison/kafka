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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.ReplicaAssignor.ReplicaAssignment;
import org.junit.Before;
import org.junit.Test;

public class MMAssignorTest {

    private final KafkaPrincipal principal = new KafkaPrincipal("type", "name");
    private ReplicaAssignor assignor;

    @Before
    public void setUp() {
        assignor = new MMAssignor();
    }

    @Test
    public void testAssignReplicasToBrokersEmptyCluster() {
        List<Node> nodes = buildNodes(1);
        Cluster cluster = buildCluster(nodes, Collections.emptyList());
        ReplicaAssignor.NewTopic topic = new ReplicaAssignor.NewTopicImpl("topic", Arrays.asList(0), (short) 3, Collections.emptyMap());
        ReplicaAssignment assignment = assignor.computeAssignment(topic, cluster, principal);
        System.out.println(assignment);
        List<Integer> replicas = assignment.assignment().get(0);
        assertNotNull(replicas);
        Set<Integer> replicaSet = new HashSet<>(replicas);
        assertEquals(new HashSet<Integer>(Arrays.asList(0, 1, 2)), replicaSet);
    }

    @Test
    public void testAssignReplicasToBrokers() {
        List<Node> nodes = buildNodes(2);
        List<PartitionInfo> partitions = Arrays.asList(
                buildPartitionInfo("topic1", 0, nodes.get(0), nodes.get(0), nodes.get(1), nodes.get(2)),
                buildPartitionInfo("topic1", 1, nodes.get(1), nodes.get(0), nodes.get(1), nodes.get(2)),
                buildPartitionInfo("topic1", 2, nodes.get(2), nodes.get(0), nodes.get(1), nodes.get(2))
        );
        Cluster cluster = buildCluster(nodes, partitions);
        Map<Integer, List<Integer>> assignment = assignor.computeAssignment("topic", Arrays.asList(0, 1, 2), 3, cluster, principal);
        System.out.println(assignment);
        List<Integer> replicas = assignment.get(0);
        assertNotNull(replicas);
        Set<Integer> replicaSet = new HashSet<>(replicas);
        assertEquals(3, replicaSet.size());
    }

    @Test
    public void testAddPartition() {
        List<Node> nodes = buildNodes(2);
        List<PartitionInfo> partitions = Arrays.asList(
                buildPartitionInfo("topic1", 0, nodes.get(0), nodes.get(0), nodes.get(1), nodes.get(2)),
                buildPartitionInfo("topic1", 1, nodes.get(1), nodes.get(0), nodes.get(1), nodes.get(2)),
                buildPartitionInfo("topic1", 2, nodes.get(2), nodes.get(0), nodes.get(1), nodes.get(2))
        );
        Cluster cluster = buildCluster(nodes, partitions);
        Map<Integer, List<Integer>> assignment = assignor.computeAssignment("topic1", Arrays.asList(0), 3, cluster, principal);
        System.out.println(assignment);
        List<Integer> replicas = assignment.get(0);
        assertNotNull(replicas);
        Set<Integer> replicaSet = new HashSet<>(replicas);
        assertEquals(3, replicaSet.size());
    }

    private static PartitionInfo buildPartitionInfo(String topic, int partition, Node leader, Node... nodes) {
        return new PartitionInfo("topic1", 0, leader, nodes, nodes);
    }

    private static Cluster buildCluster(List<Node> nodes, List<PartitionInfo> partitions) {
        return new Cluster("cId", nodes, partitions, Collections.emptySet(), Collections.emptySet());
    }

    private static List<Node> buildNodes(int nodesByRack) {
        List<Node> nodes = new ArrayList<>();
        for (int i = 0; i < nodesByRack * 3; i++) {
            nodes.add(new Node(i, "h" + i, i, "rack" + i % 3));
        }
        return nodes;
    }
}
