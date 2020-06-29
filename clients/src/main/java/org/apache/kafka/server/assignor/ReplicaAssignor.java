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
package org.apache.kafka.server.assignor;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

public interface ReplicaAssignor {

    interface RequestedAssignment {
        String topicName();
        List<Integer> partitions();
        short replicationFactor();
        Map<String, String> topicconfigs();
    }
    
    public class RequestedAssignmentImpl implements RequestedAssignment {
        final String topicName;
        final List<Integer> partitions;
        final short replicationFactor;
        final Map<String, String> topicconfigs;
        public RequestedAssignmentImpl(
                String topicName,
                List<Integer> partitions,
                short replicationFactor,
                Map<String, String> topicconfigs) {
            this.topicName = topicName;
            this.partitions = partitions;
            this.replicationFactor = replicationFactor;
            this.topicconfigs = topicconfigs;
        }
        @Override
        public String topicName() {
            return topicName;
        }
        @Override
        public List<Integer> partitions() {
            return partitions;
        }
        @Override
        public short replicationFactor() {
            return replicationFactor;
        }
        @Override
        public Map<String, String> topicconfigs() {
            return topicconfigs;
        }
    }

    interface ComputedAssignment {
        String topicName();
        Map<Integer, List<Integer>> assignment();
    }
    
    class ComputedAssignmentImpl implements ComputedAssignment {
        String topicName;
        Map<Integer, List<Integer>> assignment;
        
        public ComputedAssignmentImpl(String topicName, Map<Integer, List<Integer>> assignment) {
            this.topicName = topicName;
            this.assignment = assignment;
        }

        @Override
        public String topicName() {
            return topicName;
        }

        @Override
        public Map<Integer, List<Integer>> assignment() {
            return assignment;
        }
        
    }

    /**
     * Assigns the specified partitions to brokers
     * @param topicName The name of the topic
     * @param partitions The list of partitionIds that need an assignment
     * @param replicationFactor The replication factor of the topic
     * @param cluster The cluster metadata
     * @param principal The principal of the user initiating the request
     * @return A map of partitionId to list of assigned brokers
     */
//    public Map<Integer, List<Integer>> assignReplicasToBrokers(
//            String topicName, List<Integer> partitions, int replicationFactor,
//            Cluster cluster, KafkaPrincipal principal);

    public Map<String, ComputedAssignment> assignReplicasToBrokers(
            Map<String, RequestedAssignment> requestedReplicaAssignment,
            Cluster cluster, KafkaPrincipal principal);
    
}
