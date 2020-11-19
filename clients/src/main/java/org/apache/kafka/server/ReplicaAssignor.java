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

import java.io.Closeable;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

@InterfaceStability.Evolving
public interface ReplicaAssignor extends Configurable, Closeable {

    /**
     * Computes replica assignments for the specified partitions
     * 
     * If an assignment can't be computed, for example if the state of the cluster does not satisfy a requirement,
     * implementations can throw ReplicaAssignorException to prevent the topic/partition creation.
     * @param partitions The partitions being created
     * @param principal The principal of the user initiating the request
     * @return The computed replica assignments
     * @throw ReplicaAssignorException
     */
    public ReplicaAssignment computeAssignment(
            NewPartitions partitions,
            KafkaPrincipal principal) throws ReplicaAssignorException;

    /**
     * Metadata update callback that is invoked whenever UpdateMetadata request is received from
     * the controller.
     *
     * @param cluster Cluster metadata including brokers, partitions and their leaders if known
     */
    public void updateClusterMetadata(Cluster cluster);

    /**
     * Computed replica assignments for the specified partitions
     */
    public class ReplicaAssignment {

        private final Map<Integer, List<Integer>> assignment;

        public ReplicaAssignment(Map<Integer, List<Integer>> assignment) {
            this.assignment = assignment;
        }

        /**
         * @return a Map with the list of replicas for each partition
         */
        public Map<Integer, List<Integer>> assignment() {
            return assignment;
        }

        @Override
        public String toString() {
            return "ReplicaAssignment [assignment=" + assignment + "]";
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((assignment == null) ? 0 : assignment.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            ReplicaAssignment other = (ReplicaAssignment) obj;
            if (assignment == null) {
                if (other.assignment != null)
                    return false;
            } else if (!assignment.equals(other.assignment))
                return false;
            return true;
        }
    }

    /**
     * Partitions which require an assignment to be computed
     */
    public interface NewPartitions {

        /**
         * The name of the topic for these partitions
         */
        String topicName();

        /**
         * The list of partition ids
         */
        List<Integer> partitionIds();

        /**
         * The replication factor of the topic
         */
        short replicationFactor();

        /**
         * The configuration of the topic
         */
        Map<String, String> configs();
    }

    public class NewPartitionsImpl implements NewPartitions {

        private final String topicName;
        private final List<Integer> partitionIds;
        private final short replicationFactor;
        private final Map<String, String> configs;

        public NewPartitionsImpl(String topicName, List<Integer> partitionIds, short replicationFactor, Map<String, String> configs) {
            this.topicName = topicName;
            this.partitionIds = partitionIds;
            this.replicationFactor = replicationFactor;
            this.configs = configs;
        }

        @Override
        public String topicName() {
            return topicName;
        }

        @Override
        public List<Integer> partitionIds() {
            return partitionIds;
        }

        @Override
        public short replicationFactor() {
            return replicationFactor;
        }

        @Override
        public Map<String, String> configs() {
            return configs;
        }

        @Override
        public String toString() {
            return "NewPartitionsImpl [topicName=" + topicName + ", partitionIds=" + partitionIds
                    + ", replicationFactor=" + replicationFactor + ", configs=" + configs + "]";
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((configs == null) ? 0 : configs.hashCode());
            result = prime * result + ((partitionIds == null) ? 0 : partitionIds.hashCode());
            result = prime * result + replicationFactor;
            result = prime * result + ((topicName == null) ? 0 : topicName.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            NewPartitionsImpl other = (NewPartitionsImpl) obj;
            if (configs == null) {
                if (other.configs != null)
                    return false;
            } else if (!configs.equals(other.configs))
                return false;
            if (partitionIds == null) {
                if (other.partitionIds != null)
                    return false;
            } else if (!partitionIds.equals(other.partitionIds))
                return false;
            if (replicationFactor != other.replicationFactor)
                return false;
            if (topicName == null) {
                if (other.topicName != null)
                    return false;
            } else if (!topicName.equals(other.topicName))
                return false;
            return true;
        }
    }
}
