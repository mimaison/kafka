/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  *    http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.server

import kafka.admin.AdminUtils

import org.apache.kafka.server.assignor.ReplicaAssignor
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.security.auth.KafkaPrincipal

import java.util.List
import java.util.Map

import scala.jdk.CollectionConverters._
import java.util.Collections
import org.apache.kafka.server.assignor.ReplicaAssignor.ComputedAssignment
import org.apache.kafka.server.assignor.ReplicaAssignor.RequestedAssignment


class DefaultReplicaAssignor extends ReplicaAssignor {

  def assignReplicasToBrokers(requestedReplicaAssignment: Map[String, RequestedAssignment],
                              cluster: Cluster, principal: KafkaPrincipal): Map[String, ComputedAssignment] = {
    
    return Collections.emptyMap();
//    
//    val brokerMetadatas : Seq[kafka.admin.BrokerMetadata] = cluster.nodes().asScala.map { b => kafka.admin.BrokerMetadata(b.id, Option(b.rack)) }.toSeq;
//    AdminUtils.assignReplicasToBrokers(brokerMetadatas, partitions.size, replicationFactor).map { case(k,v) => (Integer.valueOf(k), v.map { i => Integer.valueOf(i) }.asJava) }.asJava
  }
}