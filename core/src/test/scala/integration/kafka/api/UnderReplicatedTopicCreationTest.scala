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

package kafka.api

import java.lang.{Boolean => JBoolean}
import java.time.Duration
import java.util
import java.util.Collections

import kafka.server.KafkaConfig
import kafka.utils.Logging
import kafka.utils.TestUtils._
import org.apache.kafka.clients.admin.{Admin, CreateTopicsResult, NewTopic}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.errors.InvalidReplicationFactorException
import org.junit.Assert._
import org.junit.{Before, Test}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters

import scala.collection.JavaConverters._


@RunWith(value = classOf[Parameterized])
class UnderReplicatedTopicCreationTest(enableUnderReplicatedTopicCreation: JBoolean) extends IntegrationTestHarness with Logging {
  override protected def brokerCount: Int = 2

  val topic = "topic_1"
  var adminClient: Admin = _

  // configure server properties
  this.serverConfig.setProperty(KafkaConfig.EnableUnderReplicatedTopicCreation, enableUnderReplicatedTopicCreation.toString)
  this.serverConfig.setProperty(KafkaConfig.MinInSyncReplicasProp, "1")

  @Before
  override def setUp(): Unit = {
    super.setUp()
    waitUntilBrokerMetadataIsPropagated(servers)
    killBroker(1)
    adminClient = createAdminClient()
  }

  @Test
  def testTopicCreationMissingReplicas(): Unit = {
    try {
      createTopic(topic).all.get
      assertTrue(enableUnderReplicatedTopicCreation)
      val replicas = topicReplicas(topic)
      assertTrue(replicas.contains(0))
      assertTrue(replicas.contains(-1))
    } catch {
      case e: Throwable => {
        assertTrue(e.getCause.isInstanceOf[InvalidReplicationFactorException])
        assertFalse(enableUnderReplicatedTopicCreation)
      }
    }

    restartDeadBrokers()
    waitUntilBrokerMetadataIsPropagated(servers)
    if (!enableUnderReplicatedTopicCreation) {
      createTopic(topic).all.get
    }
    val replicas = topicReplicas(topic)
    assertTrue(replicas.contains(0))
    assertTrue(replicas.contains(1))
  }

  def topicReplicas(topic: String): Set[Int] = {
    val descriptions = adminClient.describeTopics(Seq(topic).asJava).all().get
    return descriptions.get(topic).partitions().get(0).replicas().asScala.map(n => n.id).toSet
  }

  def createTopic(topic: String): CreateTopicsResult = {
    adminClient.createTopics(Collections.singleton(new NewTopic(topic, 1, 2.toShort)))
  }

}

object UnderReplicatedTopicCreationTest {
  @Parameters(name = "enableUnderReplicatedTopicCreation={0}")
  def parameters: java.util.Collection[Array[Object]] = {
    val data = new java.util.ArrayList[Array[Object]]()
    for (enableUnderReplicatedTopicCreation <- Array(JBoolean.TRUE, JBoolean.FALSE))
      data.add(Array(enableUnderReplicatedTopicCreation))
    data
  }
}
