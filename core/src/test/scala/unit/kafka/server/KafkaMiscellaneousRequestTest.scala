/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package kafka.server

import java.net.Socket
import java.util.Properties

import kafka.utils.TestUtils
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.requests.{ListGroupsRequest,ListGroupsResponse}
import org.apache.kafka.common.metrics.MetricsReporter
import org.apache.kafka.common.metrics.KafkaMetric
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.protocol.Errors

import org.junit.Assert._
import org.junit.{Before, Test}
import org.junit.After
import java.util.concurrent.atomic.AtomicInteger
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder
import org.apache.kafka.common.security.auth.AuthenticationContext
import org.apache.kafka.common.security.auth.KafkaPrincipal

/*
 * This test bundles different, independent checks in one class for speeding up the build at the expense of clarity.
 * 
 * Checks that a reporter that throws an exception will not affect other reporters
 * and will not affect the broker's message handling
 * 
 * Checks that a Listenername is available in the Authentication Context
 */
class KafkaMiscellaneousRequestTest extends BaseRequestTest {

  override def numBrokers: Int = 1

  override def propertyOverrides(properties: Properties): Unit = {
    // for testing MetricReporterExceptionHandling
    properties.put(KafkaConfig.MetricReporterClassesProp, classOf[KafkaMetricReporterExceptionHandlingTest.BadReporter].getName + "," + classOf[KafkaMetricReporterExceptionHandlingTest.GoodReporter].getName)

    // for testing ListenerAndPrincipalBuilder
    properties.put(KafkaConfig.PrincipalBuilderClassProp, classOf[ListenerAndPrincipalBuilderTest.TestPrincipalBuilder].getName)
    properties.put(KafkaConfig.ListenersProp, "PLAINTEXT://localhost:0,CUSTOM://localhost:0")
    properties.put(KafkaConfig.ListenerSecurityProtocolMapProp, "PLAINTEXT:PLAINTEXT,CUSTOM:PLAINTEXT")
  }

  @Before
  override def setUp() {
    super.setUp()

    TestUtils.retry(10000) {
      // controller to node connection using the default security.inter.broker.protocol
      assertEquals("PLAINTEXT", ListenerAndPrincipalBuilderTest.lastListenerName)
    }

    // need a quota prop to register a "throttle-time" metrics after server startup
    val quotaProps = new Properties()
    quotaProps.put(DynamicConfig.Client.RequestPercentageOverrideProp, "0.1")
    adminZkClient.changeClientIdConfig("<default>", quotaProps)
  }

  @After
  override def tearDown() {
    KafkaMetricReporterExceptionHandlingTest.goodReporterRegistered.set(0)
    KafkaMetricReporterExceptionHandlingTest.badReporterRegistered.set(0)
    
    super.tearDown()
  }

  @Test
  def testMiscellaneous() {
    val port = anySocketServer.boundPort(new ListenerName("CUSTOM"))
    val socket = new Socket("localhost", port)
    socket.setSoTimeout(10000)

    try {
      TestUtils.retry(10000) {
        // checks for KafkaMetricReporterExceptionHandling
        val error = new ListGroupsResponse(requestResponse(socket, "clientId", 0, new ListGroupsRequest.Builder())).error()
        assertEquals(Errors.NONE, error)
        assertEquals(KafkaMetricReporterExceptionHandlingTest.goodReporterRegistered.get, KafkaMetricReporterExceptionHandlingTest.badReporterRegistered.get)
        assertTrue(KafkaMetricReporterExceptionHandlingTest.goodReporterRegistered.get > 0)

        // using the simplest possible builder to send a request for the ListenerAndPrincipalBuilderTest
        requestResponse(socket, "clientId", 0, new ListGroupsRequest.Builder())
      }
    } finally {
      socket.close()
    }
    
    // check for ListenerAndPrincipalBuilder
    assertEquals("CUSTOM", ListenerAndPrincipalBuilderTest.lastListenerName)
  }
}

object KafkaMetricReporterExceptionHandlingTest {
  var goodReporterRegistered = new AtomicInteger
  var badReporterRegistered = new AtomicInteger

  class GoodReporter extends MetricsReporter {

    def configure(configs: java.util.Map[String, _]) {
    }

    def init(metrics: java.util.List[KafkaMetric]) {
    }

    def metricChange(metric: KafkaMetric) {
      if (metric.metricName.group == "Request") {
        goodReporterRegistered.incrementAndGet
      }
    }

    def metricRemoval(metric: KafkaMetric) {
    }

    def close() {
    }
  }

  class BadReporter extends GoodReporter {

    override def metricChange(metric: KafkaMetric) {
      if (metric.metricName.group == "Request") {
        badReporterRegistered.incrementAndGet
        throw new RuntimeException(metric.metricName.toString)
      }
    }
  }
}

object ListenerAndPrincipalBuilderTest {
  var lastListenerName = ""

  class TestPrincipalBuilder extends KafkaPrincipalBuilder {
    override def build(context: AuthenticationContext): KafkaPrincipal = {
      lastListenerName = context.listenerName
      KafkaPrincipal.ANONYMOUS
    }
  }
}

