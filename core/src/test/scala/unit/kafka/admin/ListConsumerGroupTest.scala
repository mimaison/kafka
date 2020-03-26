/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.admin

import joptsimple.OptionException
import org.junit.Assert._
import org.junit.Test
import kafka.utils.TestUtils
import org.apache.kafka.common.ConsumerGroupState
import org.apache.kafka.clients.admin.ConsumerGroupListing
import java.util.Optional

class ListConsumerGroupTest extends ConsumerGroupCommandTest {

  @Test
  def testListConsumerGroups(): Unit = {
    val simpleGroup = "simple-group"
    addSimpleGroupExecutor(group = simpleGroup)
    addConsumerGroupExecutor(numConsumers = 1)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--list")
    val service = getConsumerGroupService(cgcArgs)

    val expectedGroups = Set(group, simpleGroup)
    var foundGroups = Set.empty[String]
    TestUtils.waitUntilTrue(() => {
      foundGroups = service.listConsumerGroups().toSet
      expectedGroups == foundGroups
    }, s"Expected --list to show groups $expectedGroups, but found $foundGroups.")
  }

  @Test(expected = classOf[OptionException])
  def testListWithUnrecognizedNewConsumerOption(): Unit = {
    val cgcArgs = Array("--new-consumer", "--bootstrap-server", brokerList, "--list")
    getConsumerGroupService(cgcArgs)
  }

  @Test
  def testListConsumerGroupsWithStates(): Unit = {
    val simpleGroup = "simple-group"
    addSimpleGroupExecutor(group = simpleGroup)
    addConsumerGroupExecutor(numConsumers = 1)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--list", "--state")
    val service = getConsumerGroupService(cgcArgs)

    val expectedListing = Set(
        new ConsumerGroupListing(simpleGroup, true, Optional.of(ConsumerGroupState.EMPTY)),
        new ConsumerGroupListing(group, false, Optional.of(ConsumerGroupState.STABLE)))

    var foundListing = Set.empty[ConsumerGroupListing]
    TestUtils.waitUntilTrue(() => {
      foundListing = service.listConsumerGroupsWithState(ConsumerGroupState.values.toList).toSet
      expectedListing == foundListing
    }, s"Expected to show groups $expectedListing, but found $foundListing")

    val expectedListingStable = Set(
        new ConsumerGroupListing(group, false, Optional.of(ConsumerGroupState.STABLE)))

    foundListing = Set.empty[ConsumerGroupListing]
    TestUtils.waitUntilTrue(() => {
      foundListing = service.listConsumerGroupsWithState(List(ConsumerGroupState.STABLE)).toSet
      expectedListingStable == foundListing
    }, s"Expected to show groups expectedListingStable, but found $foundListing")
  }

  @Test
  def testConsumerGroupStatesFromString(): Unit = {
    val result = ConsumerGroupCommand.consumerGroupStatesFromString("STABLE, stable, Stable, eMpTy")
    assertEquals(List(ConsumerGroupState.STABLE, ConsumerGroupState.EMPTY), result)

    try {
      ConsumerGroupCommand.consumerGroupStatesFromString("bad, wrong")
    } catch {
      case e: IllegalArgumentException => //Expected
    }

    try {
      ConsumerGroupCommand.consumerGroupStatesFromString("  bad, ")
    } catch {
      case e: IllegalArgumentException => //Expected
    }

    try {
      ConsumerGroupCommand.consumerGroupStatesFromString("  bad, stable")
    } catch {
      case e: IllegalArgumentException => //Expected
    }

    try {
      ConsumerGroupCommand.consumerGroupStatesFromString("   ,   ,")
    } catch {
      case e: IllegalArgumentException => //Expected
    }
  }

}
