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

import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.junit.Assert._
import org.junit.{After, Before, Test}

import scala.collection.mutable

class BrokerEpochIntegrationTest extends ZooKeeperTestHarness {
  val brokerId1 = 0
  val brokerId2 = 1

  var servers: Seq[KafkaServer] = Seq.empty[KafkaServer]

  var staleControllerEpochDetected = false
  var staleBrokerEpochDetected = false

  @Before
  override def setUp() {
    super.setUp()

    // start both servers
    val server1 = TestUtils.createServer(KafkaConfig.fromProps(TestUtils.createBrokerConfig(brokerId1, zkConnect)))
    val server2 = TestUtils.createServer(KafkaConfig.fromProps(TestUtils.createBrokerConfig(brokerId2, zkConnect)))
    servers ++= List(server1, server2)
  }

  @After
  override def tearDown() {
    TestUtils.shutdownServers(servers)
    super.tearDown()
  }

  @Test
  def testReplicaManagerBrokerEpochMatchesWithZk(): Unit = {
    val brokerAndEpochs = zkClient.getAllBrokerAndEpochsInCluster
    assertEquals(brokerAndEpochs.size, servers.size)
    brokerAndEpochs.foreach {
      case (broker, epoch) =>
        val brokerServer = servers.find(e => e.config.brokerId == broker.id)
        assertTrue(brokerServer.isDefined)
        assertEquals(epoch, brokerServer.get.replicaManager.brokerEpoch.curBrokerEpoch)
    }
  }

  @Test
  def testControllerBrokerEpochCacheMatchesWithZk(): Unit = {
    val controller = getController()
    val otherBroker = servers.find(e => e.config.brokerId != controller.config.brokerId).get
    val controllerBrokerEpochsCache = controller.kafkaController.controllerContext.brokerEpochsCache

    // Broker epochs cache matches with zk in steady state
    checkControllerBrokerEpochsCacheMatchesWithZk(controllerBrokerEpochsCache)

    // Shutdown a broker and make sure broker epochs cache still matches with zk state
    otherBroker.shutdown()
    otherBroker.awaitShutdown()
    checkControllerBrokerEpochsCacheMatchesWithZk(controllerBrokerEpochsCache)

    // Restart a broker and make sure broker epochs cache still matches with zk state
    otherBroker.startup()
    checkControllerBrokerEpochsCacheMatchesWithZk(controllerBrokerEpochsCache)
  }

  private def getController(): KafkaServer = {
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    servers.filter(s => s.config.brokerId == controllerId).head
  }

  private def checkControllerBrokerEpochsCacheMatchesWithZk(brokerEpochsCache: mutable.Map[Int, Long]): Unit = {
    val brokerAndEpochs = zkClient.getAllBrokerAndEpochsInCluster
    assertEquals(brokerAndEpochs.size, brokerEpochsCache.size)
    brokerAndEpochs.foreach {
      case (broker, epoch) =>
        assertTrue(brokerEpochsCache.contains(broker.id))
        assertEquals(epoch, brokerEpochsCache(broker.id))
    }
  }

}
