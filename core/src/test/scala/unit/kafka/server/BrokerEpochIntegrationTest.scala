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

import kafka.api.LeaderAndIsr
import kafka.cluster.Broker
import kafka.controller.{ControllerChannelManager, ControllerContext, StateChangeLogger}
import kafka.utils.TestUtils
import kafka.utils.TestUtils.createTopic
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.UpdateMetadataRequest.EndPoint
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Time
import org.junit.Assert._
import org.junit.{After, Before, Test}

import scala.collection.JavaConverters._
import scala.collection.mutable

class BrokerEpochIntegrationTest extends ZooKeeperTestHarness {
  val brokerId1 = 0
  val brokerId2 = 1

  var servers: Seq[KafkaServer] = Seq.empty[KafkaServer]

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
    val controller = getController
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

  @Test
  def testControlRequestWithStaleBrokerEpoch() {
    // start 2 brokers
    val topic = "new-topic"
    val partitionId = 0

    // create topic with 1 partition, 2 replicas, one on each broker
    createTopic(zkClient, topic, partitionReplicaAssignment = Map(0 -> Seq(0, 1)), servers = servers)

    // start another controller
    val controllerId = 2

    val controllerConfig = KafkaConfig.fromProps(TestUtils.createBrokerConfig(controllerId, zkConnect))
    val securityProtocol = SecurityProtocol.PLAINTEXT
    val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
    val brokers = servers.map(s => new Broker(s.config.brokerId, "localhost", TestUtils.boundPort(s), listenerName,
      securityProtocol))
    val nodes = brokers.map(_.node(listenerName))

    val controllerContext = new ControllerContext
    controllerContext.liveBrokers = brokers.toSet
    val metrics = new Metrics
    val controllerChannelManager = new ControllerChannelManager(controllerContext, controllerConfig, Time.SYSTEM,
      metrics, new StateChangeLogger(controllerId, inControllerContext = true, None))
    controllerChannelManager.startup()

    try {
      // Send LeaderAndIsr request with stale broker epoch
      {
        val partitionStates = Map(
          new TopicPartition(topic, partitionId) -> new LeaderAndIsrRequest.PartitionState(2, brokerId2, LeaderAndIsr.initialLeaderEpoch,
            Seq(brokerId1, brokerId2).map(Integer.valueOf).asJava, LeaderAndIsr.initialZKVersion,
            Seq(0, 1).map(Integer.valueOf).asJava, false)
        )
        val requestBuilder = new LeaderAndIsrRequest.Builder(
          ApiKeys.LEADER_AND_ISR.latestVersion, controllerId, zkClient.getControllerEpoch.get._1,
          servers(1).brokerEpoch.get - 1, // Stale broker epoch
          partitionStates.asJava, nodes.toSet.asJava)

        verifyStaleBrokerEpochInResponse(controllerChannelManager, ApiKeys.UPDATE_METADATA, requestBuilder)
      }

      // Send UpdateMetadata request with stale broker epoch
      {
        val partitionStates = Map(
          new TopicPartition(topic, partitionId) -> new UpdateMetadataRequest.PartitionState(2, brokerId2, LeaderAndIsr.initialLeaderEpoch,
            Seq(brokerId1, brokerId2).map(Integer.valueOf).asJava, LeaderAndIsr.initialZKVersion,
            Seq(0, 1).map(Integer.valueOf).asJava, Seq.empty.asJava)
        )
        val liverBrokers = brokers.map { broker =>
          val securityProtocol = SecurityProtocol.PLAINTEXT
          val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
          val node = broker.node(listenerName)
          val endPoints = Seq(new EndPoint(node.host, node.port, securityProtocol, listenerName))
          new UpdateMetadataRequest.Broker(broker.id, endPoints.asJava, broker.rack.orNull)
        }
        val requestBuilder = new UpdateMetadataRequest.Builder(
          ApiKeys.UPDATE_METADATA.latestVersion, controllerId, zkClient.getControllerEpoch.get._1,
          servers(1).brokerEpoch.get - 1, // Stale broker epoch
          partitionStates.asJava, liverBrokers.toSet.asJava)

        verifyStaleBrokerEpochInResponse(controllerChannelManager, ApiKeys.UPDATE_METADATA, requestBuilder)
      }

      // Send StopReplica request with stale broker epoch
      {
        val requestBuilder = new StopReplicaRequest.Builder(
          ApiKeys.STOP_REPLICA.latestVersion, controllerId, zkClient.getControllerEpoch.get._1,
          servers(1).brokerEpoch.get - 1, // Stale broker epoch
          false, Set(new TopicPartition(topic, partitionId)).asJava)

        verifyStaleBrokerEpochInResponse(controllerChannelManager, ApiKeys.STOP_REPLICA, requestBuilder)
      }
    } finally {
      controllerChannelManager.shutdown()
      metrics.close()
    }
  }

  private def getController: KafkaServer = {
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

  private def verifyStaleBrokerEpochInResponse(controllerChannelManager: ControllerChannelManager, apiKeys: ApiKeys,
                                               builder: AbstractControlRequest.Builder[_ <: AbstractControlRequest]): Unit = {
    var staleBrokerEpochDetected = false
    controllerChannelManager.sendRequest(brokerId2, apiKeys, builder,
      response => {staleBrokerEpochDetected = response.errorCounts().containsKey(Errors.STALE_BROKER_EPOCH)})
    TestUtils.waitUntilTrue(() => staleBrokerEpochDetected, "Broker epoch should be stale")
    assertTrue("Stale broker epoch not detected by the broker", staleBrokerEpochDetected)
  }
}
