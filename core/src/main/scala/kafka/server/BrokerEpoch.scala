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

import java.util.concurrent.CountDownLatch
import java.util.concurrent.locks.ReentrantReadWriteLock
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}

import org.apache.kafka.common.requests.AbstractControlRequest

class BrokerEpoch(brokerId: Int) {
  var curBrokerEpoch: Long = AbstractControlRequest.UNKNOWN_BROKER_EPOCH
  val readWriteLock = new ReentrantReadWriteLock
  val initializeLatch = new CountDownLatch(1)

  def get: Long = {
    initializeLatch.await()
    inReadLock(readWriteLock) {
      curBrokerEpoch
    }
  }

  def update(newEpoch: Long): Unit = {
    inWriteLock(readWriteLock) {
      if (newEpoch > curBrokerEpoch) curBrokerEpoch = newEpoch
    }
    initializeLatch.countDown()
  }
}
