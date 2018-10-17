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
