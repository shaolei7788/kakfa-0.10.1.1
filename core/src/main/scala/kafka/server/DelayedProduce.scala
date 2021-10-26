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


import java.util.concurrent.TimeUnit

import com.yammer.metrics.core.Meter
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.Pool
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse

import scala.collection._

case class ProducePartitionStatus(requiredOffset: Long, responseStatus: PartitionResponse) {
  @volatile var acksPending = false

  override def toString = "[acksPending: %b, error: %d, startOffset: %d, requiredOffset: %d]"
    .format(acksPending, responseStatus.errorCode, responseStatus.baseOffset, requiredOffset)
}

/**
 * The produce metadata maintained by the delayed produce operation
 */
case class ProduceMetadata(produceRequiredAcks: Short,
                           produceStatus: Map[TopicPartition, ProducePartitionStatus]) {

  override def toString = "[requiredAcks: %d, partitionStatus: %s]"
    .format(produceRequiredAcks, produceStatus)
}

/**
 * A delayed produce operation that can be created by the replica manager and watched
 * in the produce operation purgatory
 */
//总结服务端创建延迟的操作对象，在尝试完成时根据主副本的最高水位判断，具体如下
//1 服务端处理生产者的拉取请求，写入消息集到主副本的本地日志
//2 服务端返回追加消息集的下一个偏移量，并且创建一个延迟的操作对象
//3 服务端处理器备份副本的拉取请求，首先会读取主副本的本地日志
//4 服务端返回读取消息集的偏移量，并更新备份副本的偏移量
//5 更新主副本的最高水位，选择ISR中所有备份副本中最小的偏移量
//6 如果主副本的最高水位超过指定的偏移量，则完成延迟的生产操作

//如果是延迟生产，根据主副本的最高水位是否超过指定的偏移量(requiredOffset)
//如果是备份副本的延迟拉取，它的外部事件是消息集追加到主副本，判断它与fetchOffset拉取偏移量的差距是否超过fetchMinBytes
//如果是消费者的延迟拉取，它的外部事件是增加主副本的最高水位,判断它与fetchOffset拉取偏移量的差距是否超过fetchMinBytes
class DelayedProduce(delayMs: Long,
                     produceMetadata: ProduceMetadata,
                     replicaManager: ReplicaManager,
                     responseCallback: Map[TopicPartition, PartitionResponse] => Unit)
  extends DelayedOperation(delayMs) {

  // first update the acks pending variable according to the error code
  produceMetadata.produceStatus.foreach { case (topicPartition, status) =>
    if (status.responseStatus.errorCode == Errors.NONE.code) {
      // Timeout error state will be cleared when required acks are received
      //将 status.acksPending 设置为true 表示主分区正在等待应答
      status.acksPending = true
      status.responseStatus.errorCode = Errors.REQUEST_TIMED_OUT.code
    } else {
      status.acksPending = false
    }

    trace("Initial partition status for %s is %s".format(topicPartition, status))
  }

  /**
   * The delayed produce operation can be completed if every partition
   * it produces to is satisfied by one of the following:
   *
   * Case A: This broker is no longer the leader: set an error in response
   * Case B: This broker is the leader:
   *   B.1 - If there was a local error thrown while checking if at least requiredAcks
   *         replicas have caught up to this operation: set an error in response
   *   B.2 - Otherwise, set the response with no error.
   */
  override def tryComplete(): Boolean = {
    // check for each partition if it still has pending acks
    produceMetadata.produceStatus.foreach { case (topicAndPartition, status) =>
      trace(s"Checking produce satisfaction for ${topicAndPartition}, current status $status")
      // skip those partitions that have already been satisfied
      if (status.acksPending) {
        //获取指定topic，分区id的分区对象
        val (hasEnough, error) = replicaManager.getPartition(topicAndPartition.topic, topicAndPartition.partition) match {
          case Some(partition) =>
            //检测其参数指定的偏移量的消息是否已经被ISR集合中所有的follower副本同步
            partition.checkEnoughReplicasReachOffset(status.requiredOffset)
          case None =>
            // Case A
            (false, Errors.UNKNOWN_TOPIC_OR_PARTITION)
        }
        if (error != Errors.NONE || hasEnough) {
          // error != Errors.NONE 有错误
          //hasEnough 为true ,即分区的ISR备份副本都同步了写入主副本的消息集会更改
          status.acksPending = false
          status.responseStatus.errorCode = error.code
        }
      }
    }

    // check if every partition has satisfied at least one of case A or B
    //判断是否可以完成延迟的操作
    if (!produceMetadata.produceStatus.values.exists(_.acksPending)) {
      //当分区的acksPending都等于false, 条件满足了，强制完成延迟操作
      forceComplete()
    } else
      false
  }

  override def onExpiration() {
    produceMetadata.produceStatus.foreach { case (topicPartition, status) =>
      if (status.acksPending) {
        DelayedProduceMetrics.recordExpiration(topicPartition)
      }
    }
  }

  /**
   * Upon completion, return the current response status along with the error code per partition
   */
  override def onComplete() {
    //执行响应回调
    val responseStatus = produceMetadata.produceStatus.mapValues(status => status.responseStatus)
    responseCallback(responseStatus)
  }
}

object DelayedProduceMetrics extends KafkaMetricsGroup {

  private val aggregateExpirationMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS)

  private val partitionExpirationMeterFactory = (key: TopicPartition) =>
    newMeter("ExpiresPerSec",
             "requests",
             TimeUnit.SECONDS,
             tags = Map("topic" -> key.topic, "partition" -> key.partition.toString))
  private val partitionExpirationMeters = new Pool[TopicPartition, Meter](valueFactory = Some(partitionExpirationMeterFactory))

  def recordExpiration(partition: TopicPartition) {
    aggregateExpirationMeter.mark()
    partitionExpirationMeters.getAndMaybePut(partition).mark()
  }
}

