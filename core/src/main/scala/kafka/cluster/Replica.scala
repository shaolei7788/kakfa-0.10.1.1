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

package kafka.cluster

import kafka.log.Log
import kafka.utils.{SystemTime, Time, Logging}
import kafka.server.{LogReadResult, LogOffsetMetadata}
import kafka.common.KafkaException

import java.util.concurrent.atomic.AtomicLong

//目的是为了增加kafka集群的高可用 每个副本对应一个Log
// 副本编号表示对应broker的编号
class Replica(val brokerId: Int,
              val partition: Partition,//副本对应的分区
              time: Time = SystemTime,
              initialHighWatermarkValue: Long = 0L,
              val log: Option[Log] = None) extends Logging {
  // the high watermark offset value, in non-leader replicas only its message offsets are kept
  //此字段由leader副本负责更新维护 HW 最高水位
  // 主副本的最高水位取决于ISR中所有副本的最小偏移量
  // 备份副本的最高水平取决于主副本的最高水位和它自己的偏移量，它会选择这两者的最小值
  @volatile private[this] var highWatermarkMetadata: LogOffsetMetadata = new LogOffsetMetadata(initialHighWatermarkValue)
  // the log end offset value, kept in all replicas;
  // 记录的是追加到log中最新消息的offset 可直接从Log.nextOffsetMetadata获取  LEO
  // 追加消息到主副本的本地日志，备份副本拉取消息到自己的本地日志，会更新日志的偏移量
  // 主副本所在的服务端处理备份副本的拉取请求，也会更新分区中备份副本对应的偏移量
  @volatile private[this] var logEndOffsetMetadata: LogOffsetMetadata = LogOffsetMetadata.UnknownOffsetMetadata

  val topic = partition.topic
  val partitionId = partition.partitionId

  def isLocal: Boolean = {
    log match {
      case Some(l) => true
      case None => false
    }
  }
  //记录follower副本最后一次赶上leader 时间戳
  private[this] val lastCaughtUpTimeMsUnderlying = new AtomicLong(time.milliseconds)

  def lastCaughtUpTimeMs = lastCaughtUpTimeMsUnderlying.get()

  //更新日志的读取结果，一般针对远程副本
  def updateLogReadResult(logReadResult : LogReadResult) {
    logEndOffset = logReadResult.info.fetchOffsetMetadata
    //代表读到最后了
    if(logReadResult.isReadFromLogEnd) {
      lastCaughtUpTimeMsUnderlying.set(time.milliseconds)
    }
  }

  //更新副本的偏移量元数据，只有远程副本可以更新
  private def logEndOffset_= (newLogEndOffset: LogOffsetMetadata) {
    if (isLocal) {
      throw new KafkaException("Should not set log end offset on partition [%s,%d]'s local replica %d".format(topic, partitionId, brokerId))
    } else {
      logEndOffsetMetadata = newLogEndOffset
      trace("Setting log end offset for replica %d for partition [%s,%d] to [%s]".format(brokerId, topic, partitionId, logEndOffsetMetadata))
    }
  }

  //获取副本的偏移量元数据，本地副本通过日志文件读取
  def logEndOffset =
    if (isLocal)
      log.get.logEndOffsetMetadata
    else
      logEndOffsetMetadata

  //设置副本的最高水位线，只有本地副本可以更新
  def highWatermark_=(newHighWatermark: LogOffsetMetadata) {
    if (isLocal) {
      highWatermarkMetadata = newHighWatermark
      trace("Setting high watermark for replica %d partition [%s,%d] on broker %d to [%s]"
        .format(brokerId, topic, partitionId, brokerId, newHighWatermark))
    } else {
      throw new KafkaException("Should not set high watermark on partition [%s,%d]'s non-local replica %d".format(topic, partitionId, brokerId))
    }
  }

  //获取副本的最高水位线
  def highWatermark = highWatermarkMetadata

  //以最新HW的offset读取Log
  def convertHWToLocalOffsetMetadata() = {
    if (isLocal) {
      highWatermarkMetadata = log.get.convertToOffsetMetadata(highWatermarkMetadata.messageOffset)
    } else {
      throw new KafkaException("Should not construct complete high watermark on partition [%s,%d]'s non-local replica %d".format(topic, partitionId, brokerId))
    }
  }

  override def equals(that: Any): Boolean = {
    if(!that.isInstanceOf[Replica])
      return false
    val other = that.asInstanceOf[Replica]
    if(topic.equals(other.topic) && brokerId == other.brokerId && partition.equals(other.partition))
      return true
    false
  }

  override def hashCode(): Int = {
    31 + topic.hashCode() + 17*brokerId + partition.hashCode()
  }


  override def toString: String = {
    val replicaString = new StringBuilder
    replicaString.append("ReplicaId: " + brokerId)
    replicaString.append("; Topic: " + topic)
    replicaString.append("; Partition: " + partition.partitionId)
    replicaString.append("; isLocal: " + isLocal)
    if(isLocal) replicaString.append("; Highwatermark: " + highWatermark)
    replicaString.toString()
  }
}
