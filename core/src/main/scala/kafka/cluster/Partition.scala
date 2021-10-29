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

import kafka.common._
import kafka.utils._
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.admin.AdminUtils
import kafka.api.LeaderAndIsr
import kafka.log.{Log, LogConfig}
import kafka.server._
import kafka.metrics.KafkaMetricsGroup
import kafka.controller.KafkaController
import kafka.message.ByteBufferMessageSet
import java.io.IOException
import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.kafka.common.errors.{NotEnoughReplicasException, NotLeaderForPartitionException}
import org.apache.kafka.common.protocol.Errors

import scala.collection.JavaConverters._
import com.yammer.metrics.core.Gauge
import org.apache.kafka.common.requests.PartitionState

/**
 * Data structure that represents a topic partition. The leader maintains the AR, ISR, CUR, RAR
 */
//分区是一个有状态的数据结构
//负责管理每个副本对应的replica对象，进行leader切换，ISR集合的管理以及调用日志存储子系统完成写入消息
class Partition(val topic: String,
                val partitionId: Int,
                time: Time,
                replicaManager: ReplicaManager) extends Logging with KafkaMetricsGroup {
  //当前broker的id
  private val localBrokerId = replicaManager.config.brokerId

  //当前broker上的logManager
  private val logManager = replicaManager.logManager

  private val zkUtils = replicaManager.zkUtils
  //维护了该分区的全部副本集合 AR  key=brokerid  value = Replica
  private val assignedReplicaMap = new Pool[Int, Replica]
  // The read lock is only required when multiple reads are executed and needs to be in a consistent manner
  private val leaderIsrUpdateLock = new ReentrantReadWriteLock()
  // zkVersion = 0
  private var zkVersion: Int = LeaderAndIsr.initialZKVersion
  //初始leaderEpoch版本 -1
  @volatile private var leaderEpoch: Int = LeaderAndIsr.initialLeaderEpoch - 1

  //该分区leader副本的id
  @volatile var leaderReplicaIdOpt: Option[Int] = None

  //维护了该分区的ISR集合
  @volatile var inSyncReplicas: Set[Replica] = Set.empty[Replica]

  //控制器选举版本 -1
  private var controllerEpoch: Int = KafkaController.InitialControllerEpoch - 1
  this.logIdent = "Partition [%s,%d] on broker %d: ".format(topic, partitionId, localBrokerId)

  //根据给定的副本编号，判断它是不是本地副本
  private def isReplicaLocal(replicaId: Int) : Boolean = replicaId == localBrokerId

  val tags = Map("topic" -> topic, "partition" -> partitionId.toString)

  // 性能监控 Gauge implements Metric
  newGauge("UnderReplicated",
    new Gauge[Int] {
      def value = {
        if (isUnderReplicated) 1 else 0
      }
    },
    tags
  )

  //验证分区
  def isUnderReplicated(): Boolean = {
    leaderReplicaIfLocal() match {
      case Some(_) =>
        inSyncReplicas.size < assignedReplicas.size
      case None =>
        false
    }
  }

  //根据给定的副本编号获取或创建副本
  //创建副本对象时，从检查点文件(replication-offset-checkpoint)读取分区的HW作为初始的最高水位
  def getOrCreateReplica(replicaId: Int = localBrokerId): Replica = {
    //获取副本编号指定的副本
    val replicaOpt = getReplica(replicaId)
    replicaOpt match {
      //查找到指定的replica对象，直接返回
      case Some(replica) => replica
      //没查找到指定的replica对象，则创建一个
      case None =>
        //判断是否为本地副本
        if (isReplicaLocal(replicaId)) {
          val config = LogConfig.fromProps(logManager.defaultConfig.originals, AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic, topic))
          //创建local replica 对应的log对象
          val log: Log = logManager.createLog(TopicAndPartition(topic, partitionId), config)
          //获取指定log目录对应的OffsetCheckpoint对象，它负责管理该log目录下的 replication-offset-checkpoint 文件
          //log.dir是日志目录  log.dir.getParentFile 是数据目录
          val checkpoint: OffsetCheckpoint = replicaManager.highWatermarkCheckpoints(log.dir.getParentFile.getAbsolutePath)
          //读取replication-offset-checkpoint文件形成map
          val offsetMap = checkpoint.read
          if (!offsetMap.contains(TopicAndPartition(topic, partitionId)))
            info("No checkpointed highwatermark is found for partition [%s,%d]".format(topic, partitionId))
          //根据TopicAndPartition 找到对应的HW，再与LEO比较，此值会作为此副本的HW
          val offset = offsetMap.getOrElse(TopicAndPartition(topic, partitionId), 0L).min(log.logEndOffset)
          //todo 创建Replica对象并添加到 assignedReplicaMap
          val localReplica = new Replica(replicaId, this, time, offset, Some(log))
          //添加本地副本
          addReplicaIfNotExists(localReplica)
        } else {
          //远程副本
          val remoteReplica = new Replica(replicaId, this, time)
          addReplicaIfNotExists(remoteReplica)
        }
        getReplica(replicaId).get
    }
  }

  //broker  localBrokerId leaderReplica leaderReplicaIfLocal  getReplica
  //1         1             2               None              Some(Replica(1))
  //2         2             2               Some(Replica(2))  Some(Replica(2))
  //3         3             2               None              Some(Replica(3))
  //获取指定replicaId的副本，默认是当前broker对应的编号
  def getReplica(replicaId: Int = localBrokerId): Option[Replica] = {
    val replica = assignedReplicaMap.get(replicaId)
    if (replica == null) {
      None
    } else {
      Some(replica)
    }
  }

  //获取分区的主副本，并且必须是本地副本，如果不是本地副本返回None,只有主副本才有数据
  def leaderReplicaIfLocal(): Option[Replica] = {
    //leaderReplicaIdOpt 该分区leader副本的id
    leaderReplicaIdOpt match {
      case Some(leaderReplicaId) =>
        if (leaderReplicaId == localBrokerId)
          //获取副本
          getReplica(localBrokerId)
        else
          None
      case None => None
    }
  }

  def addReplicaIfNotExists(replica: Replica) = {
    assignedReplicaMap.putIfNotExists(replica.brokerId, replica)
  }

  def assignedReplicas(): Set[Replica] = {
    assignedReplicaMap.values.toSet
  }

  def removeReplica(replicaId: Int) {
    assignedReplicaMap.remove(replicaId)
  }

  def delete() {
    // need to hold the lock to prevent appendMessagesToLeader() from hitting I/O exceptions due to log being deleted
    inWriteLock(leaderIsrUpdateLock) {
      assignedReplicaMap.clear()
      inSyncReplicas = Set.empty[Replica]
      leaderReplicaIdOpt = None
      try {
        logManager.deleteLog(TopicAndPartition(topic, partitionId))
        removePartitionMetrics()
      } catch {
        case e: IOException =>
          fatal("Error deleting the log for partition [%s,%d]".format(topic, partitionId), e)
          Runtime.getRuntime().halt(1)
      }
    }
  }

  def getLeaderEpoch(): Int = {
    this.leaderEpoch
  }

  /**
   * Make the local replica the leader by resetting LogEndOffset for remote replicas (there could be old LogEndOffset
   * from the time when this broker was the leader last time) and setting the new leader and ISR.
   * If the leader replica id does not change, return false to indicate the replica manager.
   */
  def makeLeader(controllerId: Int, partitionStateInfo: PartitionState, correlationId: Int): Boolean = {
    val (leaderHWIncremented, isNewLeader) = inWriteLock(leaderIsrUpdateLock) {
      //获取需要分配的AR
      val allReplicas = partitionStateInfo.replicas.asScala.map(_.toInt)
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path
      controllerEpoch = partitionStateInfo.controllerEpoch
      // 创建AR集合中所有副本对应的replica对象
      allReplicas.foreach(replica => getOrCreateReplica(replica))
      //todo 分区状态信息包含主副本，ISR,AR
      val newInSyncReplicas = partitionStateInfo.isr.asScala.map(r => getOrCreateReplica(r)).toSet
      // 移除不在AR里的副本
      (assignedReplicas().map(_.brokerId) -- allReplicas).foreach(removeReplica(_))
      inSyncReplicas = newInSyncReplicas
      leaderEpoch = partitionStateInfo.leaderEpoch
      zkVersion = partitionStateInfo.zkVersion
      val isNewLeader: Boolean =
        //检测leader 是否发生变化
        if (leaderReplicaIdOpt.isDefined && leaderReplicaIdOpt.get == localBrokerId) {
          //leader未变化
          false
        } else {
          //leader之前并不在此broker上，即leader发生变化，更新leaderReplicaIdOpt
          leaderReplicaIdOpt = Some(localBrokerId)
          true
        }
      //获取local replica
      val leaderReplica = getReplica().get
      // we may need to increment high watermark since ISR could be down to 1
      if (isNewLeader) {
        //初始化leader的hw
        leaderReplica.convertHWToLocalOffsetMetadata()
        // 重置所有的remote replica 的LEO为-1
        assignedReplicas.filter(_.brokerId != localBrokerId).foreach(_.updateLogReadResult(LogReadResult.UnknownLogReadResult))
      }
      //尝试更新HW
      (maybeIncrementLeaderHW(leaderReplica), isNewLeader)
    }
    // leader 副本的HW增加了，则可能有DelayedFetch满足执行条件
    if (leaderHWIncremented) {
      //尝试完成延迟的请求
      tryCompleteDelayedRequests()
    }
    isNewLeader
  }

  /**
   *  Make the local replica the follower by setting the new leader and ISR to empty
   *  If the leader replica id does not change, return false to indicate the replica manager
   */
  def makeFollower(controllerId: Int, partitionStateInfo: PartitionState, correlationId: Int): Boolean = {
    inWriteLock(leaderIsrUpdateLock) {
      //AR
      val allReplicas = partitionStateInfo.replicas.asScala.map(_.toInt)
      val newLeaderBrokerId: Int = partitionStateInfo.leader
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path
      controllerEpoch = partitionStateInfo.controllerEpoch
      // add replicas that are new
      //todo 所有的AR副本创建副本
      allReplicas.foreach(r => getOrCreateReplica(r))
      // 更新AR
      (assignedReplicas().map(_.brokerId) -- allReplicas).foreach(removeReplica(_))
      //创建一个空集合 因为follower不维护inSyncReplicas
      inSyncReplicas = Set.empty[Replica]
      leaderEpoch = partitionStateInfo.leaderEpoch
      zkVersion = partitionStateInfo.zkVersion

      if (leaderReplicaIdOpt.isDefined && leaderReplicaIdOpt.get == newLeaderBrokerId) {
        false
      }
      else {
        leaderReplicaIdOpt = Some(newLeaderBrokerId)
        true
      }
    }
  }

  /**
   * Update the log end offset of a certain replica of this partition
   */
  //1 更新备份副本的偏移量 2 可能扩张ISR集合
  def updateReplicaLogReadResult(replicaId: Int, logReadResult: LogReadResult) {
    getReplica(replicaId) match {
      case Some(replica) =>
        //更新备份副本的偏移量
        replica.updateLogReadResult(logReadResult)
        //可能扩张ISR集合
        maybeExpandIsr(replicaId)
        debug("Recorded replica %d log end offset (LEO) position %d for partition %s."
          .format(replicaId, logReadResult.info.fetchOffsetMetadata.messageOffset, TopicAndPartition(topic, partitionId)))
      case None =>
        throw new NotAssignedReplicaException(("Leader %d failed to record follower %d's position %d since the replica" +
          " is not recognized to be one of the assigned replicas %s for partition %s.")
          .format(localBrokerId,
                  replicaId,
                  logReadResult.info.fetchOffsetMetadata.messageOffset,
                  assignedReplicas().map(_.brokerId).mkString(","),
                  TopicAndPartition(topic, partitionId)))
    }
  }

  /**
   * Check and maybe expand the ISR of the partition.
   * This function can be triggered when a replica's LEO has incremented
   */
  //todo 扩张ISR集合  replicaId 就是指定要扩张的副本id
  def maybeExpandIsr(replicaId: Int) {
    val leaderHWIncremented = inWriteLock(leaderIsrUpdateLock) {
      // check if this replica needs to be added to the ISR
      leaderReplicaIfLocal() match {
        //只有leader副本才会管理ISR集合，所以先获取leader副本对应的replica对象
        case Some(leaderReplica) =>
          val replica : Replica = getReplica(replicaId).get
          //leader的HW
          val leaderHW : LogOffsetMetadata = leaderReplica.highWatermark
          //TODO 3个要求
          // 1 follower副本不在ISR中，
          // 2 follower副本在AR中
          // 3 follower副本追上leader副本 HW
          if(!inSyncReplicas.contains(replica) &&
             assignedReplicas.map(_.brokerId).contains(replicaId) &&
             replica.logEndOffset.offsetDiff(leaderHW) >= 0) {
            //将该follower 添加到 ISR
            val newInSyncReplicas = inSyncReplicas + replica
            info("Expanding ISR for partition [%s,%d] from %s to %s"
                         .format(topic, partitionId, inSyncReplicas.map(_.brokerId).mkString(","),
                                 newInSyncReplicas.map(_.brokerId).mkString(",")))
            // 更新ISR
            updateIsr(newInSyncReplicas)
            replicaManager.isrExpandRate.mark()
          }
          //TODO 尝试更新Leader的HW
          maybeIncrementLeaderHW(leaderReplica)
        case None => false // nothing to do if no longer leader
      }
    }

    // some delayed operations may be unblocked after HW changed
    if (leaderHWIncremented) {
      //尝试完成延迟的操作
      tryCompleteDelayedRequests()
    }
  }

  /*
   * Returns a tuple where the first element is a boolean indicating whether enough replicas reached `requiredOffset`
   * and the second element is an error (which would be `Errors.NONE` for no error).
   *
   * Note that this method will only be called if requiredAcks = -1 and we are waiting for all replicas in ISR to be
   * fully caught up to the (local) leader's offset corresponding to this produce request before we acknowledge the
   * produce request.
   */
  //todo 检测其参数指定的偏移量的消息是否已经被ISR集合中所有的follower副本同步
  def checkEnoughReplicasReachOffset(requiredOffset: Long): (Boolean, Errors) = {
    //获取ledaer主副本
    leaderReplicaIfLocal() match {
      case Some(leaderReplica) =>
        // keep the current immutable replica list reference
        val curInSyncReplicas = inSyncReplicas
        //计算发送了应答的副本数量，副本的偏移量超过requiredOffset就表示发送了应答
        def numAcks = curInSyncReplicas.count { r =>
          if (!r.isLocal) {
            //不是本地副本
            if (r.logEndOffset.messageOffset >= requiredOffset) {
              trace(s"Replica ${r.brokerId} of ${topic}-${partitionId} received offset $requiredOffset")
              true
            } else {
              false
            }
          } else {
            true
          } /* also count the local (leader) replica */
        }
        trace(s"$numAcks acks satisfied for ${topic}-${partitionId} with acks = -1")
        val minIsr = leaderReplica.log.get.config.minInSyncReplicas
        //leader副本的HW要大于等于要求的偏移量，表示所有的备份副本都赶上了主副本
        if (leaderReplica.highWatermark.messageOffset >= requiredOffset) {
          /*
           * The topic may be configured not to accept messages if there are not enough replicas in ISR
           * in this scenario the request was already appended locally and then added to the purgatory before the ISR was shrunk
           */
          if (minIsr <= curInSyncReplicas.size)
            (true, Errors.NONE)
          else {
            //ISR副本数量不满足最小的设置
            (true, Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND)
          }
        } else {
          //ISR所有的备份副本没有赶上主副本
          (false, Errors.NONE)
        }
      case None =>
        (false, Errors.NOT_LEADER_FOR_PARTITION)
    }
  }

  /**
   * Check and maybe increment the high watermark of the partition;
   * this function can be triggered when
   *
   * 1. Partition ISR changed
   * 2. Any replica's LEO changed
   *
   * Returns true if the HW was incremented, and false otherwise.
   * Note There is no need to acquire the leaderIsrUpdate lock here
   * since all callers of this private API acquire that lock
   */
  //尝试后移leader副本的HW,当ISR集合发生增或减或是ISR集合任一副本LEO发生变化，都有可能导致ISR集合中最新的LEO变大
  private def maybeIncrementLeaderHW(leaderReplica: Replica): Boolean = {
    //获取ISR集合中所有LEO
    val allLogEndOffsets = inSyncReplicas.map(_.logEndOffset)
    //将ISR集合中最小的LEO作为HW
    val newHighWatermark = allLogEndOffsets.min(new LogOffsetMetadata.OffsetOrdering)
    //获取当前的hw
    val oldHighWatermark = leaderReplica.highWatermark
    if (oldHighWatermark.messageOffset < newHighWatermark.messageOffset || oldHighWatermark.onOlderSegment(newHighWatermark)) {
      //更新HW
      leaderReplica.highWatermark = newHighWatermark
      debug("High watermark for partition [%s,%d] updated to %s".format(topic, partitionId, newHighWatermark))
      true
    } else {
      debug("Skipping update high watermark since Old hw %s is larger than new hw %s for partition [%s,%d]. All leo's are %s"
        .format(oldHighWatermark, newHighWatermark, topic, partitionId, allLogEndOffsets.mkString(",")))
      false
    }
  }

  /**
   * Try to complete any pending requests. This should be called without holding the leaderIsrUpdateLock.
   */
  private def tryCompleteDelayedRequests() {
    val requestKey = new TopicPartitionOperationKey(this.topic, this.partitionId)
    replicaManager.tryCompleteDelayedFetch(requestKey)
    replicaManager.tryCompleteDelayedProduce(requestKey)
  }

  //缩减ISR
  def maybeShrinkIsr(replicaMaxLagTimeMs: Long) {
    val leaderHWIncremented = inWriteLock(leaderIsrUpdateLock) {
      leaderReplicaIfLocal() match {
        case Some(leaderReplica) =>
          //todo 获取已滞后的follower副本集合，该滞后集合中的follower副本会被剔除ISR集合
          val outOfSyncReplicas = getOutOfSyncReplicas(leaderReplica, replicaMaxLagTimeMs)
          if(outOfSyncReplicas.nonEmpty) {
            //剔除ISR集合 生产新的ISR
            val newInSyncReplicas = inSyncReplicas -- outOfSyncReplicas
            assert(newInSyncReplicas.nonEmpty)
            info("Shrinking ISR for partition [%s,%d] from %s to %s".format(topic, partitionId,
              inSyncReplicas.map(_.brokerId).mkString(","), newInSyncReplicas.map(_.brokerId).mkString(",")))
            // 更新ISR
            updateIsr(newInSyncReplicas)
            replicaManager.isrShrinkRate.mark()
            //更新leader hw
            maybeIncrementLeaderHW(leaderReplica)
          } else {
            false
          }

        case None => false // do nothing if no longer leader
      }
    }

    // some delayed operations may be unblocked after HW changed
    if (leaderHWIncremented) {
      //尝试执行延迟任务
      tryCompleteDelayedRequests()
    }
  }

  def getOutOfSyncReplicas(leaderReplica: Replica, maxLagMs: Long): Set[Replica] = {
    /**
     * there are two cases that will be handled here -
     * 1. Stuck followers: If the leo of the replica hasn't been updated for maxLagMs ms,
     *                     the follower is stuck and should be removed from the ISR
     * 2. Slow followers: If the replica has not read up to the leo within the last maxLagMs ms,
     *                    then the follower is lagging and should be removed from the ISR
     * Both these cases are handled by checking the lastCaughtUpTimeMs which represents
     * the last time when the replica was fully caught up. If either of the above conditions
     * is violated, that replica is considered to be out of sync
     *
     **/
    val leaderLogEndOffset = leaderReplica.logEndOffset
    val candidateReplicas = inSyncReplicas - leaderReplica
    //
    val laggingReplicas = candidateReplicas.filter(r => (time.milliseconds - r.lastCaughtUpTimeMs) > maxLagMs)
    if(laggingReplicas.nonEmpty)
      debug("Lagging replicas for partition %s are %s".format(TopicAndPartition(topic, partitionId), laggingReplicas.map(_.brokerId).mkString(",")))
    laggingReplicas
  }

  def appendMessagesToLeader(messages: ByteBufferMessageSet, requiredAcks: Int = 0) = {
    val (info, leaderHWIncremented) = inReadLock(leaderIsrUpdateLock) {
      //获取leader副本
      val leaderReplicaOpt = leaderReplicaIfLocal()
      leaderReplicaOpt match {
        //如果是主副本
        case Some(leaderReplica) =>
          val log: Log = leaderReplica.log.get
          //配置的最小ISR
          val minIsr = log.config.minInSyncReplicas
          //正在同步的isr大小
          val inSyncSize = inSyncReplicas.size
          // Avoid writing to leader if there are not enough insync replicas to make it safe
          if (inSyncSize < minIsr && requiredAcks == -1) {
            throw new NotEnoughReplicasException("Number of insync replicas for partition [%s,%d] is [%d], below required minimum [%d]"
              .format(topic, partitionId, inSyncSize, minIsr))
          }
          //todo Log#append
          val info = log.append(messages, assignOffsets = true)
          // probably unblock some follower fetch requests since log end offset has been updated
          //todo 尝试完成延迟的拉取
          replicaManager.tryCompleteDelayedFetch(TopicPartitionOperationKey(this.topic, this.partitionId))
          // we may need to increment high watermark since ISR could be down to 1
          (info, maybeIncrementLeaderHW(leaderReplica))

        case None =>
          throw new NotLeaderForPartitionException("Leader not local for partition [%s,%d] on broker %d"
            .format(topic, partitionId, localBrokerId))
      }
    }

    // some delayed operations may be unblocked after HW changed
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()

    info
  }

  private def updateIsr(newIsr: Set[Replica]) {
    val newLeaderAndIsr = new LeaderAndIsr(localBrokerId, leaderEpoch, newIsr.map(r => r.brokerId).toList, zkVersion)
    //更新ISR节点信息
    val (updateSucceeded,newVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, topic, partitionId,
      newLeaderAndIsr, controllerEpoch, zkVersion)
    if(updateSucceeded) {
      replicaManager.recordIsrChange(new TopicAndPartition(topic, partitionId))
      inSyncReplicas = newIsr
      zkVersion = newVersion
      trace("ISR updated to [%s] and zkVersion updated to [%d]".format(newIsr.mkString(","), zkVersion))
    } else {
      info("Cached zkVersion [%d] not equal to that in zookeeper, skip updating ISR".format(zkVersion))
    }
  }

  /**
   * remove deleted log metrics
   */
  private def removePartitionMetrics() {
    removeMetric("UnderReplicated", tags)
  }

  override def equals(that: Any): Boolean = {
    if(!that.isInstanceOf[Partition])
      return false
    val other = that.asInstanceOf[Partition]
    if(topic.equals(other.topic) && partitionId == other.partitionId)
      return true
    false
  }

  override def hashCode(): Int = {
    31 + topic.hashCode() + 17*partitionId
  }

  override def toString: String = {
    val partitionString = new StringBuilder
    partitionString.append("Topic: " + topic)
    partitionString.append("; Partition: " + partitionId)
    partitionString.append("; Leader: " + leaderReplicaIdOpt)
    partitionString.append("; AssignedReplicas: " + assignedReplicaMap.keys.mkString(","))
    partitionString.append("; InSyncReplicas: " + inSyncReplicas.map(_.brokerId).mkString(","))
    partitionString.toString()
  }
}
