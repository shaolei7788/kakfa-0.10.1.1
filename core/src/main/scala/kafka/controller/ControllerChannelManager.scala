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
package kafka.controller

import java.net.SocketTimeoutException
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import kafka.api._
import kafka.cluster.Broker
import kafka.common.{KafkaException, TopicAndPartition}
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.clients.{ClientRequest, ClientResponse, ManualMetadataUpdater, NetworkClient}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{ChannelBuilders, LoginType, Mode, NetworkReceive, Selectable, Selector}
import org.apache.kafka.common.protocol.{ApiKeys, SecurityProtocol}
import org.apache.kafka.common.requests
import org.apache.kafka.common.requests.{UpdateMetadataRequest, _}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{Node, TopicPartition}

import scala.collection.JavaConverters._
import scala.collection.{Set, mutable}
import scala.collection.mutable.HashMap

//控制器的网络通道管理
class ControllerChannelManager(controllerContext: ControllerContext, config: KafkaConfig, time: Time, metrics: Metrics, threadNamePrefix: Option[String] = None) extends Logging {
  //todo 核心  key=brokerid  ControllerBrokerStateInfo 表示与一个broker连接的各种信息
  protected val brokerStateInfo = new HashMap[Int, ControllerBrokerStateInfo]
  private val brokerLock = new Object
  this.logIdent = "[Channel manager on controller " + config.brokerId + "]: "

  //
  controllerContext.liveBrokers.foreach(addNewBroker(_))

  def startup() = {
    brokerLock synchronized {
      //todo 添加broker信息
      brokerStateInfo.foreach(brokerState => startRequestSendThread(brokerState._1))
    }
  }

  def shutdown() = {
    brokerLock synchronized {
      brokerStateInfo.values.foreach(removeExistingBroker)
    }
  }

  def sendRequest(brokerId: Int, apiKey: ApiKeys, apiVersion: Option[Short], request: AbstractRequest, callback: AbstractRequestResponse => Unit = null) {
    brokerLock synchronized {
      val stateInfoOpt = brokerStateInfo.get(brokerId)
      stateInfoOpt match {
        case Some(stateInfo) =>
          stateInfo.messageQueue.put(QueueItem(apiKey, apiVersion, request, callback))
        case None =>
          warn("Not sending request %s to broker %d, since it is offline.".format(request, brokerId))
      }
    }
  }

  //添加broker
  def addBroker(broker: Broker) {
    // be careful here. Maybe the startup() API has already started the request send thread
    brokerLock synchronized {
      if(!brokerStateInfo.contains(broker.id)) {
        //若brokerStateInfo 不包含brokerid，则加入
        addNewBroker(broker)
        startRequestSendThread(broker.id)
      }
    }
  }

  //
  def removeBroker(brokerId: Int) {
    brokerLock synchronized {
      removeExistingBroker(brokerStateInfo(brokerId))
    }
  }

  //添加新的broker
  private def addNewBroker(broker: Broker) {
    val messageQueue = new LinkedBlockingQueue[QueueItem]
    debug("Controller %d trying to connect to broker %d".format(config.brokerId, broker.id))
    //interBrokerSecurityProtocol = security.inter.broker.protocol
    val brokerEndPoint = broker.getBrokerEndPoint(config.interBrokerSecurityProtocol)

    val brokerNode = new Node(broker.id, brokerEndPoint.host, brokerEndPoint.port)
    //todo 创建networkClient
    val networkClient = {
      val channelBuilder = ChannelBuilders.create(
        config.interBrokerSecurityProtocol,
        Mode.CLIENT,
        LoginType.SERVER,
        config.values,
        config.saslMechanismInterBrokerProtocol,
        config.saslInterBrokerHandshakeRequestEnable
      )
      val selector = new Selector(
        NetworkReceive.UNLIMITED,
        Selector.NO_IDLE_TIMEOUT_MS,
        metrics,
        time,
        "controller-channel",
        Map("broker-id" -> broker.id.toString).asJava,
        false,
        channelBuilder
      )
      new NetworkClient(
        selector,
        new ManualMetadataUpdater(Seq(brokerNode).asJava),
        config.brokerId.toString,
        1,
        0,
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        config.requestTimeoutMs,
        time
      )
    }
    val threadName = threadNamePrefix match {
      case None => "Controller-%d-to-broker-%d-send-thread".format(config.brokerId, broker.id)
      case Some(name) => "%s:Controller-%d-to-broker-%d-send-thread".format(name, config.brokerId, broker.id)
    }

    //用于发送请求的线程
    val requestThread = new RequestSendThread(config.brokerId, controllerContext, messageQueue, networkClient,
      brokerNode, config, time, threadName)
    requestThread.setDaemon(false)
    brokerStateInfo.put(broker.id, new ControllerBrokerStateInfo(networkClient, brokerNode, messageQueue, requestThread))
  }

  //移除旧的broker
  private def removeExistingBroker(brokerState: ControllerBrokerStateInfo) {
    try {
      //关闭网络连接
      brokerState.networkClient.close()
      //请求消息队列
      brokerState.messageQueue.clear()
      //停掉线程
      brokerState.requestSendThread.shutdown()
      brokerStateInfo.remove(brokerState.brokerNode.id)
    } catch {
      case e: Throwable => error("Error while removing broker by the controller", e)
    }
  }

  protected def startRequestSendThread(brokerId: Int) {
    val requestThread = brokerStateInfo(brokerId).requestSendThread
    if(requestThread.getState == Thread.State.NEW)
      requestThread.start()
  }
}

case class QueueItem(apiKey: ApiKeys, apiVersion: Option[Short], request: AbstractRequest, callback: AbstractRequestResponse => Unit)

class RequestSendThread(val controllerId: Int,
                        val controllerContext: ControllerContext,
                        val queue: BlockingQueue[QueueItem],
                        val networkClient: NetworkClient,
                        val brokerNode: Node,
                        val config: KafkaConfig,
                        val time: Time,
                        name: String)
  extends ShutdownableThread(name = name) {

  private val lock = new Object()
  private val stateChangeLogger = KafkaController.stateChangeLogger
  private val socketTimeoutMs = config.controllerSocketTimeoutMs

  override def doWork(): Unit = {

    def backoff(): Unit = CoreUtils.swallowTrace(Thread.sleep(100))

    //获取缓冲取的队列QueueItem
    val QueueItem(apiKey, apiVersion, request, callback) = queue.take()
    //完成发送请求并阻塞等待响应
    import NetworkClientBlockingOps._
    var clientResponse: ClientResponse = null
    try {
      lock synchronized {
        var isSendSuccessful = false
        while (isRunning.get() && !isSendSuccessful) {
          // if a broker goes down for a long time, then at some point the controller's zookeeper listener will trigger a
          // removeBroker which will invoke shutdown() on this thread. At that point, we will stop retrying.
          try {
            //阻塞等待符合发送条件
            if (!brokerReady()) {
              isSendSuccessful = false
              //退避3s
              backoff()
            }
            else {
              val requestHeader = apiVersion.fold(networkClient.nextRequestHeader(apiKey))(networkClient.nextRequestHeader(apiKey, _))
              val send = new RequestSend(brokerNode.idString, requestHeader, request.toStruct)
              val clientRequest = new ClientRequest(time.milliseconds(), true, send, null)
              //todo 阻塞发送请求
              clientResponse = networkClient.blockingSendAndReceive(clientRequest)(time)
              isSendSuccessful = true
            }
          } catch {
            case e: Throwable => // if the send was not successful, reconnect to broker and resend the message
              warn(("Controller %d epoch %d fails to send request %s to broker %s. " +
                "Reconnecting to broker.").format(controllerId, controllerContext.epoch,
                  request.toString, brokerNode.toString()), e)
              networkClient.close(brokerNode.idString)
              isSendSuccessful = false
              backoff()
          }
        }
        if (clientResponse != null) {

          val response = ApiKeys.forId(clientResponse.request.request.header.apiKey) match {
            case ApiKeys.LEADER_AND_ISR => new LeaderAndIsrResponse(clientResponse.responseBody)
            case ApiKeys.STOP_REPLICA => new StopReplicaResponse(clientResponse.responseBody)
            case ApiKeys.UPDATE_METADATA_KEY => new UpdateMetadataResponse(clientResponse.responseBody)
            case apiKey => throw new KafkaException(s"Unexpected apiKey received: $apiKey")
          }
          stateChangeLogger.trace("Controller %d epoch %d received response %s for a request sent to broker %s"
            .format(controllerId, controllerContext.epoch, response.toString, brokerNode.toString))

          if (callback != null) {
            callback(response)
          }
        }
      }
    } catch {
      case e: Throwable =>
        error("Controller %d fails to send a request to broker %s".format(controllerId, brokerNode.toString()), e)
        // If there is any socket error (eg, socket timeout), the connection is no longer usable and needs to be recreated.
        networkClient.close(brokerNode.idString)
    }
  }

  private def brokerReady(): Boolean = {
    import NetworkClientBlockingOps._
    try {
      if (!networkClient.isReady(brokerNode)(time)) {
        if (!networkClient.blockingReady(brokerNode, socketTimeoutMs)(time))
          throw new SocketTimeoutException(s"Failed to connect within $socketTimeoutMs ms")

        info("Controller %d connected to %s for sending state change requests".format(controllerId, brokerNode.toString()))
      }

      true
    } catch {
      case e: Throwable =>
        warn("Controller %d's connection to broker %s was unsuccessful".format(controllerId, brokerNode.toString()), e)
        networkClient.close(brokerNode.idString)
        false
    }
  }

}

//todo 向broker批量发送请求  控制器发送给broker的请求有三种
// 1 leaderAndIsr
// 2 stopReplica
// 3 updateMetadata
class ControllerBrokerRequestBatch(controller: KafkaController) extends  Logging {
  val controllerContext = controller.controllerContext
  val controllerId: Int = controller.config.brokerId
  //记录了发往指定brokerd  LeaderIsrAndControllerEpoch 请求  key = brokerid  即往一个broker节点会发送多个分区状态信息
  val leaderAndIsrRequestMap = mutable.Map.empty[Int, mutable.Map[TopicPartition, PartitionStateInfo]]
  //记录了发往指定brokerd  ApiKeys.STOP_REPLICA 请求  key = brokerid  即往一个broker节点会发送多个停止副本请求
  val stopReplicaRequestMap = mutable.Map.empty[Int, Seq[StopReplicaRequestInfo]]
  //记录了发往指定brokerd  ApiKeys.UPDATE_METADATA_KEY 请求  key = brokerid  即往一个broker节点会发送多个更新元数据请求
  val updateMetadataRequestMap = mutable.Map.empty[Int, mutable.Map[TopicPartition, PartitionStateInfo]]
  private val stateChangeLogger = KafkaController.stateChangeLogger

  //创建新的批请求，必须确保前一批的请求全部发送完毕
  def newBatch() {
    // raise error if the previous batch is not empty
    if (leaderAndIsrRequestMap.nonEmpty)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating " +
        "a new one. Some LeaderAndIsr state changes %s might be lost ".format(leaderAndIsrRequestMap.toString()))
    if (stopReplicaRequestMap.nonEmpty)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
        "new one. Some StopReplica state changes %s might be lost ".format(stopReplicaRequestMap.toString()))
    if (updateMetadataRequestMap.nonEmpty)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
        "new one. Some UpdateMetadata state changes %s might be lost ".format(updateMetadataRequestMap.toString()))
  }

  def clear() {
    leaderAndIsrRequestMap.clear()
    stopReplicaRequestMap.clear()
    updateMetadataRequestMap.clear()
  }

  //为目标broker添加LeaderAndIsrRequest请求
  def addLeaderAndIsrRequestForBrokers(brokerIds: Seq[Int], topic: String, partition: Int,
                                       leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch,
                                       replicas: Seq[Int], callback: AbstractRequestResponse => Unit = null) {
    val topicPartition = new TopicPartition(topic, partition)

    brokerIds.filter(_ >= 0).foreach { brokerId =>
      //getOrElseUpdate 有就获取 没有就创建  Map[TopicPartition, PartitionStateInfo]
      val result = leaderAndIsrRequestMap.getOrElseUpdate(brokerId, mutable.Map.empty)
      //添加leaderAndIsrRequest 请求
      result.put(topicPartition, PartitionStateInfo(leaderIsrAndControllerEpoch, replicas.toSet))
    }
    //更新了分区的信息，也需要更新主题的元数据，主题的元数据信息包含每个分区的信息，添加UpdateMetadataRequest给broker
    addUpdateMetadataRequestForBrokers(controllerContext.liveOrShuttingDownBrokerIds.toSeq,
                                       Set(TopicAndPartition(topic, partition)))
  }

  //为目标broker添加StopReplica请求
  def addStopReplicaRequestForBrokers(brokerIds: Seq[Int], topic: String, partition: Int, deletePartition: Boolean,
                                      callback: (AbstractRequestResponse, Int) => Unit = null) {
    brokerIds.filter(b => b >= 0).foreach { brokerId =>
      stopReplicaRequestMap.getOrElseUpdate(brokerId, Seq.empty[StopReplicaRequestInfo])
      val v = stopReplicaRequestMap(brokerId)
      if(callback != null)
        stopReplicaRequestMap(brokerId) = v :+ StopReplicaRequestInfo(PartitionAndReplica(topic, partition, brokerId),
          deletePartition, (r: AbstractRequestResponse) => callback(r, brokerId))
      else
        stopReplicaRequestMap(brokerId) = v :+ StopReplicaRequestInfo(PartitionAndReplica(topic, partition, brokerId),
          deletePartition)
    }
  }

  /** Send UpdateMetadataRequest to the given brokers for the given partitions and partitions that are being deleted */
  //为目标broker添加UpdateMetadata请求
  def addUpdateMetadataRequestForBrokers(brokerIds: Seq[Int],
                                         partitions: collection.Set[TopicAndPartition] = Set.empty[TopicAndPartition],
                                         callback: AbstractRequestResponse => Unit = null) {
    def updateMetadataRequestMapFor(partition: TopicAndPartition, beingDeleted: Boolean) {
      //找出controller中保存该分区的leader
      val leaderIsrAndControllerEpochOpt = controllerContext.partitionLeadershipInfo.get(partition)
      leaderIsrAndControllerEpochOpt match {
        case Some(leaderIsrAndControllerEpoch) =>
          //获取分区的AR集合
          val replicas = controllerContext.partitionReplicaAssignment(partition).toSet
          val partitionStateInfo = if (beingDeleted) {
            val leaderAndIsr = new LeaderAndIsr(LeaderAndIsr.LeaderDuringDelete, leaderIsrAndControllerEpoch.leaderAndIsr.isr)
            PartitionStateInfo(LeaderIsrAndControllerEpoch(leaderAndIsr, leaderIsrAndControllerEpoch.controllerEpoch), replicas)
          } else {
            PartitionStateInfo(leaderIsrAndControllerEpoch, replicas)
          }
          brokerIds.filter(b => b >= 0).foreach { brokerId =>
            updateMetadataRequestMap.getOrElseUpdate(brokerId, mutable.Map.empty[TopicPartition, PartitionStateInfo])
            updateMetadataRequestMap(brokerId).put(new TopicPartition(partition.topic, partition.partition), partitionStateInfo)
          }
        case None =>
          info("Leader not yet assigned for partition %s. Skip sending UpdateMetadataRequest.".format(partition))
      }
    }

    //过滤正在删除的分区
    val filteredPartitions = {
      //判断分区是否为空
      val givenPartitions = if (partitions.isEmpty) {
        controllerContext.partitionLeadershipInfo.keySet
      } else {
        partitions
      }
      //判断对应的分区是否正在等待删除
      if (controller.deleteTopicManager.partitionsToBeDeleted.isEmpty) {
        givenPartitions
      } else {
        givenPartitions -- controller.deleteTopicManager.partitionsToBeDeleted
      }
    }
    if (filteredPartitions.isEmpty) {
      // 分区为空
      brokerIds.filter(b => b >= 0).foreach { brokerId =>
        //获取或者更改updateMetadataRequestMap的value值
        updateMetadataRequestMap.getOrElseUpdate(brokerId, mutable.Map.empty[TopicPartition, PartitionStateInfo])
      }
    } else {
      //分区不为空 调用updateMetadataRequestMapFor
      filteredPartitions.foreach(partition => updateMetadataRequestMapFor(partition, beingDeleted = false))
    }

    controller.deleteTopicManager.partitionsToBeDeleted.foreach(partition => updateMetadataRequestMapFor(partition, beingDeleted = true))
  }

  //todo 核心 发送请求给brokers
  def sendRequestsToBrokers(controllerEpoch: Int) {
    try {
      //todo leaderAndIsr请求集合
      leaderAndIsrRequestMap.foreach { case (broker, partitionStateInfos) =>
        //todo partitionStateInfos 是一个Map类型 [TopicPartition, PartitionStateInfo]
        partitionStateInfos.foreach { case (topicPartition, state) =>
          //判断请求类型 如果broker == leader 则是leader 否则是 follower
          val typeOfRequest = if (broker == state.leaderIsrAndControllerEpoch.leaderAndIsr.leader) "become-leader" else "become-follower"
          stateChangeLogger.trace(("Controller %d epoch %d sending %s LeaderAndIsr request %s to broker %d " +
                                   "for partition [%s,%d]").format(controllerId, controllerEpoch, typeOfRequest,
                                                                   state.leaderIsrAndControllerEpoch, broker,
                                                                   topicPartition.topic, topicPartition.partition))
        }

        val leaderIds = partitionStateInfos.map(_._2.leaderIsrAndControllerEpoch.leaderAndIsr.leader).toSet
        val leaders = controllerContext.liveOrShuttingDownBrokers.filter(b => leaderIds.contains(b.id)).map {
          _.getNode(controller.config.interBrokerSecurityProtocol)
        }
        val partitionStates = partitionStateInfos.map { case (topicPartition, partitionStateInfo) =>
          // LeaderIsrAndControllerEpoch 是样例类
          val LeaderIsrAndControllerEpoch(leaderIsr, controllerEpoch) = partitionStateInfo.leaderIsrAndControllerEpoch
          val partitionState : requests.PartitionState = new requests.PartitionState(controllerEpoch, leaderIsr.leader,
            leaderIsr.leaderEpoch, leaderIsr.isr.map(Integer.valueOf).asJava, leaderIsr.zkVersion,
            partitionStateInfo.allReplicas.map(Integer.valueOf).asJava
          )
          //这是一个map
          topicPartition -> partitionState
        }
        val leaderAndIsrRequest = new LeaderAndIsrRequest(controllerId, controllerEpoch, partitionStates.asJava, leaders.asJava)
        //todo 发送 ApiKeys.LEADER_AND_ISR 请求
        controller.sendRequest(broker, ApiKeys.LEADER_AND_ISR, None, leaderAndIsrRequest, null)
      }
      leaderAndIsrRequestMap.clear()

      //todo updateMetadata请求集合
      updateMetadataRequestMap.foreach { case (broker, partitionStateInfos) =>

        partitionStateInfos.foreach(p => stateChangeLogger.trace(("Controller %d epoch %d sending UpdateMetadata request %s " +
          "to broker %d for partition %s").format(controllerId, controllerEpoch, p._2.leaderIsrAndControllerEpoch,
          broker, p._1)))
        val partitionStates = partitionStateInfos.map { case (topicPartition, partitionStateInfo) =>
          val LeaderIsrAndControllerEpoch(leaderIsr, controllerEpoch) = partitionStateInfo.leaderIsrAndControllerEpoch
          val partitionState = new requests.PartitionState(controllerEpoch, leaderIsr.leader,
            leaderIsr.leaderEpoch, leaderIsr.isr.map(Integer.valueOf).asJava, leaderIsr.zkVersion,
            partitionStateInfo.allReplicas.map(Integer.valueOf).asJava
          )
          topicPartition -> partitionState
        }
        val version = if (controller.config.interBrokerProtocolVersion >= KAFKA_0_10_0_IV1) 2: Short
                      else if (controller.config.interBrokerProtocolVersion >= KAFKA_0_9_0) 1: Short
                      else 0: Short

        val updateMetadataRequest: UpdateMetadataRequest =
          if (version == 0) {
            val liveBrokers = controllerContext.liveOrShuttingDownBrokers.map(_.getNode(SecurityProtocol.PLAINTEXT))
            new UpdateMetadataRequest(controllerId, controllerEpoch, liveBrokers.asJava, partitionStates.asJava)
          } else {
            val liveBrokers = controllerContext.liveOrShuttingDownBrokers.map { broker =>
              val endPoints = broker.endPoints.map { case (securityProtocol, endPoint) =>
                securityProtocol -> new UpdateMetadataRequest.EndPoint(endPoint.host, endPoint.port)
              }
              new UpdateMetadataRequest.Broker(broker.id, endPoints.asJava, broker.rack.orNull)
            }
            new UpdateMetadataRequest(version, controllerId, controllerEpoch, partitionStates.asJava, liveBrokers.asJava)
          }
        //todo 发送 ApiKeys.UPDATE_METADATA_KEY 请求
        controller.sendRequest(broker, ApiKeys.UPDATE_METADATA_KEY, Some(version), updateMetadataRequest, null)
      }
      updateMetadataRequestMap.clear()

      //todo stopReplica请求集合
      stopReplicaRequestMap.foreach { case (broker, replicaInfoList) =>
        val stopReplicaWithDelete = replicaInfoList.filter(_.deletePartition).map(_.replica).toSet
        val stopReplicaWithoutDelete = replicaInfoList.filterNot(_.deletePartition).map(_.replica).toSet
        debug("The stop replica request (delete = true) sent to broker %d is %s"
          .format(broker, stopReplicaWithDelete.mkString(",")))
        debug("The stop replica request (delete = false) sent to broker %d is %s"
          .format(broker, stopReplicaWithoutDelete.mkString(",")))
        replicaInfoList.foreach { r =>
          val stopReplicaRequest = new StopReplicaRequest(controllerId, controllerEpoch, r.deletePartition,
            Set(new TopicPartition(r.replica.topic, r.replica.partition)).asJava)
          //todo 发送 ApiKeys.STOP_REPLICA 请求
          controller.sendRequest(broker, ApiKeys.STOP_REPLICA, None, stopReplicaRequest, r.callback)
        }
      }
      stopReplicaRequestMap.clear()
    } catch {
      case e : Throwable => {
        if (leaderAndIsrRequestMap.nonEmpty) {
          error("Haven't been able to send leader and isr requests, current state of " +
              s"the map is $leaderAndIsrRequestMap. Exception message: $e")
        }
        if (updateMetadataRequestMap.nonEmpty) {
          error("Haven't been able to send metadata update requests, current state of " +
              s"the map is $updateMetadataRequestMap. Exception message: $e")
        }
        if (stopReplicaRequestMap.nonEmpty) {
          error("Haven't been able to send stop replica requests, current state of " +
              s"the map is $stopReplicaRequestMap. Exception message: $e")
        }
        throw new IllegalStateException(e)
      }
    }
  }
}

case class ControllerBrokerStateInfo(networkClient: NetworkClient,
                                     //节点信息 host ip port
                                     brokerNode: Node,
                                     //缓冲队列，存放了发往对应broker的请求， QueueItem 封装了request本身和回调函数
                                     messageQueue: BlockingQueue[QueueItem],
                                     //用于发送请求的线程
                                     requestSendThread: RequestSendThread)

case class StopReplicaRequestInfo(replica: PartitionAndReplica, deletePartition: Boolean, callback: AbstractRequestResponse => Unit = null)

class Callbacks private (var leaderAndIsrResponseCallback: AbstractRequestResponse => Unit = null,
                         var updateMetadataResponseCallback: AbstractRequestResponse => Unit = null,
                         var stopReplicaResponseCallback: (AbstractRequestResponse, Int) => Unit = null)

object Callbacks {
  class CallbackBuilder {
    var leaderAndIsrResponseCbk: AbstractRequestResponse => Unit = null
    var updateMetadataResponseCbk: AbstractRequestResponse => Unit = null
    var stopReplicaResponseCbk: (AbstractRequestResponse, Int) => Unit = null

    def leaderAndIsrCallback(cbk: AbstractRequestResponse => Unit): CallbackBuilder = {
      leaderAndIsrResponseCbk = cbk
      this
    }

    def updateMetadataCallback(cbk: AbstractRequestResponse => Unit): CallbackBuilder = {
      updateMetadataResponseCbk = cbk
      this
    }

    def stopReplicaCallback(cbk: (AbstractRequestResponse, Int) => Unit): CallbackBuilder = {
      stopReplicaResponseCbk = cbk
      this
    }

    def build: Callbacks = {
      new Callbacks(leaderAndIsrResponseCbk, updateMetadataResponseCbk, stopReplicaResponseCbk)
    }
  }
}
