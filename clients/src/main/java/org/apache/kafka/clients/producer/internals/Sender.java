/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.*;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidMetadataException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestSend;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * The background thread that handles the sending of produce requests to the Kafka cluster. This thread makes metadata
 * requests to renew its view of the cluster and then sends produce requests to the appropriate nodes.
 */
public class Sender implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Sender.class);

    /* the state of each nodes connection */
    private final KafkaClient client;

    /* the record accumulator that batches records */
    //对记录进行批处理的记录累加器
    private final RecordAccumulator accumulator;

    /* the metadata for the client */
    private final Metadata metadata;

    /* the flag indicating whether the producer should guarantee the message order on the broker or not. */
    private final boolean guaranteeMessageOrder;

    /* the maximum request size to attempt to send to the server */
    private final int maxRequestSize;

    /* the number of acknowledgements to request from the server */
    private final short acks;

    /* the number of times to retry a failed request before giving up */
    private final int retries;

    /* the clock instance used for getting the time */
    private final Time time;

    /* true while the sender thread is still running */
    private volatile boolean running;

    /* true when the caller wants to ignore all unsent/inflight messages and force close.  */
    private volatile boolean forceClose;

    /* metrics */
    private final SenderMetrics sensors;

    /* param clientId of the client */
    private String clientId;

    /* the max time to wait for the server to respond to the request*/
    private final int requestTimeout;

    public Sender(KafkaClient client,
                  Metadata metadata,
                  RecordAccumulator accumulator,
                  //默认为false
                  boolean guaranteeMessageOrder,
                  int maxRequestSize,
                  short acks,
                  int retries,
                  Metrics metrics,
                  Time time,
                  String clientId,
                  int requestTimeout) {
        this.client = client;
        this.accumulator = accumulator;
        this.metadata = metadata;
        this.guaranteeMessageOrder = guaranteeMessageOrder;
        this.maxRequestSize = maxRequestSize;
        this.running = true;
        this.acks = acks;
        this.retries = retries;
        this.time = time;
        this.clientId = clientId;
        this.sensors = new SenderMetrics(metrics);
        this.requestTimeout = requestTimeout;
    }

    /**
     * The main run loop for the sender thread
     */
    public void run() {
        log.debug("Starting Kafka producer I/O thread.");
        // main loop, runs until close is called
        //running = true,这是一个死循环,
        //所以我们要知道sender线程启动之后会一直运行
        while (running) {
            try {
                //TODO 核心代码
                run(time.milliseconds());
            } catch (Exception e) {
                log.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        }

        log.debug("Beginning shutdown of Kafka producer I/O thread, sending remaining records.");

        // okay we stopped accepting requests but there may still be
        // requests in the accumulator or waiting for acknowledgment,
        // wait until these are completed.
        while (!forceClose && (this.accumulator.hasUnsent() || this.client.inFlightRequestCount() > 0)) {
            try {
                run(time.milliseconds());
            } catch (Exception e) {
                log.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        }
        if (forceClose) {
            // We need to fail all the incomplete batches and wake up the threads waiting on
            // the futures.
            this.accumulator.abortIncompleteBatches();
        }
        try {
            this.client.close();
        } catch (Exception e) {
            log.error("Failed to close network client", e);
        }

        log.debug("Shutdown of Kafka producer I/O thread has completed.");
    }

    /**
     * Run a single iteration of sending
     * 
     * @param now
     *            The current POSIX time in milliseconds
     */
    void run(long now) {
        /**
         * (1).代码第一次运行到这里
         *      第一次进来的时候cluster里面是没有元数据的
         *      接下来的代码都会依赖元数据,所以后面的代码都不会执行
         *      指导最后一行代码this.client.poll(pollTimeout, now);才会执行
         * (2).代码第二次运行到这里
         *      第二次进来的时候,已经有元数据了
         *  步骤一: 获取元数据
         */
        //当代码第一次运行到这里的时候,其实是没有元数据的,只是返回了cluster对象
        //cluster对象里面没有元数据,接下来的代码都不会执行
        Cluster cluster = metadata.fetch();
        /**
         * 步骤二: 首先判断哪些batches<TopicPartion,Dqueue<RecordBatch>>可以发送,
         *          (换句话说,我们需要知道一个批次可以发送出去的条件)
         *      获取到这个partition的leader partition对应的broker主机(根据元数据来就行了)
         *      kafka发送消息的时候,会把消息发送到leader partition上面
         *      哪些broker上面需要我们去发送消息
         */
        // get the list of partitions with data ready to send
        // 获取准备好需要发送数据的分区列表
        RecordAccumulator.ReadyCheckResult result = this.accumulator.ready(cluster, now);
        /**
         * 步骤三: 标识有哪些topic的元数据没有被拉取到,可能是元数据拉取有问题或者元数据又发生了改变,那么强制更新元数据
         *      这其实是一个异常处理,正常情况下是不会发生的
         */
        // if there are any partitions whose leaders are not known yet, force metadata update
        // 如果没有leader partition,强制元数据更新
        // 某一时刻 topic 有 0，1，2三个分区，突然topic-2 都挂了, 就会导致topic-2 没有leader，就需要更新元数据
        if (!result.unknownLeaderTopics.isEmpty()) {
            // The set of topics with unknown leader contains topics with leader election pending as well as
            // topics which may have expired. Add the topic again to metadata to ensure it is included
            // and request metadata update, since there are messages to send to the topic.
            for (String topic : result.unknownLeaderTopics) {
                this.metadata.add(topic);
            }
            this.metadata.requestUpdate();
        }

        // remove any nodes we aren't ready to send to
        //如果主机的网络连接没有建立好,就删除readyNodes对应的节点
        Iterator<Node> iter = result.readyNodes.iterator();
        long notReadyTimeout = Long.MAX_VALUE;
        //todo 遍历每一台将要接收消息的主机
        while (iter.hasNext()) {
            Node node = iter.next();
            /**
             * 步骤四: 检查与要发送数据的主机的网络是否已经建立好
             */
            // NetworkClient#ready 从记录收集器获取准备完毕的节点，并连接所有准备好的节点
            if (!this.client.ready(node, now)) {
                //如果返回的是false,那么!false就进来了,说明网络连接没有建立好
                //移除result里面要接收消息的节点
                //所以我们会看到这里所有的主机都会被移除
                iter.remove();
                notReadyTimeout = Math.min(notReadyTimeout, this.client.connectionDelay(node, now));
            }
        }

        /**
         * 步骤五: 将数据整理成一个批处理列表,这些批次将在每个节点的基础上指定适合的大小,然后发送到指定的节点上
         *      我们要发送的分区数据可能有很多个,
         *      很有可能有一些分区的leader partition 在同一台服务器上
         *      假设我们有三台服务器,当我们的分区个数大于集群节点的个数时,一定会有多个leader partition 在同一台服务器上面,
         *      所以按照broker进行分组,同一个broker的分区在同一组
         *      p0:leader --> 0
         *      p1:leader --> 0
         *      p2:leader --> 1
         *      p3:leader --> 2
         *      ----->
         *      0:{p0,p1}
         *      1:{p2}
         *      2:{p3}
         *      这样做的原因: 正常情况下,发送一个批次就会有一个请求,那么p0,p1,p2,p3就有四个请求
         *                  如果按照broker分组,那么有几台服务器就会有几个请求
         *      优点: 1.减少了网络传输的请求次数 2.增加吞吐量
         *      kafka从细节上考虑性能优化
         */
        // create produce requests
        // 创建一个生产请求,但是,网络没有建立的话,这里的代码是不会执行的
        // 读取记录收集器，返回每个主副本节点对应的批次记录列表
        //
        Map<Integer, List<RecordBatch>> batches = this.accumulator.drain(cluster,//元数据
                                                                         result.readyNodes,//接收消息的节点
                                                                         this.maxRequestSize,//批次的大小
                                                                         now);//当前时间戳

        if (guaranteeMessageOrder) {//是否保证消息有序
            // Mute all the partitions drained    mute 沉默的  drained 排空
            // 将已经发送数据的分区解除保护
            // 如果batches是空,这里的代码也不会执行
            for (List<RecordBatch> batchList : batches.values()) {
                for (RecordBatch batch : batchList) {
                    this.accumulator.mutePartition(batch.topicPartition);
                }
            }
        }
        /**
         *  步骤六: 处理超时的批处理  requestTimeout 30s  30s没发送出去就是超时了
         */
        List<RecordBatch> expiredBatches = this.accumulator.abortExpiredBatches(this.requestTimeout, now);
        // update sensors
        // 更新发送消息的标记
        for (RecordBatch expiredBatch : expiredBatches)
            this.sensors.recordErrors(expiredBatch.topicPartition.topic(), expiredBatch.recordCount);
        //更新生产者消息发送请求指标
        sensors.updateProduceRequestMetrics(batches);
        /**
         * 步骤七: 创建生产者请求连接,将消息批次存储到请求列表中
         * 发送消息的时候,有一些分区里的消息会发送到同一台服务器上面,如果是以分区为单位依次发送,那么网络请求会有些频繁
         * 集群中的网络资源是十分珍贵的,所以发往同一个服务器的分区数据,会共用同一个请求,这样就减少了网络请求
         */
        //todo 以节点为级别的生产请求列表，即每个节点只有一个客户端请求
        List<ClientRequest> requests = createProduceRequests(batches, now);
        // If we have any nodes that are ready to send + have sendable data, poll with 0 timeout so this can immediately
        // loop and try sending more data. Otherwise, the timeout is determined by nodes that have partitions with data
        // that isn't yet sendable (e.g. lingering, backing off). Note that this specifically does not include nodes
        // with sendable data that aren't ready to send since they would cause busy looping.
        long pollTimeout = Math.min(result.nextReadyCheckDelayMs, notReadyTimeout);
        if (result.readyNodes.size() > 0) {
            log.trace("Nodes with data ready to send: {}", result.readyNodes);
            log.trace("Created {} produce requests: {}", requests.size(), requests);
            pollTimeout = 0;
        }
        //TODO 将客户端请求对象放到kafka通道里
        for (ClientRequest request : requests){
            /**
             * 接下来要发送请求了,要把数据发送到服务端
             * 那么要做的事情,就是在Selector上面绑定一个写数据的事件
             */
            //绑定了OP_WRITE事件
            client.send(request, now);
        }
        // if some partitions are already ready to be sent, the select time would be 0;
        // otherwise if some partition already has some data accumulated but not ready yet,
        // the select time will be the time difference between now and its linger expiry time;
        // otherwise the select time will be the time difference between now and the metadata expiry time;
        //TODO 重点就是去看这个方法
        //就是用的这个方法拉取的元数据
        /**
         * 步骤八: 真正执行网络读写请求的就是这个NetWorkClient组件，
         *          包括: 发送请求,接收响应(处理响应) 拉取元数据信息,靠的就是这段代码
         */
        //client对象是KafkaClient类型,它是一个接口对象,它的实现类就是NetworkClient NetworkClient#poll
        //NetworkClient实现类中的poll方法:对socket执行实际的读写操作。
        //当代码第一次进来的时候没有元数据,就是这段代码来获取元数据的
        //当没有建立连接的时候,就在这里进行网络连接
        this.client.poll(pollTimeout, now);
    }

    /**
     * Start closing the sender (won't actually complete until all data is sent out)
     */
    public void initiateClose() {
        // Ensure accumulator is closed first to guarantee that no more appends are accepted after
        // breaking from the sender loop. Otherwise, we may miss some callbacks when shutting down.
        this.accumulator.close();
        this.running = false;
        this.wakeup();
    }

    /**
     * Closes the sender without sending out any pending messages.
     */
    public void forceClose() {
        this.forceClose = true;
        initiateClose();
    }

    /**
     * Handle a produce response
     */
    private void handleProduceResponse(ClientResponse response, Map<TopicPartition, RecordBatch> batches, long now) {
        int correlationId = response.request().request().header().correlationId();
        //broker失去连接,这是小概率时间,一般不会走这里
        if (response.wasDisconnected()) {
            log.trace("Cancelled request {} due to node {} being disconnected", response, response.request()
                                                                                                  .request()
                                                                                                  .destination());
            for (RecordBatch batch : batches.values()) {
                completeBatch(batch, Errors.NETWORK_EXCEPTION, -1L, Record.NO_TIMESTAMP, correlationId, now);
            }
        } else {
            //正常情况下,是走这里
            log.trace("Received produce response from node {} with correlation id {}",
                      response.request().request().destination(),
                      correlationId);
            // if we have a response, parse it
            if (response.hasResponse()) {
                ProduceResponse produceResponse = new ProduceResponse(response.responseBody());
                /**
                 * 遍历每个分区的响应
                 */
                for (Map.Entry<TopicPartition, ProduceResponse.PartitionResponse> entry : produceResponse.responses().entrySet()) {
                    TopicPartition tp = entry.getKey();
                    ProduceResponse.PartitionResponse partResp = entry.getValue();
                    //如果处理成功就是成功了,如果服务端处理失败了,会给我吗返回失败的信息
                    //error中存储的就是服务端发过来的异常码
                    Errors error = Errors.forCode(partResp.errorCode);
                    //获取当前分区的批记录
                    RecordBatch batch = batches.get(tp);
                    //对响应进行处理  partResp.baseOffset = 基准偏移量
                    completeBatch(batch, error, partResp.baseOffset, partResp.timestamp, correlationId, now);
                }
                this.sensors.recordLatency(response.request().request().destination(), response.requestLatencyMs());
                this.sensors.recordThrottleTime(response.request().request().destination(), produceResponse.getThrottleTime());
            } else {
                // this is the acks = 0 case, just complete all requests
                for (RecordBatch batch : batches.values()) {
                    completeBatch(batch, Errors.NONE, -1L, Record.NO_TIMESTAMP, correlationId, now);
                }
            }
        }
    }

    /**
     * Complete or retry the given batch of records.
     * 
     * @param batch The record batch
     * @param error The error (or null if none)
     * @param baseOffset The base offset assigned to the records if successful
     * @param timestamp The timestamp returned by the broker for this batch
     * @param correlationId The correlation id for the request
     * @param now The current POSIX time stamp in milliseconds
     *
     *
     */
    private void completeBatch(RecordBatch batch, Errors error, long baseOffset, long timestamp, long correlationId, long now) {
        //如果响应带有异常码,并且这个请求是可以重试的
        if (error != Errors.NONE && canRetry(batch, error)) {
            // retry
            log.warn("Got error produce response with correlation id {} on topic-partition {}, retrying ({} attempts left). Error: {}",
                     correlationId,
                     batch.topicPartition,
                     this.retries - batch.attempts - 1,
                     error);
            //把发送失败的批次重新放入队列里
            this.accumulator.reenqueue(batch, now);
            this.sensors.recordRetries(batch.topicPartition.topic(), batch.recordCount);
        } else {
            //TODO 来到这里的数据,(1) 带有异常,但是不能重试(要么是压根不让重试,要么就是重试次数超了); (2) 没有异常
            RuntimeException exception;
            //如果响应里面带有没有权限的异常,
            if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                //自己封装一个异常信息(自定义异常)
                exception = new TopicAuthorizationException(batch.topicPartition.topic());
            } else {
                exception = error.exception();
            }
            // tell the user the result of their request
            //TODO 核心代码,把异常的信息也带过去了
            //这个方法里面调用了用户传进来的回调函数
            //回调函数调用完了之后,说明一个完整的消息发送流程就结束了
            //生产者处理响应
            batch.done(baseOffset, timestamp, exception);
            //回收资源
            this.accumulator.deallocate(batch);
            if (error != Errors.NONE)
                this.sensors.recordErrors(batch.topicPartition.topic(), batch.recordCount);
        }
        if (error.exception() instanceof InvalidMetadataException) {
            if (error.exception() instanceof UnknownTopicOrPartitionException)
                log.warn("Received unknown topic or partition error in produce request on partition {}. The " +
                        "topic/partition may not exist or the user may not have Describe access to it", batch.topicPartition);
            metadata.requestUpdate();
        }

        // Unmute the completed partition.
        if (guaranteeMessageOrder)
            this.accumulator.unmutePartition(batch.topicPartition);
    }

    /**
     * We can retry a send if the error is transient and the number of attempts taken is fewer than the maximum allowed
     */
    private boolean canRetry(RecordBatch batch, Errors error) {
        return batch.attempts < this.retries && error.exception() instanceof RetriableException;
    }

    /**
     * Transfer the record batches into a list of produce requests on a per-node basis
     * 转移消息批次到对应的节点的请求列表中
     */
    //已经按照节点分组了
    private List<ClientRequest> createProduceRequests(Map<Integer, List<RecordBatch>> collated, long now) {
        List<ClientRequest> requests = new ArrayList<ClientRequest>(collated.size());
        for (Map.Entry<Integer, List<RecordBatch>> entry : collated.entrySet()) {
            //entry.getKey() 节点id
            requests.add(produceRequest(now, entry.getKey(), acks, requestTimeout, entry.getValue()));
        }
        return requests;
    }

    /**
     * Create a produce request from the given record batches
     * 根据给定的消息批次创建一个请求   destination 发送的目标节点
     */
    private ClientRequest produceRequest(long now, int destination, short acks, int timeout, List<RecordBatch> batches) {
        Map<TopicPartition, ByteBuffer> produceRecordsByPartition = new HashMap<TopicPartition, ByteBuffer>(batches.size());
        final Map<TopicPartition, RecordBatch> recordsByPartition = new HashMap<TopicPartition, RecordBatch>(batches.size());
        for (RecordBatch batch : batches) {
            //每个RecordBatch 都有唯一的TopicPartition
            TopicPartition tp = batch.topicPartition;
            //todo  batch.records  MemoryRecords类型  batch.records.buffer = ByteBuffer  java.nio包下
            produceRecordsByPartition.put(tp, batch.records.buffer());
            recordsByPartition.put(tp, batch);
        }
        ProduceRequest request = new ProduceRequest(acks, timeout, produceRecordsByPartition);
        //todo 创建发送请求 RequestSend extends NetworkSend extends ByteBufferSend
        RequestSend send = new RequestSend(Integer.toString(destination),
                                           this.client.nextRequestHeader(ApiKeys.PRODUCE),
                                           request.toStruct());
        RequestCompletionHandler callback = new RequestCompletionHandler() {
            //回调函数要是被调用了,其实就是这个方法被执行了
            public void onComplete(ClientResponse response) {
                //处理响应
                //回调函数会处理2种异常,不可重试异常和可重试异常
                handleProduceResponse(response, recordsByPartition, time.milliseconds());
            }
        };
        //todo 回调函数会作为客户端请求的一个成员变量，当客户端请求完成，会调用回调函数
        return new ClientRequest(now, acks != 0, send, callback);
    }

    /**
     * Wake up the selector associated with this send thread
     */
    public void wakeup() {
        this.client.wakeup();
    }

    /**
     * A collection of sensors for the sender
     */
    private class SenderMetrics {

        private final Metrics metrics;
        public final Sensor retrySensor;
        public final Sensor errorSensor;
        public final Sensor queueTimeSensor;
        public final Sensor requestTimeSensor;
        public final Sensor recordsPerRequestSensor;
        public final Sensor batchSizeSensor;
        public final Sensor compressionRateSensor;
        public final Sensor maxRecordSizeSensor;
        public final Sensor produceThrottleTimeSensor;

        public SenderMetrics(Metrics metrics) {
            this.metrics = metrics;
            String metricGrpName = "producer-metrics";

            this.batchSizeSensor = metrics.sensor("batch-size");
            MetricName m = metrics.metricName("batch-size-avg", metricGrpName, "The average number of bytes sent per partition per-request.");
            this.batchSizeSensor.add(m, new Avg());
            m = metrics.metricName("batch-size-max", metricGrpName, "The max number of bytes sent per partition per-request.");
            this.batchSizeSensor.add(m, new Max());

            this.compressionRateSensor = metrics.sensor("compression-rate");
            m = metrics.metricName("compression-rate-avg", metricGrpName, "The average compression rate of record batches.");
            this.compressionRateSensor.add(m, new Avg());

            this.queueTimeSensor = metrics.sensor("queue-time");
            m = metrics.metricName("record-queue-time-avg", metricGrpName, "The average time in ms record batches spent in the record accumulator.");
            this.queueTimeSensor.add(m, new Avg());
            m = metrics.metricName("record-queue-time-max", metricGrpName, "The maximum time in ms record batches spent in the record accumulator.");
            this.queueTimeSensor.add(m, new Max());

            this.requestTimeSensor = metrics.sensor("request-time");
            m = metrics.metricName("request-latency-avg", metricGrpName, "The average request latency in ms");
            this.requestTimeSensor.add(m, new Avg());
            m = metrics.metricName("request-latency-max", metricGrpName, "The maximum request latency in ms");
            this.requestTimeSensor.add(m, new Max());

            this.produceThrottleTimeSensor = metrics.sensor("produce-throttle-time");
            m = metrics.metricName("produce-throttle-time-avg", metricGrpName, "The average throttle time in ms");
            this.produceThrottleTimeSensor.add(m, new Avg());
            m = metrics.metricName("produce-throttle-time-max", metricGrpName, "The maximum throttle time in ms");
            this.produceThrottleTimeSensor.add(m, new Max());

            this.recordsPerRequestSensor = metrics.sensor("records-per-request");
            m = metrics.metricName("record-send-rate", metricGrpName, "The average number of records sent per second.");
            this.recordsPerRequestSensor.add(m, new Rate());
            m = metrics.metricName("records-per-request-avg", metricGrpName, "The average number of records per request.");
            this.recordsPerRequestSensor.add(m, new Avg());

            this.retrySensor = metrics.sensor("record-retries");
            m = metrics.metricName("record-retry-rate", metricGrpName, "The average per-second number of retried record sends");
            this.retrySensor.add(m, new Rate());

            this.errorSensor = metrics.sensor("errors");
            m = metrics.metricName("record-error-rate", metricGrpName, "The average per-second number of record sends that resulted in errors");
            this.errorSensor.add(m, new Rate());

            this.maxRecordSizeSensor = metrics.sensor("record-size-max");
            m = metrics.metricName("record-size-max", metricGrpName, "The maximum record size");
            this.maxRecordSizeSensor.add(m, new Max());
            m = metrics.metricName("record-size-avg", metricGrpName, "The average record size");
            this.maxRecordSizeSensor.add(m, new Avg());

            m = metrics.metricName("requests-in-flight", metricGrpName, "The current number of in-flight requests awaiting a response.");
            this.metrics.addMetric(m, new Measurable() {
                public double measure(MetricConfig config, long now) {
                    return client.inFlightRequestCount();
                }
            });
            m = metrics.metricName("metadata-age", metricGrpName, "The age in seconds of the current producer metadata being used.");
            metrics.addMetric(m, new Measurable() {
                public double measure(MetricConfig config, long now) {
                    return (now - metadata.lastSuccessfulUpdate()) / 1000.0;
                }
            });
        }

        private void maybeRegisterTopicMetrics(String topic) {
            // if one sensor of the metrics has been registered for the topic,
            // then all other sensors should have been registered; and vice versa
            String topicRecordsCountName = "topic." + topic + ".records-per-batch";
            Sensor topicRecordCount = this.metrics.getSensor(topicRecordsCountName);
            if (topicRecordCount == null) {
                Map<String, String> metricTags = Collections.singletonMap("topic", topic);
                String metricGrpName = "producer-topic-metrics";

                topicRecordCount = this.metrics.sensor(topicRecordsCountName);
                MetricName m = this.metrics.metricName("record-send-rate", metricGrpName, metricTags);
                topicRecordCount.add(m, new Rate());

                String topicByteRateName = "topic." + topic + ".bytes";
                Sensor topicByteRate = this.metrics.sensor(topicByteRateName);
                m = this.metrics.metricName("byte-rate", metricGrpName, metricTags);
                topicByteRate.add(m, new Rate());

                String topicCompressionRateName = "topic." + topic + ".compression-rate";
                Sensor topicCompressionRate = this.metrics.sensor(topicCompressionRateName);
                m = this.metrics.metricName("compression-rate", metricGrpName, metricTags);
                topicCompressionRate.add(m, new Avg());

                String topicRetryName = "topic." + topic + ".record-retries";
                Sensor topicRetrySensor = this.metrics.sensor(topicRetryName);
                m = this.metrics.metricName("record-retry-rate", metricGrpName, metricTags);
                topicRetrySensor.add(m, new Rate());

                String topicErrorName = "topic." + topic + ".record-errors";
                Sensor topicErrorSensor = this.metrics.sensor(topicErrorName);
                m = this.metrics.metricName("record-error-rate", metricGrpName, metricTags);
                topicErrorSensor.add(m, new Rate());
            }
        }

        public void updateProduceRequestMetrics(Map<Integer, List<RecordBatch>> batches) {
            long now = time.milliseconds();
            for (List<RecordBatch> nodeBatch : batches.values()) {
                int records = 0;
                for (RecordBatch batch : nodeBatch) {
                    // register all per-topic metrics at once
                    String topic = batch.topicPartition.topic();
                    maybeRegisterTopicMetrics(topic);

                    // per-topic record send rate
                    String topicRecordsCountName = "topic." + topic + ".records-per-batch";
                    Sensor topicRecordCount = Utils.notNull(this.metrics.getSensor(topicRecordsCountName));
                    topicRecordCount.record(batch.recordCount);

                    // per-topic bytes send rate
                    String topicByteRateName = "topic." + topic + ".bytes";
                    Sensor topicByteRate = Utils.notNull(this.metrics.getSensor(topicByteRateName));
                    topicByteRate.record(batch.records.sizeInBytes());

                    // per-topic compression rate
                    String topicCompressionRateName = "topic." + topic + ".compression-rate";
                    Sensor topicCompressionRate = Utils.notNull(this.metrics.getSensor(topicCompressionRateName));
                    topicCompressionRate.record(batch.records.compressionRate());

                    // global metrics
                    this.batchSizeSensor.record(batch.records.sizeInBytes(), now);
                    this.queueTimeSensor.record(batch.drainedMs - batch.createdMs, now);
                    this.compressionRateSensor.record(batch.records.compressionRate());
                    this.maxRecordSizeSensor.record(batch.maxRecordSize, now);
                    records += batch.recordCount;
                }
                this.recordsPerRequestSensor.record(records, now);
            }
        }

        public void recordRetries(String topic, int count) {
            long now = time.milliseconds();
            this.retrySensor.record(count, now);
            String topicRetryName = "topic." + topic + ".record-retries";
            Sensor topicRetrySensor = this.metrics.getSensor(topicRetryName);
            if (topicRetrySensor != null)
                topicRetrySensor.record(count, now);
        }

        public void recordErrors(String topic, int count) {
            long now = time.milliseconds();
            this.errorSensor.record(count, now);
            String topicErrorName = "topic." + topic + ".record-errors";
            Sensor topicErrorSensor = this.metrics.getSensor(topicErrorName);
            if (topicErrorSensor != null)
                topicErrorSensor.record(count, now);
        }

        public void recordLatency(String node, long latency) {
            long now = time.milliseconds();
            this.requestTimeSensor.record(latency, now);
            if (!node.isEmpty()) {
                String nodeTimeName = "node-" + node + ".latency";
                Sensor nodeRequestTime = this.metrics.getSensor(nodeTimeName);
                if (nodeRequestTime != null)
                    nodeRequestTime.record(latency, now);
            }
        }

        public void recordThrottleTime(String node, long throttleTimeMs) {
            this.produceThrottleTimeSensor.record(throttleTimeMs, time.milliseconds());
        }

    }

}
