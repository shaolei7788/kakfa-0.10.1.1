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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.common.*;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.CopyOnWriteMap;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class acts as a queue that accumulates records into {@link org.apache.kafka.common.record.MemoryRecords}
 * instances to be sent to the server.
 *  此类充当一个队列,将累加的消息封装成MemoryRecords实例,再封装成批次,最后发送给服务器
 *
 * <p>
 * The accumulator uses a bounded amount of memory and append calls will block when that memory is exhausted, unless
 * this behavior is explicitly disabled.
 * 累加器使用有限数量的内存，当内存耗尽时，append调用将阻塞，除非显式禁用此行为。
 */
//此类充当一个内存的缓存容器,累加消息,封装成批次
public final class RecordAccumulator {

    private static final Logger log = LoggerFactory.getLogger(RecordAccumulator.class);

    private volatile boolean closed;
    private final AtomicInteger flushesInProgress;
    private final AtomicInteger appendsInProgress;
    private final int batchSize;
    private final CompressionType compression;//使用的压缩类型
    private final long lingerMs;
    //重试时间的间隔
    private final long retryBackoffMs;
    //内存池
    private final BufferPool free;
    private final Time time;
    //当前分区对应的存储队列 <封装消息主题和分区号的对象,消息>
    //申明的变量 有volatile关键字的map对象
    private final ConcurrentMap<TopicPartition, Deque<RecordBatch>> batches;
    //已经处理成功的批次
    private final IncompleteRecordBatches incomplete;
    // The following variables are only accessed by the sender thread, so we don't need to protect them.
    // 以下变量仅由发送方线程访问，因此我们不需要保护它们。
    // 是用来记录这个 tp 是否有还有未完成的 RecordBatch
    private final Set<TopicPartition> muted;
    //使用drain方法批量导出RecordBatch时，为了防止饥饿，使用drainIndex记录上次发送停止时的位置，下次继续从此位置开发发送
    private int drainIndex;

    /**
     * Create a new record accumulator
     * 
     * @param batchSize The size to use when allocating {@link org.apache.kafka.common.record.MemoryRecords} instances
     * @param totalSize The maximum memory the record accumulator can use.
     * @param compression The compression codec for the records
     * @param lingerMs An artificial delay time to add before declaring a records instance that isn't full ready for
     *        sending. This allows time for more records to arrive. Setting a non-zero lingerMs will trade off some
     *        latency for potentially better throughput due to more batching (and hence fewer, larger requests).
     * @param retryBackoffMs An artificial delay time to retry the produce request upon receiving an error. This avoids
     *        exhausting all retries in a short period of time.
     * @param metrics The metrics
     * @param time The time instance to use
     */
    public RecordAccumulator(int batchSize,
                             long totalSize,
                             CompressionType compression,
                             long lingerMs,
                             long retryBackoffMs,
                             Metrics metrics,
                             Time time) {
        this.drainIndex = 0;
        this.closed = false;
        this.flushesInProgress = new AtomicInteger(0);
        this.appendsInProgress = new AtomicInteger(0);
        this.batchSize = batchSize;
        this.compression = compression;
        this.lingerMs = lingerMs;
        this.retryBackoffMs = retryBackoffMs;
        //初始化赋值,一个map<k,v>结构
        this.batches = new CopyOnWriteMap<>();
        String metricGrpName = "producer-metrics";
        this.free = new BufferPool(totalSize, batchSize, metrics, time, metricGrpName);
        this.incomplete = new IncompleteRecordBatches();
        this.muted = new HashSet<>();
        this.time = time;
        registerMetrics(metrics, metricGrpName);
    }

    private void registerMetrics(Metrics metrics, String metricGrpName) {
        MetricName metricName = metrics.metricName("waiting-threads", metricGrpName, "The number of user threads blocked waiting for buffer memory to enqueue their records");
        Measurable waitingThreads = new Measurable() {
            public double measure(MetricConfig config, long now) {
                return free.queued();
            }
        };
        metrics.addMetric(metricName, waitingThreads);

        metricName = metrics.metricName("buffer-total-bytes", metricGrpName, "The maximum amount of buffer memory the client can use (whether or not it is currently used).");
        Measurable totalBytes = new Measurable() {
            public double measure(MetricConfig config, long now) {
                return free.totalMemory();
            }
        };
        metrics.addMetric(metricName, totalBytes);

        metricName = metrics.metricName("buffer-available-bytes", metricGrpName, "The total amount of buffer memory that is not being used (either unallocated or in the free list).");
        Measurable availableBytes = new Measurable() {
            public double measure(MetricConfig config, long now) {
                return free.availableMemory();
            }
        };
        metrics.addMetric(metricName, availableBytes);

        Sensor bufferExhaustedRecordSensor = metrics.sensor("buffer-exhausted-records");
        metricName = metrics.metricName("buffer-exhausted-rate", metricGrpName, "The average per-second number of record sends that are dropped due to buffer exhaustion");
        bufferExhaustedRecordSensor.add(metricName, new Rate());
    }

    /**
     * Add a record to the accumulator, return the append result
     * <p>
     * The append result will contain the future metadata, and flag for whether the appended batch is full or a new batch is created
     * <p>
     *
     * @param tp The topic/partition to which this record is being sent 封装分区消息的对象
     * @param timestamp The timestamp of the record
     * @param key The key for the record
     * @param value The value for the record
     * @param callback The user-supplied callback to execute when the request is complete
     * @param maxTimeToBlock The maximum time in milliseconds to block for buffer memory to be available
     */
    public RecordAppendResult append(TopicPartition tp,
                                     long timestamp,
                                     byte[] key,
                                     byte[] value,
                                     Callback callback,
                                     long maxTimeToBlock) throws InterruptedException {
        // We keep track of the number of appending thread to make sure we do not miss batches in
        // abortIncompleteBatches().
        //跟踪附加线程的数量，以确保不会错过批处理
        appendsInProgress.incrementAndGet();
        try {
            // check if we have an in-progress batch
            /**
             * 步骤一: 先根据分区找到消息应该插入到哪个队列里面
             * 如果已经存在队列,那么就直接使用已有队列
             * 如果队列不存在,那么就新创建一个队列
             *
             * 我们肯定是有了存储批次消息的队列,要注意一点,当代码第一次进入到这里的时候,获取的是一个空队列
             *
             * 这个方法里面,主要是针对batches进行的操作
             * kafka自己封装了一个数据结构 : CopeOnWriteMap(这个数据结构是线程安全的)
             */
            //TODO 从batches中根据当前分区获取对应的队列信息
            /**
             * 因为Kafka发送消息不是一条消息一条消息发送的，而是一个批次一个批次发送的，当满足一个
             * 批次了，就把批次存入到当前分区对应的队列里面去，后面由sender线程从队列里面取批次发送到服务端。
             *获取当前分区对应的队列信息会有两种可能：
             * 状况一：如果当前分区之前就有创建好了队列了，那么就直接获取到一个对应的队列
             * 状况二：如果之前这个分区对应的队列信息不存在,那么此次就获取到一个空的队列
             *
             * 我们是场景驱动分析，这个时候是第一次进来，所以之前是没有队列的
             * 这儿会创建出来一个空的队列
             */
            Deque<RecordBatch> dq = getOrCreateDeque(tp);//根据消息的主题和分区信息,去获取到消息要发往的队列
            synchronized (dq) {//分段加锁,目的是缩小锁的粒度,因为锁与锁之间还有其他的业务
                //首先进来的是第一个线程
                if (closed)
                    throw new IllegalStateException("Cannot send after the producer is closed.");
                /**
                 * 步骤二:
                 *      尝试往批次队列里面添加消息
                 *      最开始添加数据肯定是失败的,我们只是有了队列
                 *      数据是需要封装在批次对象RecordBatch里面(这个批次对象是需要分配内存的)
                 *      当代码第一次运行到这里的时候,是没有分配内存的,所以不会成功
                 */
                RecordAppendResult appendResult = tryAppend(timestamp, key, value, callback, dq);
                //第一次进来的时候appendResult就是null
                if (appendResult != null) {
                    return appendResult;
                }
            }//释放锁

            // we don't have an in-progress record batch try to allocate a new batch
            // 申请创建批次对象
            /**
             * 步骤三: 计算一个RecordBatch批次的大小
             * 执行到这里,说明上一步插入批次失败,因为batches里面没有该消息的主题分区号对应的Deque<RecordBatch>,那么就需要创建一个批次消息队列,
             * 首先就是计算一个批次RecordBatch的大小
             * 在消息的大小和批次的大小之间取一个最大值,用这个值作为当前批次的大小
             */
            int size = Math.max(this.batchSize, Records.LOG_OVERHEAD + Record.recordSize(key, value));
            log.trace("Allocating a new {} byte message buffer for topic {} partition {}", size, tp.topic(), tp.partition());
            //从内存池中分配内存
            /**
             * 步骤四: 根据批次RecordBatch的大小去分配内存 maxTimeToBlock 最大等待时间
             */
            ByteBuffer buffer = free.allocate(size, maxTimeToBlock);
            synchronized (dq) {
                // Need to check if producer is closed again after grabbing the dequeue lock.
                if (closed)
                    throw new IllegalStateException("Cannot send after the producer is closed.");
                /**
                 * 步骤五:
                 *      尝试把数据写入进批次里面
                 *      第一次代码执行到这里,依然是失败的(appendResult == null)
                 *      虽然已经分配了内存,但是还是没有创建批次对象RecordBatch,还是不能写入数据
                 */
                RecordAppendResult appendResult = tryAppend(timestamp, key, value, callback, dq);
                if (appendResult != null) {
                    // Somebody else found us a batch, return the one we waited for! Hopefully this doesn't happen often...
                    // 释放内存
                    free.deallocate(buffer);
                    return appendResult;
                }
                /**
                 * 步骤六: 根据内存大小封装批次
                 *      第一次执行到这里,会创建一个批次对象,那么其他线程也可以写数据进去了
                 */
                MemoryRecords records = MemoryRecords.emptyRecords(buffer, compression, this.batchSize);
                RecordBatch batch = new RecordBatch(tp, records, time.milliseconds());
                //尝试往里面写数据,是可以执行成功的
                FutureRecordMetadata futureRecordMetadata = batch.tryAppend(timestamp, key, value, callback, time.milliseconds());
                FutureRecordMetadata future = Utils.notNull(futureRecordMetadata);
                /**
                 * 步骤七:
                 *      将写有数据的批次对象RecordBatch放入队列的队尾
                 */
                dq.addLast(batch);
                incomplete.add(batch);
                //第三个参数 newBatchCreated = ture 即产生了新批次
                return new RecordAppendResult(future, dq.size() > 1 || batch.records.isFull(), true);
            }//释放锁
        } finally {
            appendsInProgress.decrementAndGet();
        }
    }

    /**
     * If `RecordBatch.tryAppend` fails (i.e. the record batch is full), close its memory records to release temporary
     * resources (like compression streams buffers).
     */
    private RecordAppendResult tryAppend(long timestamp, byte[] key, byte[] value, Callback callback, Deque<RecordBatch> deque) {
        //首先要获取到队列里面最新的一个批次
        RecordBatch last = deque.peekLast();
        //第一次进来的时候,last是null

        //线程二进来的时候,last不是null
        if (last != null) {
            //线程二插入数据的OK了
            FutureRecordMetadata future = last.tryAppend(timestamp, key, value, callback, time.milliseconds());
            if (future == null)
                last.records.close();
            else
                return new RecordAppendResult(future, deque.size() > 1 || last.records.isFull(), false);
        }
        return null;
    }

    /**
     * Abort the batches that have been sitting in RecordAccumulator for more than the configured requestTimeout
     * due to metadata being unavailable
     *
     * 由于元数据不可用，中止在RecordAccumulator中超时的的批处理
     */
    public List<RecordBatch> abortExpiredBatches(int requestTimeout, long now) {
        List<RecordBatch> expiredBatches = new ArrayList<>();
        int count = 0;
        for (Map.Entry<TopicPartition, Deque<RecordBatch>> entry : this.batches.entrySet()) {
            //获取到每个分区的队列,队列里面对应的批次
            Deque<RecordBatch> dq = entry.getValue();
            TopicPartition tp = entry.getKey();
            // We only check if the batch should be expired if the partition does not have a batch in flight.
            // This is to prevent later batches from being expired while an earlier batch is still in progress.
            // Note that `muted` is only ever populated if `max.in.flight.request.per.connection=1` so this protection
            // is only active in this case. Otherwise the expiration order is not guaranteed.
            if (!muted.contains(tp)) {
                synchronized (dq) {
                    // iterate over the batches and expire them if they have been in the accumulator for more than requestTimeOut
                    RecordBatch lastBatch = dq.peekLast();
                    Iterator<RecordBatch> batchIterator = dq.iterator();
                    //迭代获取每个分区中的批次
                    while (batchIterator.hasNext()) {
                        RecordBatch batch = batchIterator.next();
                        boolean isFull = batch != lastBatch || batch.records.isFull();
                        // check if the batch is expired
                        //判断是否超时
                        if (batch.maybeExpire(requestTimeout, retryBackoffMs, now, this.lingerMs, isFull)) {
                            //增加到超时的数据结构里面
                            expiredBatches.add(batch);
                            count++;
                            //从原数据结构中移除
                            batchIterator.remove();
                            //释放资源
                            deallocate(batch);
                        } else {
                            // Stop at the first batch that has not expired.
                            break;
                        }
                    }
                }
            }
        }
        if (!expiredBatches.isEmpty())
            log.trace("Expired {} batches in accumulator", count);

        return expiredBatches;
    }

    /**
     * Re-enqueue the given record batch in the accumulator to retry
     */
    public void reenqueue(RecordBatch batch, long now) {
        //重试的次数累加
        batch.attempts++;
        //上一次的重试时间
        batch.lastAttemptMs = now;
        batch.lastAppendTime = now;
        batch.setRetry();
        Deque<RecordBatch> deque = getOrCreateDeque(batch.topicPartition);
        synchronized (deque) {
            //将批次重新放入队列里面 并且放入队头
            deque.addFirst(batch);
        }
    }

    /**
     * Get a list of nodes whose partitions are ready to be sent, and the earliest time at which any non-sendable
     * partition will be ready; Also return the flag for whether there are any unknown leaders for the accumulated
     * partition batches.
     * <p>
     * A destination node is ready to send data if:
     * <ol>
     * <li>There is at least one partition that is not backing off its send
     * <li><b>and</b> those partitions are not muted (to prevent reordering if
     *   {@value org.apache.kafka.clients.producer.ProducerConfig#MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION}
     *   is set to one)</li>
     * <li><b>and <i>any</i></b> of the following are true</li>
     * <ul>
     *     <li>The record set is full</li>
     *     <li>The record set has sat in the accumulator for at least lingerMs milliseconds</li>
     *     <li>The accumulator is out of memory and threads are blocking waiting for data (in this case all partitions
     *     are immediately considered ready).</li>
     *     <li>The accumulator has been closed</li>
     * </ul>
     * </ol>
     */
    public ReadyCheckResult ready(Cluster cluster, long nowMs) {
        Set<Node> readyNodes = new HashSet<>();
        long nextReadyCheckDelayMs = Long.MAX_VALUE;
        Set<String> unknownLeaderTopics = new HashSet<>();
        //点进queued()方法里面,会发现其实是在获取waiters的大小
        //当waiters里面有数据的时候(exhausted是true),说明我们的内存池的内存已经不够了
        boolean exhausted = this.free.queued() > 0;
        //遍历batches,TopicPartition  Deque<RecordBatch>,判断RecordBatch是否符合发送条件
        for (Map.Entry<TopicPartition, Deque<RecordBatch>> entry : this.batches.entrySet()) {
            //获取到分区
            TopicPartition part = entry.getKey();
            //获取到分区对应的消息队列
            Deque<RecordBatch> deque = entry.getValue();
            //获取到该分区的leader partition所在的节点
            Node leader = cluster.leaderFor(part);
            synchronized (deque) {
                //如果没有找到对应的主机,将其添加到unknownLeaderTopics列表中
                if (leader == null && !deque.isEmpty()) {
                    // This is a partition for which leader is not known, but messages are available to send.
                    // Note that entries are currently not removed from batches when deque is empty.
                    //并不会删除deque,只是将part存入unknownLeaderTopics
                    unknownLeaderTopics.add(part.topic());
                } else if (!readyNodes.contains(leader) && !muted.contains(part)) {
                    //todo 如果 muted 集合包含这个 tp，那么在遍历时将不会处理它对应的 deque，也就是说，
                    // 如果一个 tp 加入了 muted 集合中，即使它对应的 RecordBatch 可以发送了，也不会触发引起其对应的 leader 被选择出来

                    //获取队列的第一个批次
                    RecordBatch batch = deque.peekFirst();
                    //如果batch不是null,我们就判断一下是否可以发送这个批次
                    if (batch != null) {
                        /**
                         * 接下来就是判断符合发送的条件
                         * 当一个批次没有被写满,但是到了一定的时间间隔,还是会被发送出去的,默认值是100ms
                         */
                        /**
                         * batch.attempts : 重试的次数
                         * batch.lastAttemptMs : 上一次重试的时间
                         * retryBackoffMs 重试的时间间隔
                         * backingOff : 重新发送数据的时间是否到了
                         */
                        boolean backingOff = batch.attempts > 0 && batch.lastAttemptMs + retryBackoffMs > nowMs;
                        /**
                         * nowMs : 当前的时间
                         * batch.lastAttemptMs : 上一次重试的时间
                         * waitedTimeMs : 已经等了多久
                         */
                        long waitedTimeMs = nowMs - batch.lastAttemptMs;
                        /**
                         * timeToWaitMs=lingerMs 最多能等待多久
                         * lingerMs的默认值是0,如果是0的话,表示来一条消息发送一条消息,那明显是不合适的
                         *
                         * 所以我们发送数据的时候,一定要记得去配置这个参数,比如100ms
                         *
                         * timeToWaitMs=lingerMs=100ms
                         * 消息最多存多久就必须要发送出去了
                         *
                         * backingOff : 重新发送数据的时间是否到了
                         */
                        long timeToWaitMs = backingOff ? retryBackoffMs : lingerMs;
                        /**
                         * timeToWaitMs: 最多能等待多久
                         * waitedTimeMs: 已经等了多久
                         * timeLeftMs: 还需要等待多久
                         *
                         */
                        long timeLeftMs = Math.max(timeToWaitMs - waitedTimeMs, 0);
                        /**
                         * deque.size() > 1 说明这个队列里面至少已经有一个批次写满了,如果写满了肯定是要发送出去的
                         *      当然有可能这个队列里面只有一个批次,但是这个批次已经写满了,那么也是可以发送的
                         * batch.records.isFull() 是否有写满的批次
                         */
                        boolean full = deque.size() > 1 || batch.records.isFull();
                        /**
                         * waitedTimeMs:已经等了多久
                         * timeToWaitMs:最多能等待多久
                         * expired=true: 时间到了,该发送消息了
                         */
                        boolean expired = waitedTimeMs >= timeToWaitMs;
                        /**
                         * full: 如果一个批次写满了(无论时间有没有到)
                         * expired: 时间到了(无论批次有没有写满)
                         * exhausted: 内存不够(消息发送出去之后,就会释放内存)
                         *
                         * closed || flushInProgress() : 当生产者要退出的时候,需要把内存中的所有数据发送出去
                         */
                        boolean sendable = full || expired || exhausted || closed || flushInProgress();
                        //可以发送消息了
                        if (sendable && !backingOff) {
                            //把可以发送批次的分区对应的leader分区所在的主机放入readyNodes
                            readyNodes.add(leader);
                        } else {
                            // Note that this results in a conservative estimate since an un-sendable partition may have
                            // a leader that will later be found to have sendable data. However, this is good enough
                            // since we'll just wake up and then sleep again for the remaining time.
                            nextReadyCheckDelayMs = Math.min(timeLeftMs, nextReadyCheckDelayMs);
                        }
                    }
                }
            }
        }

        return new ReadyCheckResult(readyNodes, nextReadyCheckDelayMs, unknownLeaderTopics);
    }

    /**
     * @return Whether there is any unsent record in the accumulator.
     */
    public boolean hasUnsent() {
        for (Map.Entry<TopicPartition, Deque<RecordBatch>> entry : this.batches.entrySet()) {
            Deque<RecordBatch> deque = entry.getValue();
            synchronized (deque) {
                if (!deque.isEmpty())
                    return true;
            }
        }
        return false;
    }

    /**
     * Drain all the data for the given nodes and collate them into a list of batches that will fit within the specified
     * size on a per-node basis. This method attempts to avoid choosing the same topic-node over and over.
     *  发送所有数据到给定的节点，并将它们整理成一个批处理列表，
     *  这些批处理列表将在每个节点的基础上指定适合的大小。这种方法试图避免反复选择同一个主题节点。
     * @param cluster The current cluster metadata
     * @param nodes The list of node to drain
     * @param maxSize The maximum number of bytes to drain
     * @param now The current unix time in milliseconds
     * @return A list of {@link RecordBatch} for each node specified with total size less than the requested maxSize.
     */
    //是用来遍历可发送请求的 node，然后再遍历在这个 node 上所有 tp，如果 tp 对应的 deque 有数据，将会被选择出来直到超过一个请求的最大长度（max.request.size）为止，
    // 也就说说即使 RecordBatch 没有达到条件，但为了保证每个 request 尽快多地发送数据提高发送效率，这个 RecordBatch 依然会被提前选出来并进行发送
    public Map<Integer, List<RecordBatch>> drain(Cluster cluster,
                                                 Set<Node> nodes,
                                                 int maxSize,
                                                 long now) {
        if (nodes.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<Integer, List<RecordBatch>> batches = new HashMap<>();
        //接收消息的节点
        for (Node node : nodes) {
            int size = 0;
            //获取指定nodeid，对应的分区
            List<PartitionInfo> parts = cluster.partitionsForNode(node.id());
            List<RecordBatch> ready = new ArrayList<>();
            /* to make starvation less likely this loop doesn't start at 0 */
            int start = drainIndex = drainIndex % parts.size();
            do {
                PartitionInfo part = parts.get(drainIndex);
                TopicPartition tp = new TopicPartition(part.topic(), part.partition());
                // Only proceed if the partition has no in-flight batches.
                //todo 如果 muted 集合包含这个 tp，那么在遍历时将不会处理它对应的 deque，也就是说，
                // 如果一个 tp 加入了 muted 集合中，即使它对应的 RecordBatch 可以发送了，也不会触发引起其对应的 leader 被选择出来
                if (!muted.contains(tp)) {
                    Deque<RecordBatch> deque = getDeque(new TopicPartition(part.topic(), part.partition()));
                    if (deque != null) {
                        synchronized (deque) {
                            RecordBatch first = deque.peekFirst();
                            if (first != null) {
                                boolean backoff = first.attempts > 0 && first.lastAttemptMs + retryBackoffMs > now;
                                // Only drain the batch if it is not during backoff period.
                                if (!backoff) {
                                    if (size + first.records.sizeInBytes() > maxSize && !ready.isEmpty()) {
                                        // there is a rare case that a single batch size is larger than the request size due
                                        // to compression; in this case we will still eventually send this batch in a single
                                        // request
                                        break;
                                    } else {
                                        RecordBatch batch = deque.pollFirst();
                                        batch.records.close();
                                        size += batch.records.sizeInBytes();
                                        ready.add(batch);
                                        batch.drainedMs = now;
                                    }
                                }
                            }
                        }
                    }
                }
                this.drainIndex = (this.drainIndex + 1) % parts.size();
            } while (start != drainIndex);
            batches.put(node.id(), ready);
        }
        return batches;
    }

    private Deque<RecordBatch> getDeque(TopicPartition tp) {
        return batches.get(tp);
    }

    /**
     * Get the deque for the given topic-partition, creating it if necessary.
     * TODO 注意这个方法是多线程情况下执行
     */
    private Deque<RecordBatch> getOrCreateDeque(TopicPartition tp) {
        //直接从batches里面获取到当前分区对应的队列存储
        Deque<RecordBatch> d = this.batches.get(tp);
        //当初始化,代码第一次运行到这里的时候,是获取不到队列的,也就是说d是null
        if (d != null)
            return d;
        //代码继续执行,创建一个新的空的队列
        //代码走到这儿说明这个batches对象里面没有当前分区的队列信息，所以创建了一个空队列
        d = new ArrayDeque<>();//队列的大小默认是16
        ////把空队列存入到batches里面，并返回previous对象
        Deque<RecordBatch> previous = this.batches.putIfAbsent(tp, d);
        //返回封装好的当前分区对应的空队列信息对象
        if (previous == null)
            return d;
        else
            //返回新的结果
            return previous;
    }

    /**
     * Deallocate the record batch
     */
    public void deallocate(RecordBatch batch) {
        //移除已经处理成功的批次
        incomplete.remove(batch);
        //释放内存,回收内存
        free.deallocate(batch.records.buffer(), batch.records.initialCapacity());
    }
    
    /**
     * Are there any threads currently waiting on a flush?
     *
     * package private for test
     */
    boolean flushInProgress() {
        return flushesInProgress.get() > 0;
    }

    /* Visible for testing */
    Map<TopicPartition, Deque<RecordBatch>> batches() {
        return Collections.unmodifiableMap(batches);
    }
    
    /**
     * Initiate the flushing of data from the accumulator...this makes all requests immediately ready
     */
    public void beginFlush() {
        this.flushesInProgress.getAndIncrement();
    }

    /**
     * Are there any threads currently appending messages?
     */
    private boolean appendsInProgress() {
        return appendsInProgress.get() > 0;
    }

    /**
     * Mark all partitions as ready to send and block until the send is complete
     */
    public void awaitFlushCompletion() throws InterruptedException {
        try {
            for (RecordBatch batch : this.incomplete.all())
                batch.produceFuture.await();
        } finally {
            this.flushesInProgress.decrementAndGet();
        }
    }

    /**
     * This function is only called when sender is closed forcefully. It will fail all the
     * incomplete batches and return.
     */
    public void abortIncompleteBatches() {
        // We need to keep aborting the incomplete batch until no thread is trying to append to
        // 1. Avoid losing batches.
        // 2. Free up memory in case appending threads are blocked on buffer full.
        // This is a tight loop but should be able to get through very quickly.
        do {
            abortBatches();
        } while (appendsInProgress());
        // After this point, no thread will append any messages because they will see the close
        // flag set. We need to do the last abort after no thread was appending in case there was a new
        // batch appended by the last appending thread.
        abortBatches();
        this.batches.clear();
    }

    /**
     * Go through incomplete batches and abort them.
     */
    private void abortBatches() {
        for (RecordBatch batch : incomplete.all()) {
            Deque<RecordBatch> dq = getDeque(batch.topicPartition);
            // Close the batch before aborting
            synchronized (dq) {
                batch.records.close();
                dq.remove(batch);
            }
            batch.done(-1L, Record.NO_TIMESTAMP, new IllegalStateException("Producer is closed forcefully."));
            deallocate(batch);
        }
    }

    public void mutePartition(TopicPartition tp) {
        muted.add(tp);
    }

    public void unmutePartition(TopicPartition tp) {
        muted.remove(tp);
    }

    /**
     * Close this accumulator and force all the record buffers to be drained
     */
    public void close() {
        this.closed = true;
    }

    /*
     * Metadata about a record just appended to the record accumulator
     */
    public final static class RecordAppendResult {
        public final FutureRecordMetadata future;
        public final boolean batchIsFull;
        public final boolean newBatchCreated;

        public RecordAppendResult(FutureRecordMetadata future, boolean batchIsFull, boolean newBatchCreated) {
            this.future = future;
            this.batchIsFull = batchIsFull;
            this.newBatchCreated = newBatchCreated;
        }
    }

    /*
     * The set of nodes that have at least one complete record batch in the accumulator
     */
    public final static class ReadyCheckResult {
        public final Set<Node> readyNodes;
        public final long nextReadyCheckDelayMs;
        public final Set<String> unknownLeaderTopics;

        public ReadyCheckResult(Set<Node> readyNodes, long nextReadyCheckDelayMs, Set<String> unknownLeaderTopics) {
            this.readyNodes = readyNodes;
            this.nextReadyCheckDelayMs = nextReadyCheckDelayMs;
            this.unknownLeaderTopics = unknownLeaderTopics;
        }
    }
    
    /*
     * A threadsafe helper class to hold RecordBatches that haven't been ack'd yet
     */
    private final static class IncompleteRecordBatches {
        private final Set<RecordBatch> incomplete;

        public IncompleteRecordBatches() {
            this.incomplete = new HashSet<RecordBatch>();
        }
        
        public void add(RecordBatch batch) {
            synchronized (incomplete) {
                this.incomplete.add(batch);
            }
        }
        
        public void remove(RecordBatch batch) {
            synchronized (incomplete) {
                boolean removed = this.incomplete.remove(batch);
                if (!removed)
                    throw new IllegalStateException("Remove from the incomplete set failed. This should be impossible.");
            }
        }
        
        public Iterable<RecordBatch> all() {
            synchronized (incomplete) {
                return new ArrayList<>(this.incomplete);
            }
        }
    }

}
