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
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * A batch of records that is or will be sent.
 * 
 * This class is not thread safe and external synchronization must be used when modifying it
 */
public final class RecordBatch {

    private static final Logger log = LoggerFactory.getLogger(RecordBatch.class);
    //记录了保存的Record个数
    public int recordCount = 0;
    //该批最大的Record的字节数
    public int maxRecordSize = 0;
    //尝试发送当前RecordBatch的重试次数
    public volatile int attempts = 0;
    public final long createdMs;
    public long drainedMs;
    //上一次尝试发送的时间戳
    public long lastAttemptMs;
    //消息真正存放的地方
    public final MemoryRecords records;
    //当前RecordBatch中缓存的消息，都会发送给此TopicPartition
    public final TopicPartition topicPartition;
    //标识RecordBatch状态的Future对象
    public final ProduceRequestResult produceFuture;
    //上一次向RecordBatch追加消息的时间戳
    public long lastAppendTime;
    //消息回调对象集合，Thunk中的callback字段就指向对应消息的Callback对象
    private final List<Thunk> thunks;
    //用来记录某消息在RecordBatch中的偏移量
    private long offsetCounter = 0L;
    public RecordBatch(TopicPartition tp, MemoryRecords records, long now) {
        this.createdMs = now;
        this.lastAttemptMs = now;
        this.records = records;
        this.topicPartition = tp;
        this.produceFuture = new ProduceRequestResult();
        this.thunks = new ArrayList<Thunk>();
        this.lastAppendTime = createdMs;
        this.retry = false;
    }
    //是否正在重试，如果RecordBatch中的数据发送失败，则会重新尝试发送
    private boolean retry;

    /**
     * Append the record to the current record set and return the relative offset within that record set
     * 追加消息到当前的消息记录集合中,并返回当前记录集的相对偏移量(大小)
     * @return The RecordSend corresponding to this record or null if there isn't sufficient room.
     */
    public FutureRecordMetadata tryAppend(long timestamp, byte[] key, byte[] value, Callback callback, long now) {
        if (!this.records.hasRoomFor(key, value)) {
            return null;
        } else {
            //TODO 将数据写入RecordBatch
            long checksum = this.records.append(offsetCounter++, timestamp, key, value);
            this.maxRecordSize = Math.max(this.maxRecordSize, Record.recordSize(key, value));
            this.lastAppendTime = now;
            FutureRecordMetadata future = new FutureRecordMetadata(this.produceFuture, this.recordCount,
                                                                   timestamp, checksum,
                                                                   key == null ? -1 : key.length,
                                                                   value == null ? -1 : value.length);
            if (callback != null) {
                //有回调方法的才加入到thunks里
                thunks.add(new Thunk(callback, future));
            }
            this.recordCount++;
            return future;
        }
    }

    /**
     * Complete the request
     * 
     * @param baseOffset The base offset of the messages assigned by the server
     * @param timestamp The timestamp returned by the broker.
     * @param exception The exception that occurred (or null if the request was successful)
     */
    public void done(long baseOffset, long timestamp, RuntimeException exception) {
        log.trace("Produced messages to topic-partition {} with base offset offset {} and error: {}.",
                  topicPartition,
                  baseOffset,
                  exception);
        // execute callbacks
        /**
         * 我们发送数据的时候,一条消息就代表一个thunk
         * 遍历我们所有发出去的消息
         */
        for (int i = 0; i < this.thunks.size(); i++) {
            try {
                Thunk thunk = this.thunks.get(i);
                //exception == null代表响应里面没有异常
                if (exception == null) {
                    // If the timestamp returned by server is NoTimestamp, that means CreateTime is used. Otherwise LogAppendTime is used.
                    RecordMetadata metadata = new RecordMetadata(this.topicPartition,  baseOffset, thunk.future.relativeOffset(),
                                                                 timestamp == Record.NO_TIMESTAMP ? thunk.future.timestamp() : timestamp,
                                                                 thunk.future.checksum(),
                                                                 thunk.future.serializedKeySize(),
                                                                 thunk.future.serializedValueSize());
                    //没有异常,就会调用消息的回调函数中的回调方法
                    //这里的回调函数就是生产者发送消息时传入的
                    thunk.callback.onCompletion(metadata, null);
                } else {
                    thunk.callback.onCompletion(null, exception);
                }
            } catch (Exception e) {
                log.error("Error executing user-provided callback on message for topic-partition {}:", topicPartition, e);
            }
        }
        this.produceFuture.done(topicPartition, baseOffset, exception);
    }

    /**
     * A callback and the associated FutureRecordMetadata argument to pass to it.
     */
    final private static class Thunk {
        final Callback callback;
        final FutureRecordMetadata future;

        public Thunk(Callback callback, FutureRecordMetadata future) {
            this.callback = callback;
            this.future = future;
        }
    }

    @Override
    public String toString() {
        return "RecordBatch(topicPartition=" + topicPartition + ", recordCount=" + recordCount + ")";
    }

    /**
     * A batch whose metadata is not available should be expired if one of the following is true:
     * <ol>
     *     <li> the batch is not in retry AND request timeout has elapsed after it is ready (full or linger.ms has reached).
     *     <li> the batch is in retry AND request timeout has elapsed after the backoff period ended.
     * </ol>
     */
    public boolean maybeExpire(int requestTimeoutMs, long retryBackoffMs, long now, long lingerMs, boolean isFull) {
        boolean expire = false;
        String errorMessage = null;
        /**
         * requestTimeoutMs: 请求超时的时长,默认是30s
         * now: 当前时间
         * lastAppendTime: 批次的创建时间(上次重试的时间)
         * now - this.lastAppendTime:大于30s,说明批次超时了,没有发送出去
         */
        if (!this.inRetry() && isFull && requestTimeoutMs < (now - this.lastAppendTime)) {
            expire = true;
            //记录异常信息
            errorMessage = (now - this.lastAppendTime) + " ms has passed since last append";
            /**
             * lingerMs: 100ms,无论如何都要把消息发送出去的时间
             * createdMs: 创建批次的时间
             *
             * 已经大于30s了,说明也是超时了
             */
        } else if (!this.inRetry() && requestTimeoutMs < (now - (this.createdMs + lingerMs))) {
            expire = true;
            errorMessage = (now - (this.createdMs + lingerMs)) + " ms has passed since batch creation plus linger time";
            /**
             *  针对重试:
             *   lastAttemptMs: 上次重试的时间(批次创建的时间)
             *   retryBackoffMs: 重试的时间间隔
             *   说明也超时了
             */
        } else if (this.inRetry() && requestTimeoutMs < (now - (this.lastAttemptMs + retryBackoffMs))) {
            expire = true;
            errorMessage = (now - (this.lastAttemptMs + retryBackoffMs)) + " ms has passed since last attempt plus backoff time";
        }

        if (expire) {
            this.records.close();
            //调用done方法
            //方法里面传入一个TimeoutException异常
            //TODO 处理超时的批次
            this.done(-1L, Record.NO_TIMESTAMP,
                      new TimeoutException("Expiring " + recordCount + " record(s) for " + topicPartition + " due to " + errorMessage));
        }

        return expire;
    }

    /**
     * Returns if the batch is been retried for sending to kafka
     */
    public boolean inRetry() {
        return this.retry;
    }

    /**
     * Set retry to true if the batch is being retried (for send)
     */
    public void setRetry() {
        this.retry = true;
    }
}
