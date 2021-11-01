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
package org.apache.kafka.clients.producer.internals;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;


/**
 * A pool of ByteBuffers kept under a given memory limit. This class is fairly specific to the needs of the producer. In
 * particular it has the following properties:
 * <ol>
 * <li>There is a special "poolable size" and buffers of this size are kept in a free list and recycled
 * <li>It is fair. That is all memory is given to the longest waiting thread until it has sufficient memory. This
 * prevents starvation or deadlock when a thread asks for a large chunk of memory and needs to block until multiple
 * buffers are deallocated.
 * </ol>
 */
public final class BufferPool {

    // 32m
    private final long totalMemory;
    //一个批次的大小,默认是16K
    private final int poolableSize;
    private final ReentrantLock lock;
    //内存池就是一个队列,队列里面放的就是一块块的内存
    //缓存了指定大小的ByteBuffer对象
    private final Deque<ByteBuffer> free;
    //如果内存不够,就会往waiters里面加一个Condition对象
    private final Deque<Condition> waiters;
    //记录了可用空间的大小，这个空间是totalMemory减去free列表中全部ByteBuffer的大小
    private long availableMemory;
    private final Metrics metrics;
    private final Time time;
    private final Sensor waitTime;

    /**
     * Create a new buffer pool
     * 
     * @param memory The maximum amount of memory that this buffer pool can allocate
     * @param poolableSize The buffer size to cache in the free list rather than deallocating
     * @param metrics instance of Metrics
     * @param time time instance
     * @param metricGrpName logical group name for metrics
     */
    public BufferPool(long memory, int poolableSize, Metrics metrics, Time time, String metricGrpName) {
        this.poolableSize = poolableSize;
        this.lock = new ReentrantLock();
        this.free = new ArrayDeque<ByteBuffer>();
        this.waiters = new ArrayDeque<Condition>();
        this.totalMemory = memory;
        this.availableMemory = memory;
        this.metrics = metrics;
        this.time = time;
        this.waitTime = this.metrics.sensor("bufferpool-wait-time");
        MetricName metricName = metrics.metricName("bufferpool-wait-ratio",
                                                   metricGrpName,
                                                   "The fraction of time an appender waits for space allocation.");
        this.waitTime.add(metricName, new Rate(TimeUnit.NANOSECONDS));
    }

    /**
     * Allocate a buffer of the given size. This method blocks if there is not enough memory and the buffer pool
     * is configured with blocking mode.
     * 
     * @param size The buffer size to allocate in bytes
     * @param maxTimeToBlockMs The maximum time in milliseconds to block for buffer memory to be available
     * @return The buffer
     * @throws InterruptedException If the thread is interrupted while blocked
     * @throws IllegalArgumentException if size is larger than the total memory controlled by the pool (and hence we would block
     *         forever)
     */
    public ByteBuffer allocate(int size, long maxTimeToBlockMs) throws InterruptedException {
        //如果想要申请的内存大小超过32M,就会报错
        if (size > this.totalMemory)
            throw new IllegalArgumentException("Attempt to allocate " + size
                                               + " bytes, but there is a hard limit of "
                                               + this.totalMemory
                                               + " on memory allocations.");

        //加锁的代码
        this.lock.lock();
        try {
            // check if we have a free buffer of the right size pooled
            //poolableSize代表的是一个批次的大小,默认是16k
            //如果我们申请的内存大小跟默认的批次内存大小相等,并且内存池中不为空,那么直接从内存池中获取一个内存块就行
            //当代码第一次运行到这里,内存池是空的,所以获取不到内存
            if (size == poolableSize && !this.free.isEmpty()) {
                return this.free.pollFirst();
            }
            // now check if the request is immediately satisfiable with the
            // memory on hand or if we need to block
            //内存的个数*内存的大小=free的大小
            int freeListSize = this.free.size() * this.poolableSize;
            if (this.availableMemory + freeListSize >= size) {
                //size是我们这次要申请的内存大小
                //this.availableMemory + freeListSize 目前可用的总内存
                // we have enough unallocated or pooled memory to immediately satisfy the request
                // 释放内存  freeUp 释放; 开放的意思
                freeUp(size);
                //进行内存扣减
                this.availableMemory -= size;
                lock.unlock();
                //直接分配内存
                return ByteBuffer.allocate(size);
            } else {
                //还有一种情况,就是剩余的可用内存值小于我们要申请的内存大小
                // we are out of memory and will have to block
                //统计已经分配的内存
                int accumulated = 0;
                ByteBuffer buffer = null;
                Condition moreMemory = this.lock.newCondition();
                long remainingTimeToBlockNs = TimeUnit.MILLISECONDS.toNanos(maxTimeToBlockMs);
                //等待被人释放内存 添加到最后  将阻塞线程对应的Condition加入队列 等待唤醒，唤醒的顺序根据入队顺序决定(先进先出)
                this.waiters.addLast(moreMemory);
                // loop over and over until we have a buffer or have reserved
                // enough memory to allocate one
                /**
                 * 总的分配思路: 可能一下子分配不了那么大的内存,但是可以一次分配一点
                 */
                //如果分配的内存太小,没有size大
                //那么内存池就会一直分配内存,一点一点的去分配
                //等着别人释放内存
                while (accumulated < size) {
                    long startWaitNs = time.nanoseconds();
                    long timeNs;
                    boolean waitingTimeElapsed;
                    try {
                        //在等待别人释放内存
                        //两种情况下会继续执行: 1.时间到了 2.被人唤醒
                        // 1 是超时    await 返回 false
                        // 2 是被人唤醒 await 返回 true
                        // todo 对应 BufferPool#deallocate
                        //  Condition moreMem = this.waiters.peekFirst().signal()  可能被这里唤醒
                        waitingTimeElapsed = !moreMemory.await(remainingTimeToBlockNs, TimeUnit.NANOSECONDS);
                    } catch (InterruptedException e) {
                        this.waiters.remove(moreMemory);
                        throw e;
                    } finally {
                        long endWaitNs = time.nanoseconds();
                        timeNs = Math.max(0L, endWaitNs - startWaitNs);
                        this.waitTime.record(timeNs, time.milliseconds());
                    }

                    if (waitingTimeElapsed) {
                        //在maxTimeToBlockMs时间内还没加入，则移除moreMemory，并抛异常
                        this.waiters.remove(moreMemory);
                        throw new TimeoutException("Failed to allocate memory within the configured max blocking time " + maxTimeToBlockMs + " ms.");
                    }

                    remainingTimeToBlockNs -= timeNs;
                    // check if we can satisfy this request from the free list,
                    // otherwise allocate memory
                    // 内存池中有数据,并且申请的size就是一个批次的大小
                    if (accumulated == 0 && size == this.poolableSize && !this.free.isEmpty()) {
                        // just grab a buffer from the free list
                        //这里可以直接获取到内存
                        buffer = this.free.pollFirst();
                        accumulated = size;
                    } else {
                        // we'll need to allocate memory, but we may only get
                        // part of what we need on this iteration
                        freeUp(size - accumulated);
                        int got = (int) Math.min(size - accumulated, this.availableMemory);
                        //做内存扣减
                        this.availableMemory -= got;
                        //累加已经分配了多少内存
                        accumulated += got;
                    }
                }
                Condition removed = this.waiters.removeFirst();
                if (removed != moreMemory)
                    throw new IllegalStateException("Wrong condition: this shouldn't happen.");

                // signal any additional waiters if there is more memory left
                // over for them
                if (this.availableMemory > 0 || !this.free.isEmpty()) {
                    //如果存在其他排队线程
                    if (!this.waiters.isEmpty()) {
                        //则通知下一个排队线程进行缓冲分配
                        this.waiters.peekFirst().signal();
                    }
                }
                // unlock and return the buffer
                lock.unlock();
                if (buffer == null)
                    return ByteBuffer.allocate(size);
                else
                    return buffer;
            }
        } finally {
            if (lock.isHeldByCurrentThread())
                lock.unlock();
        }
    }

    /**
     * Attempt to ensure we have at least the requested number of bytes of memory for allocation by deallocating pooled
     * buffers (if needed)
     */
    private void freeUp(int size) {
        // free不为空 并且 可用内存小于要申请的size内存大小
        while (!this.free.isEmpty() && this.availableMemory < size) {
            this.availableMemory += this.free.pollLast().capacity();
        }
    }

    /**
     * Return buffers to the pool. If they are of the poolable size add them to the free list, otherwise just mark the
     * memory as free.
     * 
     * @param buffer The buffer to return
     * @param size The size of the buffer to mark as deallocated, note that this maybe smaller than buffer.capacity
     *             since the buffer may re-allocate itself during in-place compression
     */
    public void deallocate(ByteBuffer buffer, int size) {
        lock.lock();
        try {
            //如果内存块的大小等于一个批次的大小
            if (size == this.poolableSize && size == buffer.capacity()) {
                //清楚内存块里的数据
                buffer.clear();
                //把内存块放入内存池
                this.free.add(buffer);
            } else {
                //如果我们释放的内存块大小不是一个批次的大小
                //那么就把它归为可用内存
                //等着垃圾回收即可
                this.availableMemory += size;
            }
            Condition moreMem = this.waiters.peekFirst();
            if (moreMem != null)
                //释放了内存(或者是归还了内存之后)
                //都会唤醒等待内存的线程
                moreMem.signal();
        } finally {
            lock.unlock();
        }
    }

    public void deallocate(ByteBuffer buffer) {
        deallocate(buffer, buffer.capacity());
    }

    /**
     * the total free memory both unallocated and in the free list
     */
    public long availableMemory() {
        lock.lock();
        try {
            return this.availableMemory + this.free.size() * this.poolableSize;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get the unallocated memory (not in the free list or in use)
     */
    public long unallocatedMemory() {
        lock.lock();
        try {
            return this.availableMemory;
        } finally {
            lock.unlock();
        }
    }

    /**
     * The number of threads blocked waiting on memory
     */
    public int queued() {
        lock.lock();
        try {
            return this.waiters.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * The buffer size that will be retained in the free list after use
     */
    public int poolableSize() {
        return this.poolableSize;
    }

    /**
     * The total memory managed by this pool
     */
    public long totalMemory() {
        return this.totalMemory;
    }

    // package-private method used only for testing
    Deque<Condition> waiters() {
        return this.waiters;
    }
}
