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
package org.apache.kafka.clients;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A class encapsulating some of the logic around metadata.
 * <p>
 * This class is shared by the client thread (for partitioning) and the background sender thread.
 *
 * Metadata is maintained for only a subset of topics, which can be added to over time. When we request metadata for a
 * topic we don't have any metadata for it will trigger a metadata update.
 * <p>
 * If topic expiry is enabled for the metadata, any topic that has not been used within the expiry interval
 * is removed from the metadata refresh set after an update. Consumers disable topic expiry since they explicitly
 * manage topics while producers rely on topic expiry to limit the refresh set.
 */
//真正的元数据肯定是服务端在管理,Producer只是拿过去用
//元数据没有持久化,只是放入内存而已
public final class Metadata {

    private static final Logger log = LoggerFactory.getLogger(Metadata.class);

    public static final long TOPIC_EXPIRY_MS = 5 * 60 * 1000;
    private static final long TOPIC_EXPIRY_NEEDS_UPDATE = -1L;

    //两次更新元数据的请求之间最短的间隔时间,默认值是100ms
    //目的是减少网络的压力
    private final long refreshBackoffMs;
    //多久自动更新一次元数据,默认值是5分钟更新一次
    private final long metadataExpireMs;
    //对于Producer端来讲,元数据是有版本号的
    //每次更新元数据,就会更新一次版本号
    private int version;
    //上一次更新元数据的时间
    private long lastRefreshMs;
    //上一次成功更新元数据的时间
    //如果元数据更新成功,lastRefreshMs和lastSuccessfulRefreshMs就是相等的
    private long lastSuccessfulRefreshMs;
    //kafka集群本身的元数据
    private Cluster cluster;
    //这是一个标识,用来判定是否需要更新元数据
    private boolean needUpdate;
    /* Topics with expiry time */
    //记录了当前已有的topic
    private final Map<String, Long> topics;
    private final List<Listener> listeners;
    private final ClusterResourceListeners clusterResourceListeners;
    private boolean needMetadataForAllTopics;
    //主题是否已经过期
    private final boolean topicExpiryEnabled;

    /**
     * Create a metadata instance with reasonable defaults
     */
    public Metadata() {
        this(100L, 60 * 60 * 1000L);
    }

    public Metadata(long refreshBackoffMs, long metadataExpireMs) {
        this(refreshBackoffMs, metadataExpireMs, false, new ClusterResourceListeners());
    }

    /**
     * Create a new Metadata instance
     * @param refreshBackoffMs The minimum amount of time that must expire between metadata refreshes to avoid busy
     *        polling
     * @param metadataExpireMs The maximum amount of time that metadata can be retained without refresh
     * @param topicExpiryEnabled If true, enable expiry of unused topics
     * @param clusterResourceListeners List of ClusterResourceListeners which will receive metadata updates.
     */
    public Metadata(long refreshBackoffMs, long metadataExpireMs, boolean topicExpiryEnabled, ClusterResourceListeners clusterResourceListeners) {
        this.refreshBackoffMs = refreshBackoffMs;
        this.metadataExpireMs = metadataExpireMs;
        this.topicExpiryEnabled = topicExpiryEnabled;
        this.lastRefreshMs = 0L;
        this.lastSuccessfulRefreshMs = 0L;
        this.version = 0;
        this.cluster = Cluster.empty();
        this.needUpdate = false;
        this.topics = new HashMap<>();
        this.listeners = new ArrayList<>();
        this.clusterResourceListeners = clusterResourceListeners;
        this.needMetadataForAllTopics = false;
    }

    /**
     * Get the current cluster info without blocking
     * 获取当前未被阻塞的集群信息
     */
    public synchronized Cluster fetch() {
        return this.cluster;
    }

    /**
     * Add the topic to maintain in the metadata. If topic expiry is enabled, expiry time
     * will be reset on the next update.
     *
     * 在元数据中添加要维护的主题。如果启用了主题过期，将在下次更新时重置过期时间。
     */
    public synchronized void add(String topic) {
        topics.put(topic, TOPIC_EXPIRY_NEEDS_UPDATE);
    }

    /**
     * The next time to update the cluster info is the maximum of the time the current info will expire and the time the
     * current info can be updated (i.e. backoff time has elapsed); If an update has been request then the expiry time
     * is now
     */
    public synchronized long timeToNextUpdate(long nowMs) {
        long timeToExpire = needUpdate ? 0 : Math.max(this.lastSuccessfulRefreshMs + this.metadataExpireMs - nowMs, 0);
        long timeToAllowUpdate = this.lastRefreshMs + this.refreshBackoffMs - nowMs;
        return Math.max(timeToExpire, timeToAllowUpdate);
    }

    /**
     * Request an update of the current cluster metadata info, return the current version before the update
     * 请求更新当前群集元数据信息，在更新之前返回当前版本
     * 这个方法做了2件事: 1.请求更新 2.返回更新之前的版本号
     */
    public synchronized int requestUpdate() {
        this.needUpdate = true;
        return this.version;
    }

    /**
     * Check whether an update has been explicitly requested.
     * @return true if an update was requested, false otherwise
     */
    public synchronized boolean updateRequested() {
        return this.needUpdate;
    }

    /**
     * Wait for metadata update until the current version is larger than the last version we know of
     * 等待元数据更新，直到当前版本大于我们知道的最后版本
     * 参数一: lastVersion: 更新前的版本号
     * 参数二: maxWaitMs: 最长可以等待的时间
     */
    public synchronized void awaitUpdate(final int lastVersion, final long maxWaitMs) throws InterruptedException {
        //maxWaitMs 60000
        if (maxWaitMs < 0) {
            throw new IllegalArgumentException("Max time to wait for metadata updates should not be < 0 milli seconds");
        }
        //获取当前时间
        long begin = System.currentTimeMillis();
        //剩余可以等待的时间,一开始是最大的等待时间
        long remainingWaitMs = maxWaitMs;
        //version是当前元数据的版本号,如果当前版本号小于等于上一次的版本号,说明元数据还没有更新
        //如果sender线程更新了元数据并且更新成功了,那么版本号就会累加
        while (this.version <= lastVersion) {
            //如果还有剩余时间
            if (remainingWaitMs != 0) {
                //todo 当元数据更新之后,就会唤醒这个线程  对应 Metadata # update # notifyAll
                //这个线程有两种情况会被唤醒: 1.元数据更新了,被sender唤醒;2.时间到了
                wait(remainingWaitMs);//让当前线程阻塞,比如remainingWaitMs是10秒钟,那么就等10s
            }
            //如果代码执行到这里,说明要么被唤醒了,要么到时间了
            //计算一下花了多长时间
            long elapsed = System.currentTimeMillis() - begin;
            //已经超时了
            if (elapsed >= maxWaitMs)
                throw new TimeoutException("Failed to update metadata after " + maxWaitMs + " ms.");
            //剩余可以等待的时间
            remainingWaitMs = maxWaitMs - elapsed;
        }
    }

    /**
     * Replace the current set of topics maintained to the one provided.
     * If topic expiry is enabled, expiry time of the topics will be
     * reset on the next update.
     * @param topics
     */
    public synchronized void setTopics(Collection<String> topics) {
        if (!this.topics.keySet().containsAll(topics))
            requestUpdate();
        this.topics.clear();
        for (String topic : topics)
            this.topics.put(topic, TOPIC_EXPIRY_NEEDS_UPDATE);
    }

    /**
     * Get the list of topics we are currently maintaining metadata for
     */
    public synchronized Set<String> topics() {
        return new HashSet<>(this.topics.keySet());
    }

    /**
     * Check if a topic is already in the topic set.
     * @param topic topic to check
     * @return true if the topic exists, false otherwise
     */
    public synchronized boolean containsTopic(String topic) {
        return this.topics.containsKey(topic);
    }

    /**
     * Updates the cluster metadata. If topic expiry is enabled, expiry time
     * is set for topics if required and expired topics are removed from the metadata.
     */
    public synchronized void update(Cluster cluster, long now) {
        Objects.requireNonNull(cluster, "cluster should not be null");
        this.needUpdate = false;
        this.lastRefreshMs = now;
        this.lastSuccessfulRefreshMs = now;
        this.version += 1;//如果线程获取到了元数据,元数据的版本号会递增加1
        // 这个默认值是true
        if (topicExpiryEnabled) {
            // Handle expiry of topics from the metadata refresh set.
            // 处理元数据刷新之后过期的主题
            //当第一次进来的时候,topics是空的,所以下面的代码是不会执行的
            for (Iterator<Map.Entry<String, Long>> it = topics.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<String, Long> entry = it.next();
                long expireMs = entry.getValue();
                if (expireMs == TOPIC_EXPIRY_NEEDS_UPDATE)
                    entry.setValue(now + TOPIC_EXPIRY_MS);
                else if (expireMs <= now) {
                    it.remove();
                    log.debug("Removing unused topic {} from the metadata list, expiryMs {} now {}", entry.getKey(), expireMs, now);
                }
            }
        }

        for (Listener listener: listeners)
            listener.onMetadataUpdate(cluster);

        String previousClusterId = cluster.clusterResource().clusterId();
        //这个的默认值是false,第一次执行到这里的时候,就会直接跳进else的代码里
        if (this.needMetadataForAllTopics) {
            // the listener may change the interested topics, which could cause another metadata refresh.
            // If we have already fetched all topics, however, another fetch should be unnecessary.
            this.needUpdate = false;
            this.cluster = getClusterForCurrentTopics(cluster);
        } else {
            //第一次进来的时候,代码执行的是这一部分
            //直接把刚刚传进来的对象赋值给this.cluster
            //cluster代表的是kafka集群的元数据
            //初始化的时候,update方法没有去服务端拉取元数据
            this.cluster = cluster;
        }

        // The bootstrap cluster is guaranteed not to have any useful information
        // isBootstrapConfigured 默认是true
        if (!cluster.isBootstrapConfigured()) {
            String clusterId = cluster.clusterResource().clusterId();
            if (clusterId == null ? previousClusterId != null : !clusterId.equals(previousClusterId))
                log.info("Cluster ID: {}", cluster.clusterResource().clusterId());
            clusterResourceListeners.onUpdate(cluster.clusterResource());
        }
        //todo 唤醒那个wait线程  对应 Metadata # awaitUpdate # wait
        // 第一次代码运行到这里的时候,整个流程通过notifyAll唤醒线程就走通了
        notifyAll();
        log.debug("Updated cluster metadata version {} to {}", this.version, this.cluster);
    }

    /**
     * Record an attempt to update the metadata that failed. We need to keep track of this
     * to avoid retrying immediately.
     */
    public synchronized void failedUpdate(long now) {
        this.lastRefreshMs = now;
    }

    /**
     * @return The current metadata version
     */
    public synchronized int version() {
        return this.version;
    }

    /**
     * The last time metadata was successfully updated.
     */
    public synchronized long lastSuccessfulUpdate() {
        return this.lastSuccessfulRefreshMs;
    }

    /**
     * The metadata refresh backoff in ms
     */
    public long refreshBackoff() {
        return refreshBackoffMs;
    }

    /**
     * Set state to indicate if metadata for all topics in Kafka cluster is required or not.
     * @param needMetadataForAllTopics boolean indicating need for metadata of all topics in cluster.
     */
    public synchronized void needMetadataForAllTopics(boolean needMetadataForAllTopics) {
        this.needMetadataForAllTopics = needMetadataForAllTopics;
    }

    /**
     * Get whether metadata for all topics is needed or not
     * 是否需要获取所有主题的元数据
     */
    public synchronized boolean needMetadataForAllTopics() {
        return this.needMetadataForAllTopics;
    }

    /**
     * Add a Metadata listener that gets notified of metadata updates
     */
    public synchronized void addListener(Listener listener) {
        this.listeners.add(listener);
    }

    /**
     * Stop notifying the listener of metadata updates
     */
    public synchronized void removeListener(Listener listener) {
        this.listeners.remove(listener);
    }

    /**
     * MetadataUpdate Listener
     */
    public interface Listener {
        void onMetadataUpdate(Cluster cluster);
    }

    private Cluster getClusterForCurrentTopics(Cluster cluster) {
        Set<String> unauthorizedTopics = new HashSet<>();
        Collection<PartitionInfo> partitionInfos = new ArrayList<>();
        List<Node> nodes = Collections.emptyList();
        Set<String> internalTopics = Collections.emptySet();
        String clusterId = null;
        if (cluster != null) {
            clusterId = cluster.clusterResource().clusterId();
            internalTopics = cluster.internalTopics();
            unauthorizedTopics.addAll(cluster.unauthorizedTopics());
            unauthorizedTopics.retainAll(this.topics.keySet());

            for (String topic : this.topics.keySet()) {
                List<PartitionInfo> partitionInfoList = cluster.partitionsForTopic(topic);
                if (partitionInfoList != null) {
                    partitionInfos.addAll(partitionInfoList);
                }
            }
            nodes = cluster.nodes();
        }
        return new Cluster(clusterId, nodes, partitionInfos, unauthorizedTopics, internalTopics);
    }
}
