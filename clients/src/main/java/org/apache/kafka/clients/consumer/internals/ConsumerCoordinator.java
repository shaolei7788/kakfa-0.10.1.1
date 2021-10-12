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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor.Subscription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.JoinGroupRequest.ProtocolMetadata;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This class manages the coordination process with the consumer coordinator.
 */
//todo 消费者协调器 更新分区状态的提交偏移量(消费偏移量)
public final class ConsumerCoordinator extends AbstractCoordinator {

    private static final Logger log = LoggerFactory.getLogger(ConsumerCoordinator.class);
    //分区分配器
    private final List<PartitionAssignor> assignors;
    //集群元数据信息
    private final Metadata metadata;
    private final ConsumerCoordinatorMetrics sensors;
    private final SubscriptionState subscriptions;
    private final OffsetCommitCallback defaultOffsetCommitCallback;
    //是否自动提交偏移量
    private final boolean autoCommitEnabled;
    //自动提交偏移量时间间隔
    private final int autoCommitIntervalMs;
    //拦截器集合
    private final ConsumerInterceptors<?, ?> interceptors;
    //标识是否排除内部的topic
    private final boolean excludeInternalTopics;

    // this collection must be thread-safe because it is modified from the response handler
    // of offset commit requests, which may be invoked from the heartbeat thread
    private final ConcurrentLinkedQueue<OffsetCommitCompletion> completedOffsetCommits;

    private boolean isLeader = false;
    private Set<String> joinedSubscription;
    //用来存储Metadata快照信息，主要用来检查topic是否发生了分区变化
    private MetadataSnapshot metadataSnapshot;
    private MetadataSnapshot assignmentSnapshot;
    private long nextAutoCommitDeadline;

    /**
     * Initialize the coordination manager.
     */
    public ConsumerCoordinator(ConsumerNetworkClient client,
                               String groupId,
                               int rebalanceTimeoutMs,
                               int sessionTimeoutMs,
                               int heartbeatIntervalMs,
                               List<PartitionAssignor> assignors,
                               Metadata metadata,
                               SubscriptionState subscriptions,
                               Metrics metrics,
                               String metricGrpPrefix,
                               Time time,
                               long retryBackoffMs,
                               OffsetCommitCallback defaultOffsetCommitCallback,
                               boolean autoCommitEnabled,
                               int autoCommitIntervalMs,
                               ConsumerInterceptors<?, ?> interceptors,
                               boolean excludeInternalTopics) {
        super(client,
                groupId,
                rebalanceTimeoutMs,
                sessionTimeoutMs,
                heartbeatIntervalMs,
                metrics,
                metricGrpPrefix,
                time,
                retryBackoffMs);
        this.metadata = metadata;
        this.metadataSnapshot = new MetadataSnapshot(subscriptions, metadata.fetch());
        this.subscriptions = subscriptions;
        this.defaultOffsetCommitCallback = defaultOffsetCommitCallback;
        this.autoCommitEnabled = autoCommitEnabled;
        this.autoCommitIntervalMs = autoCommitIntervalMs;
        this.assignors = assignors;
        this.completedOffsetCommits = new ConcurrentLinkedQueue<>();
        this.sensors = new ConsumerCoordinatorMetrics(metrics, metricGrpPrefix);
        this.interceptors = interceptors;
        this.excludeInternalTopics = excludeInternalTopics;

        if (autoCommitEnabled)
            this.nextAutoCommitDeadline = time.milliseconds() + autoCommitIntervalMs;

        this.metadata.requestUpdate();
        addMetadataListener();
    }

    @Override
    public String protocolType() {
        return ConsumerProtocol.PROTOCOL_TYPE;
    }

    @Override
    public List<ProtocolMetadata> metadata() {
        this.joinedSubscription = subscriptions.subscription();
        List<ProtocolMetadata> metadataList = new ArrayList<>();
        for (PartitionAssignor assignor : assignors) {
            //创建了一个订阅状态，包含了消费者订阅的topic列表
            Subscription subscription = assignor.subscription(joinedSubscription);
            ByteBuffer metadata = ConsumerProtocol.serializeSubscription(subscription);
            //每个分区分配器的元数据实际上都是一样的
            metadataList.add(new ProtocolMetadata(assignor.name(), metadata));
        }
        return metadataList;
    }

    public void updatePatternSubscription(Cluster cluster) {
        final Set<String> topicsToSubscribe = new HashSet<>();

        for (String topic : cluster.topics())
            if (subscriptions.getSubscribedPattern().matcher(topic).matches() &&
                    !(excludeInternalTopics && cluster.internalTopics().contains(topic)))
                topicsToSubscribe.add(topic);

        subscriptions.subscribeFromPattern(topicsToSubscribe);

        // note we still need to update the topics contained in the metadata. Although we have
        // specified that all topics should be fetched, only those set explicitly will be retained
        metadata.setTopics(subscriptions.groupSubscription());
    }

    private void addMetadataListener() {
        this.metadata.addListener(new Metadata.Listener() {
            @Override
            public void onMetadataUpdate(Cluster cluster) {
                // if we encounter any unauthorized topics, raise an exception to the user
                if (!cluster.unauthorizedTopics().isEmpty())
                    throw new TopicAuthorizationException(new HashSet<>(cluster.unauthorizedTopics()));

                if (subscriptions.hasPatternSubscription())
                    updatePatternSubscription(cluster);

                // check if there are any changes to the metadata which should trigger a rebalance
                if (subscriptions.partitionsAutoAssigned()) {
                    MetadataSnapshot snapshot = new MetadataSnapshot(subscriptions, cluster);
                    if (!snapshot.equals(metadataSnapshot))
                        metadataSnapshot = snapshot;
                }
            }
        });
    }

    private PartitionAssignor lookupAssignor(String name) {
        for (PartitionAssignor assignor : this.assignors) {
            if (assignor.name().equals(name))
                return assignor;
        }
        return null;
    }

    @Override
    protected void onJoinComplete(int generation,
                                  String memberId,
                                  String assignmentStrategy,
                                  ByteBuffer assignmentBuffer) {
        // only the leader is responsible for monitoring for metadata changes (i.e. partition changes)
        if (!isLeader)
            assignmentSnapshot = null;
        //获取分区分配器
        PartitionAssignor assignor = lookupAssignor(assignmentStrategy);
        if (assignor == null)
            throw new IllegalStateException("Coordinator selected invalid assignment protocol: " + assignmentStrategy);
        //反序列化分区分配的结果
        Assignment assignment = ConsumerProtocol.deserializeAssignment(assignmentBuffer);

        // set the flag to refresh last committed offsets
        //todo 将needsFetchCommittedOffsets = true ,允许从服务端获取最近一次提交的offset
        subscriptions.needRefreshCommits();
        //更新分区分配的结果
        subscriptions.assignFromSubscribed(assignment.partitions());
        // give the assignor a chance to update internal state based on the received assignment
        //一个空实现
        assignor.onAssignment(assignment);
        // reschedule the auto commit starting from now
        //下次自动提交戒指时间
        this.nextAutoCommitDeadline = time.milliseconds() + autoCommitIntervalMs;

        // execute the user's callback after rebalance
        ConsumerRebalanceListener listener = subscriptions.listener();
        log.info("Setting newly assigned partitions {} for group {}", subscriptions.assignedPartitions(), groupId);
        try {
            Set<TopicPartition> assigned = new HashSet<>(subscriptions.assignedPartitions());
            //分区分配好后，可以指定
            listener.onPartitionsAssigned(assigned);
        } catch (WakeupException e) {
            throw e;
        } catch (Exception e) {
            log.error("User provided listener {} for group {} failed on partition assignment",
                    listener.getClass().getName(), groupId, e);
        }
    }

    /**
     * Poll for coordinator events. This ensures that the coordinator is known and that the consumer
     * has joined the group (if it is using group management). This also handles periodic offset commits
     * if they are enabled.
     *
     * @param now current time in milliseconds
     */
    public void poll(long now) {
        //触发已完成提交偏移量的回调函数
        invokeCompletedOffsetCommitCallbacks();
        //todo subscriptions.partitionsAutoAssigned() 代表自动分配分区
        // coordinator 为空
        if (subscriptions.partitionsAutoAssigned() && coordinatorUnknown()) {
            //todo 确保Coordinator创建完成
            ensureCoordinatorReady();
            now = time.milliseconds();
        }

        //判断是否需要加入group
        if (needRejoin()) {
            // due to a race condition between the initial metadata fetch and the initial rebalance,
            // we need to ensure that the metadata is fresh before joining initially. This ensures
            // that we have matched the pattern against the cluster's topics at least once before joining.
            //消费者有正则表达式订阅类型
            if (subscriptions.hasPatternSubscription()) {
                client.ensureFreshMetadata();
            }
            //todo 确保group is active
            ensureActiveGroup();
            now = time.milliseconds();
        }

        pollHeartbeat(now);
        //todo 可能异步自动提交偏移量
        maybeAutoCommitOffsetsAsync(now);
    }

    /**
     * Return the time to the next needed invocation of {@link #poll(long)}.
     * @param now current time in milliseconds
     * @return the maximum time in milliseconds the caller should wait before the next invocation of poll()
     */
    public long timeToNextPoll(long now) {
        if (!autoCommitEnabled)
            return timeToNextHeartbeat(now);

        if (now > nextAutoCommitDeadline)
            return 0;

        return Math.min(nextAutoCommitDeadline - now, timeToNextHeartbeat(now));
    }

    @Override
    protected Map<String, ByteBuffer> performAssignment(String leaderId,
                                                        String assignmentStrategy,
                                                        Map<String, ByteBuffer> allSubscriptions) {
        //获取指定的分区分配器 默认是range分区
        PartitionAssignor assignor = lookupAssignor(assignmentStrategy);
        if (assignor == null)
            throw new IllegalStateException("Coordinator selected invalid assignment protocol: " + assignmentStrategy);

        //订阅的所有主题
        Set<String> allSubscribedTopics = new HashSet<>();
        Map<String, Subscription> subscriptions = new HashMap<>();
        for (Map.Entry<String, ByteBuffer> subscriptionEntry : allSubscriptions.entrySet()) {
            //反序列化消费者的订阅信息
            Subscription subscription = ConsumerProtocol.deserializeSubscription(subscriptionEntry.getValue());
            // subscriptionEntry.getKey() 是memberid
            subscriptions.put(subscriptionEntry.getKey(), subscription);
            allSubscribedTopics.addAll(subscription.topics());
        }
        //更新消费组订阅的主题
        this.subscriptions.groupSubscribe(allSubscribedTopics);
        //更新集群订阅的主题
        metadata.setTopics(this.subscriptions.groupSubscription());
        // update metadata (if needed) and keep track of the metadata used for assignment so that
        // we can check after rebalance completion whether anything has changed
        client.ensureFreshMetadata();

        isLeader = true;
        assignmentSnapshot = metadataSnapshot;
        log.debug("Performing assignment for group {} using strategy {} with subscriptions {}", groupId, assignor.name(), subscriptions);

        //todo 进行分区分配 metadata.fetch() = Cluster    subscriptions 所有的消费者信息
        // key是每个消费者id   Assignment是分配的结果
        Map<String, Assignment> assignment = assignor.assign(metadata.fetch(), subscriptions);
        log.debug("Finished assignment for group {}: {}", groupId, assignment);
        Map<String, ByteBuffer> groupAssignment = new HashMap<>();
        for (Map.Entry<String, Assignment> assignmentEntry : assignment.entrySet()) {
            //对Assignment进行序列化
            ByteBuffer buffer = ConsumerProtocol.serializeAssignment(assignmentEntry.getValue());
            groupAssignment.put(assignmentEntry.getKey(), buffer);
        }
        return groupAssignment;
    }

    @Override
    protected void onJoinPrepare(int generation, String memberId) {
        // commit offsets prior to rebalance if auto-commit enabled
        //同步提交偏移量
        maybeAutoCommitOffsetsSync();

        // execute the user's callback before rebalance
        // subscriptions = SubscriptionState
        //消费者再平衡监听器
        ConsumerRebalanceListener listener = subscriptions.listener();
        log.info("Revoking previously assigned partitions {} for group {}", subscriptions.assignedPartitions(), groupId);
        try {
            Set<TopicPartition> revoked = new HashSet<>(subscriptions.assignedPartitions());
            //调用onPartitionsRevoked方法
            listener.onPartitionsRevoked(revoked);
        } catch (WakeupException e) {
            throw e;
        } catch (Exception e) {
            log.error("User provided listener {} for group {} failed on partition revocation",
                    listener.getClass().getName(), groupId, e);
        }

        isLeader = false;
        subscriptions.resetGroupSubscription();
    }

    @Override
    public boolean needRejoin() {
        //判断分区是否自动分配  subscriptions.partitionsAutoAssigned() 自动分配
        if (!subscriptions.partitionsAutoAssigned())
            return false;

        // we need to rejoin if we performed the assignment and metadata has changed
        //指定的快照跟元数据快照不相等 也需要重新加入
        if (assignmentSnapshot != null && !assignmentSnapshot.equals(metadataSnapshot))
            return true;

        // we need to join if our subscription has changed since the last join
        if (joinedSubscription != null && !joinedSubscription.equals(subscriptions.subscription()))
            return true;

        return super.needRejoin();
    }

    //如果有必要，更新提交偏移量
    public void refreshCommittedOffsetsIfNeeded() {
        if (subscriptions.refreshCommitsNeeded()) {
            //获取已经分配的分区
            Set<TopicPartition> topicPartitions = subscriptions.assignedPartitions();
            //todo 发送ApiKeys.OFFSET_FETCH 请求给协调者，获取分区已提交的偏移量
            Map<TopicPartition, OffsetAndMetadata> offsets = fetchCommittedOffsets(topicPartitions);
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                TopicPartition tp = entry.getKey();
                // verify assignment is still active
                if (subscriptions.isAssigned(tp)){
                    //更新分区状态的committed变量
                    this.subscriptions.committed(tp, entry.getValue());
                }
            }
            //将needsFetchCommittedOffsets 置为false 即不需要拉取提交偏移量
            this.subscriptions.commitsRefreshed();
        }
    }

    /**
     * Fetch the current committed offsets from the coordinator for a set of partitions.
     * @param partitions The partitions to fetch offsets for
     * @return A map from partition to the committed offset
     */
    //发送获取偏移量请求给协调者
    public Map<TopicPartition, OffsetAndMetadata> fetchCommittedOffsets(Set<TopicPartition> partitions) {
        while (true) {
            //确保协调者是连接好的
            ensureCoordinatorReady();
            // contact coordinator to fetch committed offsets
            //todo 发送ApiKeys.OFFSET_FETCH 请求给协调者，获取分区已提交的偏移量
            RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future = sendOffsetFetchRequest(partitions);
            client.poll(future);

            if (future.succeeded())
                return future.value();

            if (!future.isRetriable())
                throw future.exception();

            time.sleep(retryBackoffMs);
        }
    }

    @Override
    public void close() {
        // we do not need to re-enable wakeups since we are closing already
        client.disableWakeups();
        try {
            maybeAutoCommitOffsetsSync();
        } finally {
            super.close();
        }
    }

    // visible for testing
    void invokeCompletedOffsetCommitCallbacks() {
        while (true) {
            OffsetCommitCompletion completion = completedOffsetCommits.poll();
            if (completion == null)
                break;
            completion.invoke();
        }
    }


    public void commitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
        //调用完成提交偏移量的回调
        invokeCompletedOffsetCommitCallbacks();
        if (!coordinatorUnknown()) {
            //todo 有协调者
            doCommitOffsetsAsync(offsets, callback);
        } else {
            // we don't know the current coordinator, so try to find it and then send the commit
            // or fail (we don't want recursive retries which can cause offset commits to arrive
            // out of order). Note that there may be multiple offset commits chained to the same
            // coordinator lookup request. This is fine because the listeners will be invoked in
            // the same order that they were added. Note also that AbstractCoordinator prevents
            // multiple concurrent coordinator lookup requests.
            RequestFuture<Void> future = lookupCoordinator();
            future.addListener(new RequestFutureListener<Void>() {
                @Override
                public void onSuccess(Void value) {
                    doCommitOffsetsAsync(offsets, callback);
                }

                @Override
                public void onFailure(RuntimeException e) {
                    completedOffsetCommits.add(new OffsetCommitCompletion(callback, offsets, new RetriableCommitFailedException(e)));
                }
            });
        }

        // ensure the commit has a chance to be transmitted (without blocking on its completion).
        // Note that commits are treated as heartbeats by the coordinator, so there is no need to
        // explicitly allow heartbeats through delayed task execution.
        client.pollNoWakeup();
    }

    //todo 异步提交偏移量
    private void doCommitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
        this.subscriptions.needRefreshCommits();
        //todo 发送提交偏移量请求
        RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
        final OffsetCommitCallback cb = callback == null ? defaultOffsetCommitCallback : callback;
        future.addListener(new RequestFutureListener<Void>() {
            @Override
            public void onSuccess(Void value) {
                if (interceptors != null){
                    interceptors.onCommit(offsets);
                }
                completedOffsetCommits.add(new OffsetCommitCompletion(cb, offsets, null));
            }

            @Override
            public void onFailure(RuntimeException e) {
                Exception commitException = e;

                if (e instanceof RetriableException)
                    commitException = new RetriableCommitFailedException(e);

                completedOffsetCommits.add(new OffsetCommitCompletion(cb, offsets, commitException));
            }
        });
    }

    //todo 同步提交偏移量
    public void commitOffsetsSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        invokeCompletedOffsetCommitCallbacks();
        if (offsets.isEmpty()){
            return;
        }
        while (true) {
            ensureCoordinatorReady();
            RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
            //阻塞等待OffsetCommitResponse
            client.poll(future);
            if (future.succeeded()) {
                if (interceptors != null)
                    interceptors.onCommit(offsets);
                return;
            }
            if (!future.isRetriable())
                throw future.exception();
            //todo 检查到RetriableException会进行重新
            time.sleep(retryBackoffMs);
        }
    }

    private void maybeAutoCommitOffsetsAsync(long now) {
        //判断是不是自动提交偏移量
        if (autoCommitEnabled) {
            if (coordinatorUnknown()) {
                this.nextAutoCommitDeadline = now + retryBackoffMs;
            } else if (now >= nextAutoCommitDeadline) {
                //todo 下次提交偏移量时间   autoCommitIntervalMs = 5000
                this.nextAutoCommitDeadline = now + autoCommitIntervalMs;
                doAutoCommitOffsetsAsync();
            }
        }
    }

    public void maybeAutoCommitOffsetsNow() {
        if (autoCommitEnabled && !coordinatorUnknown())
            doAutoCommitOffsetsAsync();
    }

    private void doAutoCommitOffsetsAsync() {
        //已经消费的分区偏移量
        Map<TopicPartition, OffsetAndMetadata> allConsumed = subscriptions.allConsumed();
        //todo 异步提交偏移量
        OffsetCommitCallback offsetCommitCallback = new OffsetCommitCallback() {
            @Override
            public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                if (exception != null) {
                    log.warn("Auto offset commit failed for group {}: {}", groupId, exception.getMessage());
                    if (exception instanceof RetriableException)
                        nextAutoCommitDeadline = Math.min(time.milliseconds() + retryBackoffMs, nextAutoCommitDeadline);
                } else {
                    log.debug("Completed autocommit of offsets {} for group {}", offsets, groupId);
                }
            }
        };
        commitOffsetsAsync(allConsumed,offsetCommitCallback);
    }

    private void maybeAutoCommitOffsetsSync() {
        if (autoCommitEnabled) {
            try {
                commitOffsetsSync(subscriptions.allConsumed());
            } catch (WakeupException e) {
                // rethrow wakeups since they are triggered by the user
                throw e;
            } catch (Exception e) {
                // consistent with async auto-commit failures, we do not propagate the exception
                log.warn("Auto offset commit failed for group {}: {}", groupId, e.getMessage());
            }
        }
    }

    public static class DefaultOffsetCommitCallback implements OffsetCommitCallback {
        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            if (exception != null)
                log.error("Offset commit failed.", exception);
        }
    }

    /**
     * Commit offsets for the specified list of topics and partitions. This is a non-blocking call
     * which returns a request future that can be polled in the case of a synchronous commit or ignored in the
     * asynchronous case.
     *
     * @param offsets The list of offsets per partition that should be committed.
     * @return A request future whose value indicates whether the commit was successful or not
     */
    private RequestFuture<Void> sendOffsetCommitRequest(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (offsets.isEmpty())
            return RequestFuture.voidSuccess();

        Node coordinator = coordinator();
        if (coordinator == null)
            return RequestFuture.coordinatorNotAvailable();

        // create the offset commit request
        //创建请求对象
        Map<TopicPartition, OffsetCommitRequest.PartitionData> offsetData = new HashMap<>(offsets.size());
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            OffsetAndMetadata offsetAndMetadata = entry.getValue();
            offsetData.put(entry.getKey(), new OffsetCommitRequest.PartitionData(offsetAndMetadata.offset(), offsetAndMetadata.metadata()));
        }

        final Generation generation;
        //判断是否是自动分配分区
        if (subscriptions.partitionsAutoAssigned()){
            generation = generation();
        } else{
            generation = Generation.NO_GENERATION;
        }
        // if the generation is null, we are not part of an active group (and we expect to be).
        // the only thing we can do is fail the commit and let the user rejoin the group in poll()
        if (generation == null)
            return RequestFuture.failure(new CommitFailedException());

        OffsetCommitRequest req = new OffsetCommitRequest(
                this.groupId,
                generation.generationId,
                generation.memberId,
                OffsetCommitRequest.DEFAULT_RETENTION_TIME,//次offset最长保存的时间
                offsetData);

        log.trace("Sending offset-commit request with {} to coordinator {} for group {}", offsets, coordinator, groupId);
        //todo 有compose回调函数
        RequestFuture<ClientResponse> send = client.send(coordinator, ApiKeys.OFFSET_COMMIT, req);
        RequestFuture<Void> future = send.compose(new OffsetCommitResponseHandler(offsets));
        return future;

    }

    //todo 提交偏移量响应handler
    private class OffsetCommitResponseHandler extends CoordinatorResponseHandler<OffsetCommitResponse, Void> {

        private final Map<TopicPartition, OffsetAndMetadata> offsets;

        public OffsetCommitResponseHandler(Map<TopicPartition, OffsetAndMetadata> offsets) {
            this.offsets = offsets;
        }

        @Override
        public OffsetCommitResponse parse(ClientResponse response) {
            return new OffsetCommitResponse(response.responseBody());
        }

        @Override
        public void handle(OffsetCommitResponse commitResponse, RequestFuture<Void> future) {
            sensors.commitLatency.record(response.requestLatencyMs());
            Set<String> unauthorizedTopics = new HashSet<>();

            for (Map.Entry<TopicPartition, Short> entry : commitResponse.responseData().entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetAndMetadata offsetAndMetadata = this.offsets.get(tp);
                long offset = offsetAndMetadata.offset();

                Errors error = Errors.forCode(entry.getValue());
                if (error == Errors.NONE) {
                    //没有错误
                    log.debug("Group {} committed offset {} for partition {}", groupId, offset, tp);
                    if (subscriptions.isAssigned(tp)){
                        // update the local cache only if the partition is still assigned
                        subscriptions.committed(tp, offsetAndMetadata);
                    }
                } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    log.error("Not authorized to commit offsets for group {}", groupId);
                    future.raise(new GroupAuthorizationException(groupId));
                    return;
                } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                    unauthorizedTopics.add(tp.topic());
                } else if (error == Errors.OFFSET_METADATA_TOO_LARGE
                        || error == Errors.INVALID_COMMIT_OFFSET_SIZE) {
                    // raise the error to the user
                    log.debug("Offset commit for group {} failed on partition {}: {}", groupId, tp, error.message());
                    future.raise(error);
                    return;
                } else if (error == Errors.GROUP_LOAD_IN_PROGRESS) {
                    // just retry
                    log.debug("Offset commit for group {} failed: {}", groupId, error.message());
                    future.raise(error);
                    return;
                } else if (error == Errors.GROUP_COORDINATOR_NOT_AVAILABLE
                        || error == Errors.NOT_COORDINATOR_FOR_GROUP
                        || error == Errors.REQUEST_TIMED_OUT) {
                    log.debug("Offset commit for group {} failed: {}", groupId, error.message());
                    coordinatorDead();
                    future.raise(error);
                    return;
                } else if (error == Errors.UNKNOWN_MEMBER_ID
                        || error == Errors.ILLEGAL_GENERATION
                        || error == Errors.REBALANCE_IN_PROGRESS) {
                    // need to re-join group
                    log.debug("Offset commit for group {} failed: {}", groupId, error.message());
                    resetGeneration();
                    future.raise(new CommitFailedException());
                    return;
                } else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                    log.debug("Offset commit for group {} failed on partition {}: {}", groupId, tp, error.message());
                    future.raise(new KafkaException("Partition " + tp + " may not exist or user may not have Describe access to topic"));
                    return;
                } else {
                    log.error("Group {} failed to commit partition {} at offset {}: {}", groupId, tp, offset, error.message());
                    future.raise(new KafkaException("Unexpected error in commit: " + error.message()));
                    return;
                }
            }

            if (!unauthorizedTopics.isEmpty()) {
                log.error("Not authorized to commit to topics {} for group {}", unauthorizedTopics, groupId);
                future.raise(new TopicAuthorizationException(unauthorizedTopics));
            } else {
                future.complete(null);
            }
        }
    }

    /**
     * Fetch the committed offsets for a set of partitions. This is a non-blocking call. The
     * returned future can be polled to get the actual offsets returned from the broker.
     *
     * @param partitions The set of partitions to get offsets for.
     * @return A request future containing the committed offsets.
     */
    private RequestFuture<Map<TopicPartition, OffsetAndMetadata>> sendOffsetFetchRequest(Set<TopicPartition> partitions) {
        Node coordinator = coordinator();
        if (coordinator == null)
            return RequestFuture.coordinatorNotAvailable();

        log.debug("Group {} fetching committed offsets for partitions: {}", groupId, partitions);
        // construct the request
        OffsetFetchRequest request = new OffsetFetchRequest(this.groupId, new ArrayList<>(partitions));

        // 创建发送请求对象
        RequestFuture<ClientResponse> send = client.send(coordinator, ApiKeys.OFFSET_FETCH, request);
        //返回的是adapted 对象
        RequestFuture<Map<TopicPartition, OffsetAndMetadata>> compose = send.compose(new OffsetFetchResponseHandler());
        return compose;

    }

    //todo 偏移量拉取响应handler
    private class OffsetFetchResponseHandler extends CoordinatorResponseHandler<OffsetFetchResponse, Map<TopicPartition, OffsetAndMetadata>> {

        @Override
        public OffsetFetchResponse parse(ClientResponse response) {
            return new OffsetFetchResponse(response.responseBody());
        }

        @Override
        public void handle(OffsetFetchResponse response, RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future) {
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(response.responseData().size());
            for (Map.Entry<TopicPartition, OffsetFetchResponse.PartitionData> entry : response.responseData().entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetFetchResponse.PartitionData data = entry.getValue();
                if (data.hasError()) {
                    Errors error = Errors.forCode(data.errorCode);
                    log.debug("Group {} failed to fetch offset for partition {}: {}", groupId, tp, error.message());

                    if (error == Errors.GROUP_LOAD_IN_PROGRESS) {
                        // just retry
                        future.raise(error);
                    } else if (error == Errors.NOT_COORDINATOR_FOR_GROUP) {
                        // re-discover the coordinator and retry
                        coordinatorDead();
                        future.raise(error);
                    } else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                        future.raise(new KafkaException("Partition " + tp + " may not exist or user may not have Describe access to topic"));
                    } else {
                        future.raise(new KafkaException("Unexpected error in fetch offset response: " + error.message()));
                    }
                    return;
                } else if (data.offset >= 0) {
                    // record the position with the offset (-1 indicates no committed offset to fetch)
                    offsets.put(tp, new OffsetAndMetadata(data.offset, data.metadata));
                } else {
                    log.debug("Group {} has no committed offset for partition {}", groupId, tp);
                }
            }

            future.complete(offsets);
        }
    }

    private class ConsumerCoordinatorMetrics {
        public final Metrics metrics;
        public final String metricGrpName;

        public final Sensor commitLatency;

        public ConsumerCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metrics = metrics;
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            this.commitLatency = metrics.sensor("commit-latency");
            this.commitLatency.add(metrics.metricName("commit-latency-avg",
                this.metricGrpName,
                "The average time taken for a commit request"), new Avg());
            this.commitLatency.add(metrics.metricName("commit-latency-max",
                this.metricGrpName,
                "The max time taken for a commit request"), new Max());
            this.commitLatency.add(metrics.metricName("commit-rate",
                this.metricGrpName,
                "The number of commit calls per second"), new Rate(new Count()));

            Measurable numParts =
                new Measurable() {
                    public double measure(MetricConfig config, long now) {
                        return subscriptions.assignedPartitions().size();
                    }
                };
            metrics.addMetric(metrics.metricName("assigned-partitions",
                this.metricGrpName,
                "The number of partitions currently assigned to this consumer"), numParts);
        }
    }

    private static class MetadataSnapshot {
        private final Map<String, Integer> partitionsPerTopic;

        public MetadataSnapshot(SubscriptionState subscription, Cluster cluster) {
            Map<String, Integer> partitionsPerTopic = new HashMap<>();
            for (String topic : subscription.groupSubscription())
                partitionsPerTopic.put(topic, cluster.partitionCountForTopic(topic));
            this.partitionsPerTopic = partitionsPerTopic;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MetadataSnapshot that = (MetadataSnapshot) o;
            return partitionsPerTopic != null ? partitionsPerTopic.equals(that.partitionsPerTopic) : that.partitionsPerTopic == null;
        }

        @Override
        public int hashCode() {
            return partitionsPerTopic != null ? partitionsPerTopic.hashCode() : 0;
        }
    }

    private static class OffsetCommitCompletion {
        private final OffsetCommitCallback callback;
        private final Map<TopicPartition, OffsetAndMetadata> offsets;
        private final Exception exception;

        public OffsetCommitCompletion(OffsetCommitCallback callback, Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            this.callback = callback;
            this.offsets = offsets;
            this.exception = exception;
        }

        public void invoke() {
            if (callback != null)
                callback.onComplete(offsets, exception);
        }
    }

}
